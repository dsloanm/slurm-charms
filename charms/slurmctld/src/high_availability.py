# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Slurmctld high availability (HA) features."""

import logging
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

import ops
from constants import HA_MOUNT_POINT

from charms.filesystem_client.v0.mount_info import (
    MountedFilesystemEvent,
    MountInfo,
    MountProviderConnectedEvent,
    MountRequires,
)

logger = logging.getLogger()


class SlurmctldHA(ops.Object):
    """Slurmctld high availability (HA)."""

    def __init__(self, charm, relation_name: str):
        """Initialize filesystem-client event observation."""
        super().__init__(charm, relation_name)
        self._charm = charm
        self._mount = MountRequires(self._charm, relation_name)

        self.framework.observe(
            self._mount.on.mount_provider_connected, self._on_mount_provider_connected
        )
        self.framework.observe(self._mount.on.mounted_filesystem, self._on_mounted_filesystem)

    def _on_mount_provider_connected(self, event: MountProviderConnectedEvent) -> None:
        """Handle filesystem-client connected event."""
        for relation in self._mount.relations:
            self._mount.set_mount_info(relation.id, MountInfo(mountpoint=HA_MOUNT_POINT))
        status_message = f"Requesting file system mount: {HA_MOUNT_POINT}"
        logger.debug(status_message)
        self._charm.unit.status = ops.MaintenanceStatus(status_message)

    def _on_mounted_filesystem(self, event: MountedFilesystemEvent) -> None:
        """Handle filesystem-client mounted event."""
        if self._charm.unit.is_leader() and not self._charm.slurmctld.config.path.exists():
            logger.debug("slurm.conf not found. deferring event")
            event.defer()
            return

        # Both leader and non-leaders migrate /etc/ config files
        etc_source = Path("/etc/slurm")
        target = Path(HA_MOUNT_POINT)

        try:
            self._migrate_etc_data(etc_source, target / "etc" / "slurm")
        except shutil.Error:
            logger.exception("failed to migrate %s to %s. deferring event", etc_source, target)
            event.defer()
            return

        if not self._charm.unit.is_leader():
            # Non-leaders have no more data to migrate
            logger.debug("storage mounted. starting unit")
            self._charm.on.start.emit()
            return

        # The leader must also migrate StateSaveLocation data
        config = self._charm.slurmctld.config.load()
        state_save_source = Path(config.state_save_location)

        # JWT key requires separate handling - it is in the state directory by default
        jwt_key_path = Path(config.auth_alt_parameters["jwt_key"])
        if jwt_key_path.is_relative_to(state_save_source):
            # Given:
            #
            # jwt_key_path      = /var/lib/slurm/checkpoint/jwt_hs256.key
            # state_save_source = /var/lib/slurm/checkpoint
            # target            = /mnt/slurmctld-statefs
            #
            # jwt_key_path becomes /mnt/slurmctld-statefs/checkpoint/jwt_hs256.key
            jwt_key_path = target / jwt_key_path.relative_to(state_save_source.parent)

        try:
            self._migrate_state_save_location_data(state_save_source, target)
        except subprocess.CalledProcessError:
            logger.exception(
                "failed to migrate %s to %s. deferring event", state_save_source, target
            )
            event.defer()
            return

        # Migration has been successful, update configs to the new path and restart service
        self._charm.slurmctld.jwt.path = jwt_key_path
        with self._charm.slurmctld.config.edit() as config:
            if config.auth_alt_parameters["jwt_key"] != jwt_key_path:
                config.auth_alt_parameters["jwt_key"] = str(jwt_key_path)
            config.state_save_location = str(target / state_save_source.name)
        self._charm.on.start.emit()

    def _migrate_etc_data(self, source: Path, target: Path) -> None:
        """Migrate the given source etc directory to the given target.

        The charm leader recursively copies the source directory to the target.
        All units then replace the source with a symlink to the target.

        This is necessary in a high availability (HA) deployment as all slurmctld units require
        access to identical conf files. For this reason, the target must be located on shared
        storage mounted on all slurmctld units.

        To avoid data loss, the existing configuration is backed up to a directory suffixed by the
        current date and time before migration. For example, `/etc/slurm_20250620_161437`.

        Args:
            source: Path to the directory containing Slurm config files, e.g. `/etc/slurm`
            target: Path to the directory Slurm config files are migrated to,
                    e.g. `/mnt/slurmctld-statefs`
        """
        # Nothing to do if target already correctly symlinked
        if source.is_symlink() and source.resolve() == target:
            logger.debug("%s -> %s sylink already exists", source, target)
            return

        if source.exists():
            if self._charm.unit.is_leader():
                logger.debug("leader copying %s to %s", source, target)

                def copy_preserve_ids(source, target):
                    """Preserve owner and group IDs of copied files."""
                    output = shutil.copy2(source, target)
                    stat = Path(source).stat()
                    shutil.chown(target, user=stat.st_uid, group=stat.st_gid)
                    return output

                shutil.copytree(
                    source, target, copy_function=copy_preserve_ids, dirs_exist_ok=True
                )

            # Timestamp to avoid overwriting any existing backup
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_target = Path(f"{source}_{timestamp}")
            logger.debug("backing up %s to %s", source, backup_target)
            shutil.move(source, backup_target)
        else:
            logger.warning("%s not found. unable to backup existing slurm data", source)

        logger.debug("symlinking %s to %s", source, target)
        source.symlink_to(target)

    def _migrate_state_save_location_data(self, source: Path, target: Path):
        """Migrate the given source StateSaveLocation directory to the given target.

        Performs an initial `rsync` to the target while slurmctld.service is running.
        Then stops the service and runs a second `rsync` to copy the delta.

        Args:
            source: Path to the directory containing StateSaveLocation data,
                    e.g. `/var/lib/slurm/checkpoint`
            target: Path to the *parent* directory the source is migrated to,
                    e.g. `/mnt/slurmctld-statefs` to migrate to `/mnt/slurmctld-statefs/checkpoint`
        """
        checkpoint_target = target / source.name
        if checkpoint_target.exists() and source == checkpoint_target:
            logger.warning(
                "state save location is already %s. aborting migration", checkpoint_target
            )
            return

        status_message = f"Migrating {source} to {target}"
        logger.debug(status_message)
        self._charm.unit.status = ops.MaintenanceStatus(status_message)

        # Perform initial copy of data while slurmctld.service is still running then stop and sync
        # the delta
        rsync_cmd = f"/usr/bin/rsync --archive --delete {source} {target}".split()
        try:
            subprocess.check_output(rsync_cmd)
        except subprocess.CalledProcessError:
            logger.exception("failed initial sync of %s to %s", source, target)
            raise

        self._charm.slurmctld.service.stop()

        try:
            subprocess.check_output(rsync_cmd)
        except subprocess.CalledProcessError:
            logger.exception("failed delta sync of %s to %s", source, target)
            # Immediately restart slurmctld.service on failure
            self._charm.slurmctld.service.start()
            raise

        # On success, slurmctld.service is restarted after this function, once slurm.conf is updated
