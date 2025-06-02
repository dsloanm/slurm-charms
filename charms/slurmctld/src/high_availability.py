# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Slurmctld high availability (HA) features."""

import json
import logging
import subprocess
import textwrap
from pathlib import Path
from typing import Optional

from constants import (
    CHARM_MAINTAINED_SLURM_CONF_PARAMETERS,
    CHECKPOINT_AUTOFS_MASTER,
    CHECKPOINT_SYNC_SERVICE,
    CHECKPOINT_SYNC_TIMER,
)
from ops import (
    CharmEvents,
    EventBase,
    EventSource,
    Object,
)

import charms.operator_libs_linux.v0.apt as apt
import charms.operator_libs_linux.v1.systemd as systemd

logger = logging.getLogger()


class SlurmctldFailoverEvent(EventBase):
    """Emitted when a failover to this backup controller occurs."""


class HAEvents(CharmEvents):
    """High Availability events."""

    slurmctld_failover = EventSource(SlurmctldFailoverEvent)


class HAOpsError(Exception):
    """Exception raised when a high availability operation failed."""

    @property
    def message(self) -> str:
        """Return message passed as argument to exception."""
        return self.args[0]


class HASlurmctld(Object):
    """Ensapsulate slurmctld high availability operations."""

    def __init__(self, charm):
        super().__init__(charm, "high-availability-slurmctld")
        self._charm = charm
        self.framework.observe(self._charm.on.slurmctld_failover, self._on_failover)

    def install(self) -> None:
        """Install software and services necessary for a slurmctld high availability configuration."""
        # HA mount point. NFS share of active slurmctld instance checkpoint directory.
        Path(f"{CHARM_MAINTAINED_SLURM_CONF_PARAMETERS['StateSaveLocation']}-active").mkdir(
            parents=True, exist_ok=True
        )

        # Program executed by slurmctld when a HA backup instance takes over from the primary
        primary_on = Path(CHARM_MAINTAINED_SLURM_CONF_PARAMETERS["SlurmctldPrimaryOnProg"])
        primary_on.parent.mkdir(parents=True, exist_ok=True)
        exec_cmd = f"/usr/bin/juju-exec JUJU_DISPATCH_PATH=hooks/slurmctld_failover {self._charm.charm_dir}/dispatch"
        primary_on.write_text(
            textwrap.dedent(
                f"""\
                #!/bin/bash
                #/usr/bin/sudo {exec_cmd}
                """
            )
        )
        primary_on.chmod(0o755)
        # HACK FIXME: juju-exec needs to run as root but slurmctld runs as the slurm user. Give slurm user sudo access(!) here.
        Path("/etc/sudoers.d/slurm").write_text(f"slurm ALL=(root) NOPASSWD: {exec_cmd}")

        pkgs = ["nfs-kernel-server", "autofs"]
        try:
            apt.add_package(pkgs)
        except (apt.PackageNotFoundError, apt.PackageError) as e:
            raise HAOpsError(f"failed to install packages: {pkgs}. reason: {e}")

        # Don't start these services yet. All controllers install them but only backups should have them running.
        Path("/etc/systemd/system/checkpoint-sync.service").write_text(CHECKPOINT_SYNC_SERVICE)
        Path("/etc/systemd/system/checkpoint-sync.timer").write_text(CHECKPOINT_SYNC_TIMER)
        systemd.daemon_reload()

    def start_save_state_location_sync(self, active_host):
        """Start this backup synchronizing its StateSaveLocation with that of the given active controller."""
        # Stop any existing sync operations in case of failover.
        self._stop_sync()

        # All backups mount the active instances's state save location then periodically sync with their own state save location
        #Path("/etc/auto.master.d/checkpoint.autofs").write_text(CHECKPOINT_AUTOFS_MASTER)
        #Path("/etc/auto.checkpoint").write_text(
        #    f"{CHARM_MAINTAINED_SLURM_CONF_PARAMETERS['StateSaveLocation']}-active -ro,soft,retrans=1,retry=0 {active_host}:{CHARM_MAINTAINED_SLURM_CONF_PARAMETERS['StateSaveLocation']}"
        #)

        self._start_sync()

    def add_controller(self, hostname) -> None:
        """Export the Slurm StateSaveLocation to the given hostname."""
        # TODO: write an unexport function for departing units that deletes their file from /etc/exports.d and does "exportfs -a -u"
        export = Path(f"/etc/exports.d/{hostname}-checkpoint.exports")
        export.parent.mkdir(parents=True, exist_ok=True)
        export.write_text(
            f"{CHARM_MAINTAINED_SLURM_CONF_PARAMETERS['StateSaveLocation']} {hostname}(ro,no_subtree_check)\n"
        )

        try:
            subprocess.check_call(["/usr/sbin/exportfs", "-a", "-r"])
        except subprocess.CalledProcessError as e:
            raise HAOpsError(f"failed to NFS export checkpoint directory, reason: {e}")

    def get_activate_instance(self) -> Optional[str]:
        """Return the hostname of the active controller instance. Return None if no controllers are active."""
        # Example snippet of ping output:
        #   "pings": [
        #     {
        #       "hostname": "juju-829e74-84",
        #       "pinged": "DOWN",
        #       "latency": 1850,
        #       "mode": "primary"
        #     },
        #     {
        #       "hostname": "juju-829e74-85",
        #       "pinged": "UP",
        #       "latency": 1278,
        #       "mode": "backup1"
        #     },
        #     {
        #       "hostname": "juju-829e74-86",
        #       "pinged": "UP",
        #       "latency": 984,
        #       "mode": "backup2"
        #     }
        #   ],
        ping_output = json.loads(self._charm._slurmctld.scontrol("ping", "--json"))
        logger.debug("scontrol ping output: %s", ping_output)

        # Slurm fails over to controllers in order. Active instance is the first that is "UP".
        for host in ping_output["pings"]:
            if host["pinged"] == "UP":
                return host["hostname"]

        return None

    def _on_failover(self, event) -> None:
        """Promote this backup to the new active controller."""
        logger.warning("failover event occurred. this unit is now the active controller")
        self._stop_sync()

        Path("/etc/auto.master.d/checkpoint.autofs").write_text("")
        Path("/etc/auto.checkpoint").write_text("")
        systemd.service_reload("autofs", restart_on_failure=True)

        # Inform remaining units of failover
        self._charm._slurmctld_peer.failover()

    def _start_sync(self) -> None:
        """Start AutoFS mount of active controller's checkpoints directory and begin synchronization to local directory."""
        try:
            systemd.service_reload("autofs", restart_on_failure=True)
            #systemd.service_enable("checkpoint-sync.timer")
            #systemd.service_start("checkpoint-sync.timer")
        except systemd.SystemdError:
            logger.exception("failed to set up checkpoint synchronization")
            # TODO: raise exception

    def _stop_sync(self) -> None:
        """Stop synchronization of active controller's checkpoints directory and unmount."""
        systemd.service_disable("checkpoint-sync.timer")
        systemd.service_stop("checkpoint-sync.timer")
        # TODO: try this and fall back to `systemctl kill --signal=9 checkpoint-sync.service` in case of timeout?
        systemd.service_stop("checkpoint-sync.service")
        active_dir = f"{CHARM_MAINTAINED_SLURM_CONF_PARAMETERS['StateSaveLocation']}-active"
        try:
            # TODO: confirm this manual unmount is needed. Can AutoFS force unmount a stuck NFS share?
            # TODO: include --lazy?
            subprocess.check_call(["/usr/bin/umount", "--quiet", "--force", active_dir])
        except subprocess.CalledProcessError as e:
            # Unmount can fail if path is currently not mounted, e.g. unit is starting up and path hasn't been mounted before
            # --quiet suppresses "not mounted" error messages but still gives a non-zero exit code
            # Ignore errors that print no output
            if e.stderr or e.output:
                raise HAOpsError(
                    f"failed to unmount NFS checkpoint directory {active_dir}, reason: {e}"
                )
