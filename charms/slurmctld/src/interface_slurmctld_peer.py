# Copyright (c) 2025 Omnivector, LLC
# See LICENSE file for licensing details.

"""SlurmctldPeer."""

import json
import logging
import secrets
import subprocess
from pathlib import Path
from typing import Optional

from constants import (
    CHARM_MAINTAINED_SLURM_CONF_PARAMETERS,
    CLUSTER_NAME_PREFIX
)
from ops import (
    Object,
    RelationChangedEvent,
    RelationCreatedEvent,
    RelationJoinedEvent,
)
from slurmutils.models import SlurmConfig

import charms.operator_libs_linux.v1.systemd as systemd

logger = logging.getLogger()


class SlurmctldPeerError(Exception):
    """Exception raised from slurmctld-peer interface errors."""

    @property
    def message(self) -> str:
        """Return message passed as argument to exception."""
        return self.args[0]


class SlurmctldPeer(Object):
    """SlurmctldPeer Interface."""

    def __init__(self, charm, relation_name):
        """Initialize the interface."""
        super().__init__(charm, relation_name)
        self._charm = charm
        self._relation_name = relation_name

        self.framework.observe(
            self._charm.on[self._relation_name].relation_created,
            self._on_relation_created,
        )
        self.framework.observe(
            self._charm.on[self._relation_name].relation_joined,
            self._on_relation_joined,
        )
        self.framework.observe(
            self._charm.on[self._relation_name].relation_changed,
            self._on_relation_changed,
        )

    @property
    def _relation(self):
        """Slurmctld peer relation."""
        return self.framework.model.get_relation(self._relation_name)

    def _on_relation_created(self, event: RelationCreatedEvent) -> None:
        self._relation.data[self._charm.unit]["hostname"] = self._charm.hostname

        if not self._charm.unit.is_leader():
            return

        self._relation.data[self.model.app]["slurmctld_info"] = json.dumps(
            {
                "auth_key": self._charm.get_munge_key(),
                "cluster_name": f"{CLUSTER_NAME_PREFIX}-{secrets.token_urlsafe(3)}",
                "jwt_key": self._charm.get_jwt_rsa(),
            }
        )

    def _on_relation_joined(self, event: RelationJoinedEvent) -> None:
        # TODO: should this be a primary check rather than leader? Issue is cannot call get_instances() until every unit has a slurm.conf
        if not self._charm.unit.is_leader():
            return

        if "hostname" not in self._relation.data[event.unit]:
            logger.debug("joining unit %s yet to add its hostname to databag: %s. deferring event", event.unit, self._relation.data[event.unit])
            event.defer()
            return

        # TODO: consider moving this to main charm
        self._charm._on_write_slurm_conf(event)
        # TODO: update NFS share allowed hosts here too

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        # TODO: should this be a primary check rather than leader? Issue is cannot call get_instances() until every unit has a slurm.conf
        # TODO: If the primary isn't the leader, we need to write out config files but not mount/sync the checkpoint directory...
        if self._charm.unit.is_leader():
            return

        slurmctld_info = json.loads(self._relation.data[self.model.app]["slurmctld_info"])
        slurm_conf = SlurmConfig.from_str(slurmctld_info["slurm_conf"])

        if self._charm.hostname not in slurm_conf.slurmctld_host:
            logger.debug("leader yet to add this backup host to slurm config. deferring event")
            event.defer()
            return

        # TODO: emit an event here and let main charm handle this for better separation of concerns
        self._charm._slurmctld.config.dump(slurmctld_info["slurm_conf"])
        if "gres_conf" in slurmctld_info:
            self._slurmctld.gres.dump(slurmctld_info["gres_conf"])
        self._charm._slurmctld.munge.key.set(slurmctld_info["auth_key"])
        self._charm._slurmctld.jwt.set(slurmctld_info["jwt_key"])

        # Required so subsequent slurm.conf write from this unit (possible if it has taken over as primary) do not use default paths
        with self._charm._slurmctld.config.edit() as config:
            self._charm._stored.save_state_location = str(config.state_save_location)
            self._charm._stored.jwt_key_path = str(config.auth_alt_parameters["jwt_key"])

        # TODO: emit and event here and move this to main charm?
        # All backups mount the primary's state save location then periodically sync with their own state save location
        primary, _ = self._charm.get_instances()
        save_state_location = CHARM_MAINTAINED_SLURM_CONF_PARAMETERS["StateSaveLocation"]
        mount_point_primary = Path(f"{save_state_location}-primary")
        mount_point_primary.mkdir(parents=True, exist_ok=True)

        # FIXME: this only works until the charm reboots. Add to fstab or use autofs.
        try:
            subprocess.check_output(["mount", "-o", "soft", f"{primary}:{save_state_location}", mount_point_primary])
        except subprocess.CalledProcessError:
            logger.exception("mount of %s:%s to %s failed", primary, save_state_location, mount_point_primary)
            # TODO raise an exception
            return

        systemd.service_enable("checkpoint-sync.timer")
        systemd.service_start("checkpoint-sync.timer")
        self._charm._slurmctld.munge.service.restart()
        self._charm._slurmctld.service.restart()

    def _property_get(self, property_name) -> Optional[str]:
        """Return the property from app relation data."""
        slurmctld_info = json.loads(self._relation.data[self.model.app]["slurmctld_info"])
        return slurmctld_info.get(property_name)

    def _property_set(self, property_name, property_value: str) -> None:
        """Set the property on app relation data."""
        slurmctld_info = json.loads(self._relation.data[self.model.app]["slurmctld_info"])
        slurmctld_info[property_name] = property_value
        self._relation.data[self.model.app]["slurmctld_info"] = json.dumps(slurmctld_info)
        logger.debug("peer relation data slurmctld_info set to %s", slurmctld_info)

    @property
    def hostnames(self):
        return [self._charm.hostname] + [self._relation.data[unit]["hostname"] for unit in self._relation.units]

    @property
    def cluster_name(self) -> Optional[str]:
        """Return the cluster_name from app relation data."""
        return self._property_get("cluster_name")

    @cluster_name.setter
    def cluster_name(self, name: str) -> None:
        """Set the cluster_name on app relation data."""
        self._property_set("cluster_name", name)

    @property
    def auth_key(self) -> Optional[str]:
        """Return the auth_key from app relation data."""
        return self._property_get("auth_key")

    @auth_key.setter
    def auth_key(self, value: str) -> None:
        """Set the auth_key on app relation data."""
        self._property_set("auth_key", value)

    @property
    def jwt_key(self) -> Optional[str]:
        """Return the jwt_key from app relation data."""
        return self._property_get("jwt_key")

    @jwt_key.setter
    def jwt_key(self, value: str) -> None:
        """Set the jwt_key on app relation data."""
        self._property_set("jwt_key", value)

    @property
    def slurm_conf(self) -> Optional[str]:
        """Return the slurm_conf from app relation data."""
        return self._property_get("slurm_conf")

    @slurm_conf.setter
    def slurm_conf(self, value: str) -> None:
        """Set the slurm_conf on app relation data."""
        self._property_set("slurm_conf", value)

    @property
    def gres_conf(self) -> Optional[str]:
        """Return the gres_conf from app relation data."""
        return self._property_get("gres_conf")

    @gres_conf.setter
    def gres_conf(self, value: str) -> None:
        """Set the gres_conf on app relation data."""
        self._property_set("gres_conf", value)
