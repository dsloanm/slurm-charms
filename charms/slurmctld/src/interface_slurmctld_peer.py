# Copyright (c) 2025 Omnivector, LLC
# See LICENSE file for licensing details.

"""SlurmctldPeer."""

import logging
from typing import Optional

from ops import Object

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

    @property
    def _relation(self):
        """Slurmctld peer relation."""
        return self.framework.model.get_relation(self._relation_name)

    def _property_get(self, property_name) -> Optional[str]:
        """Return the property from app relation data."""
        property_value = None
        if self._relation:
            if property_from_relation := self._relation.data[self.model.app].get(
                property_name
            ):
                property_value = property_from_relation
            logger.debug(f"## `slurmctld-peer` relation available. {property_name}: {property_value}.")
        else:
            logger.debug(
                f"## `slurmctld-peer` relation not available yet, cannot get {property_name}."
            )
        return property_value

    def _property_set(self, property_name, property_value: str) -> None:
        """Set the property on app relation data."""
        if not self.framework.model.unit.is_leader():
            logger.debug(f"only leader can set {property_name}")
            return

        if not self._relation:
            raise SlurmctldPeerError(
                f"`slurmctld-peer` relation not available yet, cannot set {property_name}."
            )

        self._relation.data[self.model.app][property_name] = property_value

    @property
    def hostnames(self) -> Optional[str]:
        """Return the hostnames of all units in this relation."""
        return self._property_get("hostnames").split(",") if self._property_get("hostnames") else None

    def add_hostname(self, hostname: str) -> None:
        """Adds hostname to app relation data."""
        if not self._relation:
            raise SlurmctldPeerError(
                f"`slurmctld-peer` relation not available yet, cannot add hostname."
            )

        # TODO: ensure hostname is unique
        hostnames = self._property_get("hostnames")
        self._relation.data[self.model.app]["hostnames"] = (
            f"{hostnames},{hostname}" if hostnames else hostname
        )

    # TODO: remove_hostname()

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
    def auth_key(self, key: str) -> None:
        """Set the auth_key on app relation data."""
        self._property_set("auth_key", key)

    @property
    def jwt_key(self) -> Optional[str]:
        """Return the jwt_key from app relation data."""
        return self._property_get("jwt_key")

    @jwt_key.setter
    def jwt_key(self, key: str) -> None:
        """Set the jwt_key on app relation data."""
        self._property_set("jwt_key", key)

    @property
    def slurm_conf(self) -> Optional[str]:
        """Return the slurm_conf from app relation data."""
        return self._property_get("slurm_conf")

    @slurm_conf.setter
    def slurm_conf(self, key: str) -> None:
        """Set the slurm_conf on app relation data."""
        self._property_set("slurm_conf", key)

    @property
    def gres_conf(self) -> Optional[str]:
        """Return the gres_conf from app relation data."""
        return self._property_get("gres_conf")

    @gres_conf.setter
    def gres_conf(self, key: str) -> None:
        """Set the gres_conf on app relation data."""
        self._property_set("gres_conf", key)
