# Copyright (c) 2025 Omnivector, LLC
# See LICENSE file for licensing details.

"""SlurmctldPeer."""

import json
import logging
import secrets

from constants import CLUSTER_NAME_PREFIX
from ops import (
    EventBase,
    EventSource,
    Object,
    ObjectEvents,
    RelationChangedEvent,
    RelationCreatedEvent,
    RelationDepartedEvent,
)
from slurmutils.models import SlurmConfig

logger = logging.getLogger()


class SlurmctldPeerError(Exception):
    """Exception raised from slurmctld-peer interface errors."""

    @property
    def message(self) -> str:
        """Return message passed as argument to exception."""
        return self.args[0]


class SlurmctldAvailableEvent(EventBase):
    """Emitted when a new controller instance joins."""


class SlurmctldDepartedEvent(EventBase):
    """Emitted when a controller leaves."""


class SlurmctldPeerAvailableEvent(EventBase):
    """Emitted when a non-leader controller receives a configuration update from the leader."""


class Events(ObjectEvents):
    """Interface events."""

    slurmctld_available = EventSource(SlurmctldAvailableEvent)
    slurmctld_departed = EventSource(SlurmctldDepartedEvent)
    slurmctld_peer_available = EventSource(SlurmctldPeerAvailableEvent)


class SlurmctldPeer(Object):
    """SlurmctldPeer Interface."""

    on = Events()  # pyright: ignore [reportIncompatibleMethodOverride, reportAssignmentType]

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
            self._charm.on[self._relation_name].relation_changed,
            self._on_relation_changed,
        )

    @property
    def _relation(self):
        """Slurmctld peer relation."""
        if (relation := self.framework.model.get_relation(self._relation_name)):
            return relation
        raise SlurmctldPeerError("attempted to access peer relation before it was established")

    def _on_relation_created(self, event: RelationCreatedEvent) -> None:
        self._relation.data[self._charm.unit]["hostname"] = self._charm.hostname

        if not self._charm.unit.is_leader():
            return

        # Retrieve the cluster name from either charm config or the app relation if it's already set.
        # Otherwise, generate a new random name.
        if charm_config_cluster_name := str(self._charm.config.get("cluster-name", "")):
            cluster_name = charm_config_cluster_name
        elif cluster_json := self._relation.data[self.model.app].get("cluster_info"):
            cluster_name = json.loads(cluster_json)["cluster_name"]
        else:
            cluster_name = f"{CLUSTER_NAME_PREFIX}-{secrets.token_urlsafe(3)}"

        self._relation.data[self.model.app]["cluster_info"] = json.dumps(
            {
                "auth_key": self._charm.get_munge_key(),
                "cluster_name": cluster_name,
                "controllers": self._charm.hostname,
            }
        )

        self._relation.data[self.model.app]["slurmctld_confs"] = json.dumps(
            { "slurm": "", "acct_gather": "", "cgroup": "", "gres": "" }
        )

        logger.debug("cluster_info: %s", self._relation.data[self.model.app]["cluster_info"])

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        if self._charm.unit.is_leader():
            # Add any new instances to list of controllers.
            if event.unit and (event_hostname := self._relation.data[event.unit].get("hostname")):
                if event_hostname not in self.controllers:
                    # List dictates order that hostnames are written to slurm.conf, i.e. controller failover order.
                    # Appending here ensures this unit will be the last backup.
                    self.add_controller(event_hostname)
                    self.on.slurmctld_available.emit()
            return

        if slurmctld_confs := self._relation.data[self.model.app].get("slurmctld_confs"):
            slurmctld_confs = json.loads(slurmctld_confs)

            if slurm_str := slurmctld_confs.get("slurm"):
                slurm_conf = SlurmConfig.from_str(slurm_str)
                if self._charm.hostname in slurm_conf.slurmctld_host:
                    self.on.slurmctld_peer_available.emit()

    def _on_relation_departed(self, event: RelationDepartedEvent) -> None:
        """Handle hook when a unit departs."""
        if not self._charm.unit.is_leader():
            return

        # Remove departing unit from list of controllers by enumerating all remaining units and removing the extra hostname.
        # Required as departing unit hostname is no longer available from its databag.
        remaining_controllers = set()
        for unit in self._relation.units:
            remaining_controllers.add(self._relation.data[unit].get("hostname"))
        departing_controllers = [
            controller
            for controller in self.controllers.split(",")
            if controller not in remaining_controllers
        ]

        for controller in departing_controllers:
            self.remove_controller(controller)

        self.on.slurmctld_departed.emit()

    def _property_get(self, info_name, property_name) -> str:
        """Return the property from app relation data."""
        info = json.loads(self._relation.data[self.model.app][info_name])
        return info.get(property_name, "")

    def _property_set(self, info_name, property_name, property_value: str) -> None:
        """Set the property on app relation data."""
        info = json.loads(self._relation.data[self.model.app][info_name])
        info[property_name] = property_value
        self._relation.data[self.model.app][info_name] = json.dumps(info)
        logger.debug("peer relation data %s set to %s", info_name, info)

    @property
    def controllers(self) -> str:
        """Return the controllers from app relation data."""
        return self._property_get("cluster_info", "controllers")

    def add_controller(self, hostname: str) -> None:
        """Append the given hostname to the list of controllers if not already present."""
        controllers = self._property_get("cluster_info", "controllers")
        if hostname not in controllers:
            controllers += f",{hostname}"
            self._property_set("cluster_info", "controllers", controllers)

    def remove_controller(self, hostname: str) -> None:
        """Remove the given hostname from the list of controllers."""
        controllers = self._property_get("cluster_info", "controllers").split(",")
        try:
            controllers.remove(hostname)
            self._property_set("cluster_info", "controllers", ",".join(controllers))
        except ValueError:
            logger.warning(
                "failed to remove controller %s as not present in current list of controllers: %s",
                hostname,
                controllers,
            )

    @property
    def auth_key(self) -> str:
        """Return the auth_key from app relation data."""
        return self._property_get("cluster_info", "auth_key")

    @property
    def cluster_name(self) -> str:
        """Return the cluster_name from app relation data."""
        return self._property_get("cluster_info", "cluster_name")

    @property
    def slurmctld_confs(self) -> str:
        """Return all slurmctld_confs from app relation data."""
        return json.loads(self._relation.data[self.model.app]["slurmctld_confs"])

    @property
    def slurm_conf(self) -> str:
        """Return the slurm_conf from app relation data."""
        return self._property_get("slurmctld_confs", "slurm")

    @slurm_conf.setter
    def slurm_conf(self, value: str) -> None:
        """Set the slurm_conf on app relation data."""
        self._property_set("slurmctld_confs", "slurm", value)

    @property
    def acct_gather_conf(self) -> str:
        """Return the acct_gather_conf from app relation data."""
        return self._property_get("slurmctld_confs", "acct_gather")

    @acct_gather_conf.setter
    def acct_gather_conf(self, value: str) -> None:
        """Set the acct_gather_conf on app relation data."""
        self._property_set("slurmctld_confs", "acct_gather", value)

    @property
    def cgroup_conf(self) -> str:
        """Return the cgroup_conf from app relation data."""
        return self._property_get("slurmctld_confs", "cgroup")

    @cgroup_conf.setter
    def cgroup_conf(self, value: str) -> None:
        """Set the cgroup_conf on app relation data."""
        self._property_set("slurmctld_confs", "cgroup", value)

    @property
    def gres_conf(self) -> str:
        """Return the gres_conf from app relation data."""
        return self._property_get("slurmctld_confs", "gres")

    @gres_conf.setter
    def gres_conf(self, value: str) -> None:
        """Set the gres_conf on app relation data."""
        self._property_set("slurmctld_confs", "gres", value)
