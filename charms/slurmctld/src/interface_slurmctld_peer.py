# Copyright (c) 2025 Omnivector, LLC
# See LICENSE file for licensing details.

"""SlurmctldPeer."""

import json
import logging

from ops import (
    EventBase,
    EventSource,
    Object,
    ObjectEvents,
    RelationBrokenEvent,
    RelationChangedEvent,
    RelationCreatedEvent,
    RelationDepartedEvent,
    RelationJoinedEvent,
)

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


class Events(ObjectEvents):
    """Interface events."""

    slurmctld_available = EventSource(SlurmctldAvailableEvent)
    slurmctld_departed = EventSource(SlurmctldDepartedEvent)


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
            self._charm.on[self._relation_name].relation_joined,
            self._on_relation_joined,
        )
        self.framework.observe(
            self._charm.on[self._relation_name].relation_changed,
            self._on_relation_changed,
        )
        self.framework.observe(
            self._charm.on[self._relation_name].relation_departed,
            self._on_relation_departed,
        )
        self.framework.observe(
            self._charm.on[self._relation_name].relation_broken,
            self._on_relation_broken,
        )

    @property
    def _relation(self):
        """Slurmctld peer relation."""
        if relation := self.framework.model.get_relation(self._relation_name):
            return relation
        raise SlurmctldPeerError("attempted to access peer relation before it was established")

    def _on_relation_created(self, event: RelationCreatedEvent) -> None:
        if not self._charm.unit.is_leader():
            return

        # TODO: Remove everything auth_key related once rebased on auth/slurm.
        # "cluster_info" can already be in the relation if a new unit is elected leader as it is starting,
        # e.g. if all other slurmctld instances are down and a new one is added.
        address = self._charm._ingress_address
        if "cluster_info" in self._relation.data[self.model.app]:
            logger.debug(
                "cluster_info already exists in peer relation. updating with self: %s", address
            )
            cluster_info = json.loads(self._relation.data[self.model.app]["cluster_info"])
            cluster_info["new_controllers"] = cluster_info.get("new_controllers", []) + [address]
            self._relation.data[self.model.app]["cluster_info"] = json.dumps(cluster_info)
            return

        self._relation.data[self.model.app]["cluster_info"] = json.dumps(
            {
                "auth_key": self._charm.get_munge_key(),
                "new_controllers": [address],
            }
        )

        logger.debug("cluster_info: %s", self._relation.data[self.model.app]["cluster_info"])

    def _on_relation_joined(self, event: RelationJoinedEvent) -> None:
        if not self._charm.unit.is_leader():
            return

        # New controllers are added to peer relation to be picked up next time slurm.conf is written.
        address = self._relation.data[event.unit]["ingress-address"]
        cluster_info = json.loads(self._relation.data[self.model.app]["cluster_info"])
        cluster_info["new_controllers"] = cluster_info.get("new_controllers", []) + [address]
        self._relation.data[self.model.app]["cluster_info"] = json.dumps(cluster_info)

        logger.debug("cluster_info: %s", self._relation.data[self.model.app]["cluster_info"])

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        if self._charm.unit.is_leader():
            self.on.slurmctld_available.emit()
            return

        # TODO: remove this once rebased with auth/slurm changes.
        if cluster_info := self._relation.data[self.model.app].get("cluster_info"):
            cluster_info = json.loads(cluster_info)
            if auth_key := cluster_info.get("auth_key"):
                self._charm._slurmctld.munge.key.set(auth_key)

    def _on_relation_departed(self, event: RelationDepartedEvent) -> None:
        """Handle hook when a unit departs."""
        if not self._charm.unit.is_leader():
            return

        # Departing controllers are added to peer relation to be picked up next time slurm.conf is written.
        address = self._relation.data[event.unit]["ingress-address"]
        cluster_info = json.loads(self._relation.data[self.model.app]["cluster_info"])
        cluster_info["departing_controllers"] = cluster_info.get("departing_controllers", []) + [
            address
        ]
        self._relation.data[self.model.app]["cluster_info"] = json.dumps(cluster_info)

        logger.debug("cluster_info: %s", self._relation.data[self.model.app]["cluster_info"])

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Clear the cluster info if the relation is broken."""
        if self.framework.model.unit.is_leader():
            event.relation.data[self.model.app]["cluster_info"] = ""

    def _property_get(self, info_name, property_name) -> str:
        """Return the property from app relation data."""
        info = json.loads(self._relation.data[self.model.app][info_name])
        return info.get(property_name, "")

    def get_controller_changes(self):
        """Return both the list of newly joining controllers and the list of departing controllers."""
        cluster_info = json.loads(self._relation.data[self.model.app]["cluster_info"])
        return cluster_info.get("new_controllers", []), cluster_info.get(
            "departing_controllers", []
        )

    def clear_controller_changes(self):
        """Clear lists of new and departing controllers."""
        cluster_info = json.loads(self._relation.data[self.model.app]["cluster_info"])
        cluster_info.pop("new_controllers", None)
        cluster_info.pop("departing_controllers", None)
        self._relation.data[self.model.app]["cluster_info"] = json.dumps(cluster_info)

    @property
    def auth_key(self) -> str:
        """Return the auth_key from app relation data."""
        return self._property_get("cluster_info", "auth_key")
