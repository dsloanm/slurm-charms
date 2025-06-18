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
            self._charm.on[self._relation_name].relation_changed,
            self._on_relation_changed,
        )
        self.framework.observe(
            self._charm.on[self._relation_name].relation_departed,
            self._on_relation_departed,
        )

    @property
    def _relation(self):
        """Slurmctld peer relation."""
        if relation := self.framework.model.get_relation(self._relation_name):
            return relation
        raise SlurmctldPeerError("attempted to access peer relation before it was established")

    def _on_relation_created(self, event: RelationCreatedEvent) -> None:
        self._relation.data[self._charm.unit]["hostname"] = self._charm.hostname

        if not self._charm.unit.is_leader():
            return

        # TODO: Remove everything auth_key related once rebased on auth/slurm.
        # "cluster_info" can already be in the relation if a new unit is elected leader as it is starting,
        # e.g. if all other slurmctld instances are down and a new one is added.
        if "cluster_info" in self._relation.data[self.model.app]:
            logger.debug("cluster_info already exists in peer relation. skipping initialization")
            return

        self._relation.data[self.model.app]["cluster_info"] = json.dumps(
            {
                "auth_key": self._charm.get_munge_key(),
            }
        )

        logger.debug("cluster_info: %s", self._relation.data[self.model.app]["cluster_info"])

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        # Fire only once the leader unit has completed relation-joined for all units.
        if self._charm.unit.is_leader() and self.all_units_observed():
            self.on.slurmctld_available.emit()
            return

        # TODO: remove this once rebased with auth/slurm changes.
        if cluster_info := self._relation.data[self.model.app].get("cluster_info"):
            cluster_info = json.loads(cluster_info)
            if auth_key := cluster_info.get("auth_key"):
                self._charm._slurmctld.munge.key.set(auth_key)

    def _on_relation_departed(self, event: RelationDepartedEvent) -> None:
        """Handle hook when a unit departs."""
        # Fire only once the leader unit has seen the last departing unit leave.
        if self._charm.unit.is_leader() and self.all_units_observed():
            self.on.slurmctld_departed.emit()

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Clear the cluster info if the relation is broken."""
        if self.framework.model.unit.is_leader():
            event.relation.data[self.model.app]["cluster_info"] = ""

    def _property_get(self, info_name, property_name) -> str:
        """Return the property from app relation data."""
        info = json.loads(self._relation.data[self.model.app][info_name])
        return info.get(property_name, "")

    def all_units_observed(self) -> bool:
        """Return True if this unit has observed all other units in the peer relation. False otherwise."""
        seen_units = len(self._relation.units)
        planned_units = self.model.app.planned_units()-1 # -1 as includes self
        logger.debug("seen %s slurmctld unit(s) of planned %s", seen_units, planned_units)
        return seen_units == planned_units

    @property
    def controllers(self) -> list:
        """Return the list of controllers."""
        logger.debug("gathering controller hostnames from peer relation: %s", self._relation.data)
        return [data["hostname"] for data in self._relation.data.values() if "hostname" in data]

    @property
    def auth_key(self) -> str:
        """Return the auth_key from app relation data."""
        return self._property_get("cluster_info", "auth_key")
