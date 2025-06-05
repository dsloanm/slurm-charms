# Copyright (c) 2025 Omnivector, LLC
# See LICENSE file for licensing details.

"""SlurmctldPeer."""

import json
import logging
import secrets
from typing import Optional

from constants import CLUSTER_NAME_PREFIX
import high_availability as ha
from ops import (
    EventBase,
    EventSource,
    Object,
    ObjectEvents,
    RelationChangedEvent,
    RelationCreatedEvent,
    RelationJoinedEvent,
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
    """Emitted when a controller leader observes another controller instance."""


class SlurmctldChangedEvent(EventBase):
    """Emitted when a non-leader controller receives a configuration update from the leader."""

    def __init__(self, handle, auth_key, slurm_conf, gres_conf=None):
        super().__init__(handle)
        self.auth_key = auth_key
        self.slurm_conf = slurm_conf
        self.gres_conf = gres_conf

    def snapshot(self):
        """Snapshot the event data."""
        return {
            "auth_key": self.auth_key,
            "slurm_conf": self.slurm_conf,
            "gres_conf": self.gres_conf,
        }

    def restore(self, snapshot):
        """Restore the snapshot of the event data."""
        self.auth_key = snapshot.get("auth_key")
        self.slurm_conf = snapshot.get("slurm_conf")
        self.gres_conf = snapshot.get("gres_conf")


class Events(ObjectEvents):
    """Interface events."""

    slurmctld_available = EventSource(SlurmctldAvailableEvent)
    slurmctld_changed = EventSource(SlurmctldChangedEvent)


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

    @property
    def _relation(self):
        """Slurmctld peer relation."""
        return self.framework.model.get_relation(self._relation_name)

    def _on_relation_created(self, event: RelationCreatedEvent) -> None:
        self._relation.data[self._charm.unit]["hostname"] = self._charm.hostname

        if not self._charm.unit.is_leader():
            return

        ha_cert, ha_cert_key = ha.generate_cert()
        self._relation.data[self.model.app]["cluster_info"] = json.dumps(
            {
                "auth_key": self._charm.get_munge_key(),
                "cluster_name": f"{CLUSTER_NAME_PREFIX}-{secrets.token_urlsafe(3)}",
                "controllers": self._charm.hostname,
                "ha_cert": ha_cert,
                "ha_cert_key": ha_cert_key,
                "ha_key": ha.generate_key(),
            }
        )

    def _on_relation_joined(self, event: RelationJoinedEvent) -> None:
        # Triggered whenever a slurmctld instance observes a new instance:
        # - not triggered when there's only a single instance
        # - triggered once per instance in an HA setup (e.g. adding slurmctld/3 will trigger this method 3 times: once each on slurmctld/0, slurmctld/1, slurmctld/2)
        if not (hostname := self._relation.data[event.unit].get("hostname")):
            logger.debug(
                "joining unit %s yet to add its hostname to databag: %s. deferring event",
                event.unit,
                self._relation.data[event.unit],
            )
            event.defer()
            return

        if not self._charm.unit.is_leader():
            return

        # List dictates order that hostnames are written to slurm.conf, i.e. controller failover order.
        # Appending here ensures this unit will be the last backup.
        self.add_controller(hostname)
        self.on.slurmctld_available.emit()

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        cluster_info = json.loads(self._relation.data[self.model.app]["cluster_info"])

        # Update high availability configuration.
        ha.stop_sync()
        ha.set_key(cluster_info["ha_key"])
        ha.set_cert(cluster_info["ha_cert"], cluster_info["ha_cert_key"])
        ha.set_controllers(cluster_info["controllers"].split(","))
        ha.start_sync()

        # The leader writes out config files into the application peer relation in the main charm code so can skip the rest of this event.
        if self._charm.unit.is_leader():
            return

        # Peers (non-leaders) get their config data here from the application peer relation, as set by the leader.
        # NOTE: The charm leader is not necessarily the active slurmctld instance in an HA setup.
        # TODO clean up this series of checks - replace with a try/except?
        if "slurm_conf" not in cluster_info:
            logger.debug(
                "leader yet to add slurm configuration to peer relation. skipping event"
            )
            return

        slurm_conf = SlurmConfig.from_str(cluster_info["slurm_conf"])

        if self._charm.hostname not in slurm_conf.slurmctld_host:
            logger.debug("leader yet to add this backup host to slurm config. skipping event")
            return

        self.on.slurmctld_changed.emit(auth_key=cluster_info["auth_key"],
                                       slurm_conf=cluster_info["slurm_conf"],
                                       gres_conf=cluster_info.get("gres_conf"))

    def _property_get(self, property_name) -> Optional[str]:
        """Return the property from app relation data."""
        cluster_info = json.loads(self._relation.data[self.model.app]["cluster_info"])
        return cluster_info.get(property_name)

    def _property_set(self, property_name, property_value: str) -> None:
        """Set the property on app relation data."""
        cluster_info = json.loads(self._relation.data[self.model.app]["cluster_info"])
        cluster_info[property_name] = property_value
        self._relation.data[self.model.app]["cluster_info"] = json.dumps(cluster_info)
        logger.debug("peer relation data cluster_info set to %s", cluster_info)

    @property
    def controllers(self) -> Optional[str]:
        """Return the controllers from app relation data."""
        return self._property_get("controllers")

    def add_controller(self, hostname: str) -> None:
        """Append the given hostname to the list of controllers if not already present."""
        controllers = self._property_get("controllers")
        if hostname not in controllers:
            controllers += f",{hostname}"
            self._property_set("controllers", controllers)

    def remove_controller(self, hostname: str) -> None:
        """Remove the given hostname to the list of controllers."""
        controllers = self._property_get("controllers").split(",")
        # TODO handle ValueError if hostname not in list
        controllers.remove(hostname)
        self._property_set("controllers", ",".join(controllers))
        # TODO make this a custom event handled by main slurmctld charm?
        # Update list of controllers on other Slurm services
        self._charm._sackd.update_controllers()
        self._charm._slurmd.update_controllers()

    @property
    def auth_key(self) -> Optional[str]:
        """Return the auth_key from app relation data."""
        return self._property_get("auth_key")

    @auth_key.setter
    def auth_key(self, value: str) -> None:
        """Set the auth_key on app relation data."""
        self._property_set("auth_key", value)

    @property
    def cluster_name(self) -> Optional[str]:
        """Return the cluster_name from app relation data."""
        return self._property_get("cluster_name")

    @cluster_name.setter
    def cluster_name(self, value: str) -> None:
        """Set the cluster_name on app relation data."""
        self._property_set("cluster_name", value)

    @property
    def cluster_info(self) -> Optional[str]:
        """Return the cluster_info from app relation data."""
        return self._relation.data[self.model.app].get("cluster_info")

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
