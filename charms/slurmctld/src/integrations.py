# Copyright 2025 Vantage Compute Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Integration interface implementation for `slurmctld-peer` interface."""

__all__ = [
    "ControllerPeerAppData",
    "SlurmctldPeerConnectedEvent",
    "SlurmctldPeer",
]

import json
import logging
import time
from dataclasses import dataclass
from typing import overload

import ops
from hpc_libs.interfaces.base import Interface
from hpc_libs.utils import leader, plog
from state import all_units_observed

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ControllerPeerAppData:
    """Data provided by the primary/leader Slurm controller, `slurmctld`.

    Attributes:
        cluster_name: The unique name of this cluster.
        restart_signal: A nonce to indicate all controllers should restart `slurmctld.service`.
    """

    cluster_name: str = ""
    restart_signal: str = ""


@dataclass(frozen=True)
class ControllerPeerUnitData:
    """Data provided by each Slurm controller (`slurmctld`) unit.

    Attributes:
        hostname: The hostname for this unit.
    """

    hostname: str = ""


def unit_data_decoder(value: str) -> str:
    # `relation.load()`'s default value decoder is json.loads. It is not able to decode the default
    # IP addresses in the unit databag.
    # E.g. The unit databag, by default, contains an entry like 'ingress-address': '10.200.245.215'.
    # This causes json.loads to fail:
    #
    # >>> json.loads("10.200.245.215")
    # [...]
    # json.decoder.JSONDecodeError: Extra data: line 1 column 7 (char 6)
    #
    # Hence this custom decoder which quotes unquoted strings.
    # The IP addresses are not used but must be decoded to avoid the above error.
    if not (value.startswith('"') and value.endswith('"')):
        value = f'"{value}"'
    return json.loads(value)


class SlurmctldPeerConnectedEvent(ops.RelationEvent):
    """Event emitted when `slurmctld` is connected to the peer integration."""


class SlurmctldJoinedEvent(ops.RelationEvent):
    """Emitted when a new `slurmctld` controller instance joins."""


class SlurmctldDepartedEvent(ops.RelationEvent):
    """Emitted when a controller leaves."""


class _SlurmctldPeerEvents(ops.CharmEvents):
    """`slurmctld` peer events."""

    slurmctld_peer_connected = ops.EventSource(SlurmctldPeerConnectedEvent)
    slurmctld_joined = ops.EventSource(SlurmctldJoinedEvent)
    slurmctld_departed = ops.EventSource(SlurmctldDepartedEvent)


class SlurmctldPeer(Interface):
    """Integration interface implementation for `slurmctld` peers."""

    on = _SlurmctldPeerEvents()  # type: ignore
    _stored = ops.StoredState()

    def __init__(self, charm: ops.CharmBase, integration_name: str) -> None:
        super().__init__(charm, integration_name)

        self._stored.set_default(
            last_restart_signal=str(),  # nonce to indicate slurmctld.service restart required
        )

        self.charm.framework.observe(
            self.charm.on[self._integration_name].relation_created,
            self._on_relation_created,
        )
        self.charm.framework.observe(
            self.charm.on[self._integration_name].relation_changed,
            self._on_relation_changed,
        )
        self.framework.observe(
            self.charm.on[self._integration_name].relation_departed,
            self._on_relation_departed,
        )

    def _on_relation_created(self, event: ops.RelationCreatedEvent) -> None:
        """Handle when `slurmctld` peer integration is first established."""
        # Each slurmctld instance must set its hostname in its own unit databag for the leader to
        # gather to assemble all SlurmctldHost entries in slurm.conf.
        # Set here rather than the emitted event handler to ensure the hostname is available before
        # the `start` hook runs.
        self.hostname = self.charm.slurmctld.hostname

        self.on.slurmctld_peer_connected.emit(event.relation)

    def _on_relation_changed(self, event: ops.RelationChangedEvent) -> None:
        """Handle when `slurmctld` peer integration data is changed.

        Handles when:
          - A slurmctld.service restart signal has been sent by the leader.
          - Unit(s) have joined the relation.
        """
        data = self.get_controller_peer_app_data()
        if not data:
            _logger.debug("no application data set in peer relation. ignoring change event")
            return

        # - A slurmctld.service restart signal has been sent by the leader.
        if data.restart_signal != self._stored.last_restart_signal:
            _logger.debug("restart signal found. restarting slurmctld")
            self._stored.last_restart_signal = data.restart_signal

            # Leader already restarted its service when reconfiguring
            if not self.unit.is_leader():
                # Emits if a start event is not already in the defer queue
                self.charm.on.start.emit()
            return

        # - Unit(s) have joined the relation.
        # Fire once the leader unit has observed relation-joined for all units
        if self.unit.is_leader() and all_units_observed(self.charm).ok:
            self.on.slurmctld_joined.emit(event.relation)
            return

    def _on_relation_departed(self, event: ops.RelationDepartedEvent) -> None:
        """Handle when a `slurmctld` unit departs the peer relation."""
        # Fire only once the leader unit has seen the last departing unit leave
        if self.unit.is_leader() and all_units_observed(self.charm).ok:

            if event.departing_unit == self.unit:
                _logger.debug(
                    "leader is departing. next elected leader will refresh controller config"
                )
                # self.charm.slurmctld.service.stop() TODO: confirm if this is needed
                return

            self.on.slurmctld_departed.emit(event.relation)

    # A generic function is used for getting peer data. These overloads ensure:
    #   - ControllerPeerAppData is returned from an application target
    #   - ControllerPeerUnitData is returned from a unit target
    @overload
    def _get_peer_data(
        self,
        target: ops.Application,
        data_type: type[ControllerPeerAppData],
        decoder,
    ) -> ControllerPeerAppData: ...
    @overload
    def _get_peer_data(
        self,
        target: ops.Unit,
        data_type: type[ControllerPeerUnitData],
        decoder,
    ) -> ControllerPeerUnitData: ...

    def _get_peer_data(
        self,
        target: ops.Application | ops.Unit,
        data_type: type[ControllerPeerAppData] | type[ControllerPeerUnitData],
        decoder=None,
    ) -> ControllerPeerAppData | ControllerPeerUnitData | None:
        """Get unit or app peer data."""
        integration = self.get_integration()
        if not integration:
            _logger.info(
                "`%s` integration is not connected. no data to retrieve for '%s'",
                self._integration_name,
                target.name,
            )
            return None

        _logger.info(
            "`%s` integration is connected. retrieving data for '%s'",
            self._integration_name,
            target.name,
        )

        if decoder is None:
            return integration.load(data_type, target)
        return integration.load(data_type, target, decoder=decoder)

    def _set_peer_data(
        self,
        target: ops.Application | ops.Unit,
        data: ControllerPeerAppData | ControllerPeerUnitData,
    ) -> None:
        """Set app or unit peer data."""
        integration = self.get_integration()
        if not integration:
            _logger.info(
                "`%s` integration not connected. not setting data for '%s'",
                self._integration_name,
                target.name,
            )
            return

        _logger.info(
            "`%s` integration is connected. setting data for '%s'",
            self._integration_name,
            target.name,
        )
        _logger.debug("`%s` data for '%s':\n%s", self._integration_name, target.name, plog(data))
        integration.save(data, target)

    @leader
    def update_controller_peer_app_data(
        self,
        cluster_name: str | None = None,
        restart_signal: str | None = None,
    ) -> None:
        """Update the controller peer data in the `slurmctld-peer` application databag.

        Args:
            cluster_name: The unique name of this cluster.
            restart_signal: A nonce to indicate all controllers should restart `slurmctld.service`.

        Warnings:
            - Only the `slurmctld` application leader can update controller peer app data.
        """
        current = self.get_controller_peer_app_data() or ControllerPeerAppData()
        data = ControllerPeerAppData(
            cluster_name=cluster_name if cluster_name is not None else current.cluster_name,
            restart_signal=(
                restart_signal if restart_signal is not None else current.restart_signal
            ),
        )
        self._set_peer_data(self.app, data)

    def get_controller_peer_app_data(self) -> ControllerPeerAppData | None:
        """Get controller peer from the `slurmctld-peer` application databag."""
        return self._get_peer_data(self.app, ControllerPeerAppData)

    def update_controller_peer_unit_data(self, hostname: str | None = None) -> None:
        """Update the controller peer data in this unit's databag.

        Only updates fields that are not None.
        """
        current = self.get_controller_peer_unit_data(self.unit) or ControllerPeerUnitData()
        data = ControllerPeerUnitData(
            hostname=hostname if hostname is not None else current.hostname,
        )
        self._set_peer_data(self.unit, data)

    def get_controller_peer_unit_data(self, unit: ops.Unit) -> ControllerPeerUnitData | None:
        """Get controller peer data from a `slurmctld-peer` unit databag.

        Args:
            unit: Unit to get the data from.

        Returns:
            The controller peer data for the given unit, or `None` if no data is set.
        """
        return self._get_peer_data(unit, ControllerPeerUnitData, unit_data_decoder)

    @property
    def cluster_name(self) -> str:
        """Get the unique cluster name from the application integration data.

        Warnings:
            - The cluster name should only be set once during the entire lifetime of
              a deployed `slurmctld` application. If the cluster name is overwritten
              after being set, this will unrecoverably corrupt the controller's
              `StateSaveLocation` data.
        """
        data = self.get_controller_peer_app_data()
        return data.cluster_name if data else ""

    @cluster_name.setter
    @leader
    def cluster_name(self, value: str) -> None:
        """Set the unique cluster name in the application integration data."""
        self.update_controller_peer_app_data(cluster_name=value)

    @property
    def hostname(self) -> str:
        """Get this unit's hostname from the unit integration data."""
        data = self.get_controller_peer_unit_data(self.unit)
        return data.hostname if data else ""

    @hostname.setter
    def hostname(self, value: str) -> None:
        """Set this unit's hostname in the unit integration data."""
        self.update_controller_peer_unit_data(hostname=value)

    @property
    def controllers(self) -> set[str]:
        """Return controller hostnames from the peer relation.

        Always includes the hostname of this unit, even when the peer relation is not established.
        This ensures a valid controller is returned when integrations with other applications, such
        as slurmd or sackd, occur first.
        """
        # self.hostname fails if relation not yet established
        controllers = {self.charm.slurmctld.hostname}

        integration = self.get_integration()
        if not integration:
            _logger.debug(
                "`%s` integration is not connected. returning local controller only",
                self._integration_name,
            )
            return controllers

        for unit in integration.units:
            data = self.get_controller_peer_unit_data(unit)
            if data and data.hostname != "":
                controllers.add(data.hostname)

        _logger.debug("returning controllers: %s", controllers)
        return controllers

    @leader
    def signal_slurmctld_restart(self) -> None:
        """Add a message to the peer relation to indicate all peers should restart slurmctld.service.

        This is a workaround for `scontrol reconfigure` not instructing all slurmctld daemons to
        re-read SlurmctldHost lines from slurm.conf.
        """
        # Current timestamp used so a unique value is written to the relation on each call.
        timestamp = str(time.time())
        self.update_controller_peer_app_data(restart_signal=timestamp)
