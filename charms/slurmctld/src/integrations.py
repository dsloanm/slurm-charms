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

import logging
from dataclasses import dataclass
import time
from typing import overload

import ops
from hpc_libs.interfaces.base import Interface
from hpc_libs.utils import leader, plog

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


class SlurmctldPeerConnectedEvent(ops.RelationEvent):
    """Event emitted when `slurmctld` is connected to the peer integration."""


class _SlurmctldPeerEvents(ops.CharmEvents):
    """`slurmctld` peer events."""

    slurmctld_peer_connected = ops.EventSource(SlurmctldPeerConnectedEvent)


class SlurmctldPeer(Interface):
    """Integration interface implementation for `slurmctld` peers."""

    on = _SlurmctldPeerEvents()  # type: ignore

    def __init__(self, charm: ops.CharmBase, integration_name: str) -> None:
        super().__init__(charm, integration_name)

        self.charm.framework.observe(
            self.charm.on[self._integration_name].relation_created,
            self._on_relation_created,
        )

    @leader
    def _on_relation_created(self, event: ops.RelationCreatedEvent) -> None:
        """Handle when `slurmctld` peer integration is first established."""
        self.on.slurmctld_peer_connected.emit(event.relation)

    # A generic function is used for getting peer data. These overloads ensure:
    #   - ControllerPeerAppData is returned from an application target
    #   - ControllerPeerUnitData is returned from a unit target
    @overload
    def _get_peer_data(self, target: ops.Application, data_type: type[ControllerPeerAppData]) -> ControllerPeerAppData: ...
    @overload
    def _get_peer_data(self, target: ops.Unit, data_type: type[ControllerPeerUnitData]) -> ControllerPeerUnitData: ...

    def _get_peer_data(self,
                       target: ops.Application | ops.Unit,
                       data_type: type[ControllerPeerAppData] | type[ControllerPeerUnitData]
                      ) -> ControllerPeerAppData | ControllerPeerUnitData | None:
        """Get unit or app peer data."""
        integration = self.get_integration()
        if not integration:
            _logger.info(
                "`%s` integration is not connected. no data to retrieve for '%s'",
                self._integration_name, target.name,
            )
            return None

        _logger.info(
            "`%s` integration is connected. retrieving data for '%s'",
            self._integration_name, target.name,
        )
        return integration.load(data_type, target)

    def _set_peer_data(self,
                       target: ops.Application | ops.Unit,
                       data: ControllerPeerAppData | ControllerPeerUnitData) -> None:
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
    def set_controller_peer_app_data(self, data: ControllerPeerAppData, /) -> None:
        """Set the controller peer data in the `slurmctld-peer` application databag.

        Args:
            data: Controller peer data to set in the peer integration's application databag.

        Warnings:
            - Only the `slurmctld` application leader can set controller peer app data.
        """
        self._set_peer_data(self.app, data)

    def get_controller_peer_app_data(self) -> ControllerPeerAppData | None:
        """Get controller peer from the `slurmctld-peer` application databag."""
        return self._get_peer_data(self.app, ControllerPeerAppData)

    def set_controller_peer_unit_data(self, data: ControllerPeerUnitData, unit: ops.Unit, /) -> None:
        """Set the controller peer data in a `slurmctld-peer` unit databag.

        Args:
            unit: Unit to set the data in.
            data: Controller peer data to set in the unit databag.
        """
        self._set_peer_data(unit, data)

    def get_controller_peer_unit_data(self, unit: ops.Unit) -> ControllerPeerUnitData | None:
        """Get controller peer data from a `slurmctld-peer` unit databag.

        Args:
            unit: Unit to get the data from.

        Returns:
            The controller peer data for the given unit, or `None` if no data is set.
        """
        return self._get_peer_data(unit, ControllerPeerUnitData)

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
        self.set_controller_peer_app_data(ControllerPeerAppData(cluster_name=value))

    @property
    def hostname(self) -> str:
        """Get this unit's hostname from the unit integration data."""
        data = self.get_controller_peer_unit_data(self.unit)
        return data.hostname if data else ""

    @hostname.setter
    def hostname(self, value: str) -> None:
        """Set this unit's hostname in the unit integration data."""
        self.set_controller_peer_unit_data(ControllerPeerUnitData(hostname=value), self.unit)

    @property
    def controllers(self) -> set[str]:
        """Return controller hostnames from the peer relation.

        Always includes the hostname of this unit, even when the peer relation is not established.
        This ensures a valid controller is returned when integrations with other applications, such
        as slurmd or sackd, occur first.
        """
        controllers = {self.hostname}

        integration = self.get_integration()
        if not integration:
            _logger.debug(
                "`%s` integration is not connected. returning local controller only",
                self._integration_name,
            )
            return controllers

        for unit in integration.units:
            hostname = integration.load(ControllerPeerUnitData, unit).hostname
            if hostname != "":
                controllers.add(hostname)

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
        self.set_controller_peer_app_data(ControllerPeerAppData(restart_signal=timestamp))
