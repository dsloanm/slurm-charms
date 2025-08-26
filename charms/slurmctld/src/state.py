# Copyright 2025 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Manage the state of the `slurmctld` charmed operator."""

from pathlib import Path
from typing import TYPE_CHECKING

import ops
from hpc_libs.interfaces import ConditionEvaluation

if TYPE_CHECKING:
    from charm import SlurmctldCharm


def slurmctld_installed(charm: "SlurmctldCharm") -> ConditionEvaluation:
    """Check if `slurmctld` is installed on the unit."""
    installed = charm.slurmctld.is_installed()
    return ConditionEvaluation(
        installed,
        "`slurmctld` is not installed. See `juju debug-log` for details" if not installed else "",
    )


def cluster_name_set(charm: "SlurmctldCharm") -> ConditionEvaluation:
    """Check if the cluster name has been set."""
    try:
        name_set = charm.slurmctld_peer.cluster_name != ""
    except ops.RelationNotFoundError:
        name_set = False

    return ConditionEvaluation(
        name_set, "Waiting for the cluster name to be set" if not name_set else ""
    )


def config_ready(charm: "SlurmctldCharm") -> ConditionEvaluation:
    """Check if the `slurm.conf` file is ready to shared with other applications."""
    ready = charm.slurmctld.config.exists()
    return ConditionEvaluation(
        ready, "Waiting for Slurm configuration to be updated" if not ready else ""
    )


def all_units_observed(charm: "SlurmctldCharm") -> ConditionEvaluation:
    """Check if this unit has observed all other units in the peer relation."""
    planned_units = charm.model.app.planned_units() - 1  # -1 to exclude self

    try:
        observed_units = len(charm.slurmctld_peer.get_integration().units)
    except ops.RelationNotFoundError:
        observed_units = 0

    all_joined = observed_units == planned_units
    return ConditionEvaluation(
        all_joined, "Waiting for all planned units" if not all_joined else ""
    )


def shared_state_mounted(charm: "SlurmctldCharm") -> ConditionEvaluation:
    """Check if the shared state file system for high availability is mounted."""
    if charm.unit.is_leader():
        return ConditionEvaluation(True, "")

    # Check the *parent* as StateSaveLocation is a subdirectory under the shared filesystem in HA
    # e.g. "/mnt/slurmctld-statefs/checkpoint" => check if "/mnt/slurmctld-statefs" is a mount
    config = charm.slurmctld.config.load()
    state_save_parent = Path(config.state_save_location).parent
    if not state_save_parent.is_mount():
        return ConditionEvaluation(False, "A shared file system must be provided to enable `slurmctld` high availability")

    return ConditionEvaluation(True, "")


def peer_ready(charm: "SlurmctldCharm") -> ConditionEvaluation:
        """Check that this peer/non-leader can become active.

        These conditions must be satisfied:
        - slurm.conf exists
        - slurm.conf includes this unit's hostname
        - auth key exists
        - JWT key exists
        """
        if charm.unit.is_leader():
            return ConditionEvaluation(True, "")

        if not charm.slurmctld.config.path.exists():
            return ConditionEvaluation(False, f"Waiting for {charm.slurmctld.config.path}")

        config = charm.slurmctld.config.load()
        if charm.slurmctld.hostname not in config.slurmctld_host:
            return ConditionEvaluation(False, f"Waiting for {charm.slurmctld.hostname} to be added to {charm.slurmctld.config.path}")

        if not charm.slurmctld.key.path.exists():
            return ConditionEvaluation(False, f"Waiting for {charm.slurmctld.key.path}")

        if not charm.slurmctld.jwt.path.exists():
            return ConditionEvaluation(False, f"Waiting for {charm.slurmctld.jwt.path}")

        return ConditionEvaluation(True, "")


def slurmctld_ready(charm: "SlurmctldCharm") -> bool:
    """Check if the `slurmctld` service is ready to integrate with other applications.

    Required conditions:
        1. `slurmctld` is installed on the unit.
        2. Cluster name is set in the `slurmctld-peer` integration.
        3. `slurmctld` service is active.
    """
    return all(
        (
            slurmctld_installed(charm).ok,
            cluster_name_set(charm).ok,
            charm.slurmctld.service.is_active(),
        )
    )


def check_slurmctld(charm: "SlurmctldCharm") -> ops.StatusBase:
    """Determine the state of the `slurmctld` application/unit based on satisfied conditions."""
    ok, message = slurmctld_installed(charm)
    if not ok:
        return ops.BlockedStatus(message)

    ok, message = cluster_name_set(charm)
    if not ok:
        return ops.WaitingStatus(message)

    if not charm.slurmctld.service.is_active():
        return ops.WaitingStatus("Waiting for `slurmctld` to start")

    try:
        status = charm.get_controller_status(charm.slurmctld.hostname)
    except:
        # Failure to query controller active status
        status = ""

    return ops.ActiveStatus(status)
