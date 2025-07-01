#!/usr/bin/env python3
# Copyright 2023-2025 Canonical Ltd.
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

"""Slurm charm integration tests."""

import json
import logging
import subprocess

import jubilant
import pytest
import tenacity
from constants import (
    CEPHFS_SERVER_PROXY_APP_NAME,
    DEFAULT_FILESYSTEM_CHARM_CHANNEL,
    DEFAULT_SLURM_CHARM_CHANNEL,
    FILESYSTEM_CLIENT_APP_NAME,
    FILESYSTEM_CLIENT_MOUNTPOINT,
    MICROCEPH_APP_NAME,
    MYSQL_APP_NAME,
    SACKD_APP_NAME,
    SLURM_APPS,
    SLURMCTLD_APP_NAME,
    SLURMD_APP_NAME,
    SLURMDBD_APP_NAME,
    SLURMRESTD_APP_NAME,
)

logger = logging.getLogger(__name__)


def assert_pinged(controllers: dict, expected_statuses: dict):
    for name, expected in expected_statuses.items():
        actual = controllers[name]["pinged"]
        assert actual == expected, f"status for {name}: expected '{expected}', got '{actual}'"


def assert_hostname(new: dict, old: dict, mapping: dict):
    for new_key, old_key in mapping.items():
        expected = old[old_key]["hostname"]
        actual = new[new_key]["hostname"]
        assert (
            actual == expected
        ), f"hostname mismatch for {new_key} vs {old_key}: expected '{expected}', got '{actual}'"


def assert_sinfo(sinfo_result: jubilant.Task):
    assert (
        sinfo_result.return_code == 0
    ), f"`sinfo` operation status: '{sinfo_result.status}'\nstdout: {sinfo_result.stdout}\nstderr: {sinfo_result.stderr}"


def assert_powered_off(juju: jubilant.Juju, machine_id, hostname):
    assert (
        juju.status().machines[machine_id].juju_status.current == "down"
    ), f"machine '{hostname}' is not powered off"


@tenacity.retry(
    wait=tenacity.wait.wait_exponential(multiplier=2, min=1, max=10),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def _get_slurm_controllers(juju: jubilant.Juju, query_unit: str = f"{SACKD_APP_NAME}/0") -> dict:
    """Return a dictionary of Slurmctld statuses allowing lookup by mode."""
    status = juju.status()

    # Query controller status by running `scontrol ping` on the login node.
    # Example snippet of ping output:
    #   "pings": [
    #     {
    #       "hostname": "juju-829e74-84",
    #       "pinged": "DOWN",
    #       "latency": 123,
    #       "mode": "primary"
    #     },
    ping_output = json.loads(juju.exec("scontrol ping --json", unit=query_unit).stdout)
    pings = ping_output["pings"]

    # Temp dictionary for more efficient lookup of pings by hostname
    pings_by_hostname = {ping["hostname"]: ping for ping in pings}

    slurm_controllers = {}
    for unit, unit_status in status.apps[SLURMCTLD_APP_NAME].units.items():
        hostname = status.machines[unit_status.machine].instance_id
        if hostname in pings_by_hostname:
            ping_data = pings_by_hostname[hostname]
            # Unit name, leader status and machine ID added to output for test convenience
            ping_data["unit"] = unit
            ping_data["leader"] = unit_status.leader
            ping_data["machine"] = unit_status.machine
            slurm_controllers[ping_data["mode"]] = ping_data

    return slurm_controllers


@pytest.mark.order(1)
def test_deploy(juju: jubilant.Juju, base, sackd, slurmctld, slurmd, slurmdbd, slurmrestd) -> None:
    """Test if the Slurm charms can successfully reach active status."""
    # Ceph shared storage necessary for all controller instances to share StateSaveLocation data
    juju.deploy(
        "microceph",
        MICROCEPH_APP_NAME,
        constraints={"mem": "4G", "root-disk": "20G", "virt-type": "virtual-machine"},
        storage={"osd-standalone": "loop,2G,3"},
    )

    # Deploy 2 slurmctld controllers in HA configuration
    juju.deploy(
        slurmctld,
        SLURMCTLD_APP_NAME,
        base=base,
        channel=DEFAULT_SLURM_CHARM_CHANNEL if isinstance(slurmctld, str) else None,
        constraints={"virt-type": "virtual-machine"},
        config={"use-network-state": True, "slurm-conf-parameters": "SlurmctldTimeout=10\n"},
        num_units=2,
    )

    # Deploy remaining Slurm and auxiliary services
    juju.deploy(
        sackd,
        SACKD_APP_NAME,
        base=base,
        channel=DEFAULT_SLURM_CHARM_CHANNEL if isinstance(sackd, str) else None,
    )
    juju.deploy(
        slurmd,
        SLURMD_APP_NAME,
        base=base,
        channel=DEFAULT_SLURM_CHARM_CHANNEL if isinstance(slurmd, str) else None,
    )
    juju.deploy(
        slurmdbd,
        SLURMDBD_APP_NAME,
        base=base,
        channel=DEFAULT_SLURM_CHARM_CHANNEL if isinstance(slurmdbd, str) else None,
    )
    juju.deploy(
        slurmrestd,
        SLURMRESTD_APP_NAME,
        base=base,
        channel=DEFAULT_SLURM_CHARM_CHANNEL if isinstance(slurmrestd, str) else None,
    )
    juju.deploy("mysql", MYSQL_APP_NAME)

    # Integrate applications together
    # Applications are not usable until filesystem-client is later integrated with slurmctld
    juju.integrate(SACKD_APP_NAME, SLURMCTLD_APP_NAME)
    juju.integrate(SLURMD_APP_NAME, SLURMCTLD_APP_NAME)
    juju.integrate(SLURMDBD_APP_NAME, SLURMCTLD_APP_NAME)
    juju.integrate(SLURMRESTD_APP_NAME, SLURMCTLD_APP_NAME)
    juju.integrate(MYSQL_APP_NAME, SLURMDBD_APP_NAME)

    # Must wait for Microceph to become active before CephFS and filesystem-client can be set up
    juju.wait(
        lambda status: jubilant.all_active(status, "microceph"),
        error=jubilant.any_error,
    )

    # Set up CephFS
    microceph_unit = f"{MICROCEPH_APP_NAME}/0"
    cephfs_setup = [
        "microceph.ceph osd pool create cephfs_data",
        "microceph.ceph osd pool create cephfs_metadata",
        "microceph.ceph fs new cephfs cephfs_metadata cephfs_data",
        "microceph.ceph fs authorize cephfs client.fs-client / rw",
    ]
    for cmd in cephfs_setup:
        juju.exec(cmd, unit=microceph_unit)

    # Gather necessary config from microceph to set up filesystem charms
    microceph_host = juju.exec("hostname -I", unit=microceph_unit).stdout.strip()
    microceph_fsid = juju.exec(
        "microceph.ceph -s -f json | jq -r '.fsid'", unit=microceph_unit
    ).stdout.strip()
    microceph_key = juju.exec(
        "microceph.ceph auth print-key client.fs-client", unit=microceph_unit
    ).stdout
    juju.deploy(
        "cephfs-server-proxy",
        CEPHFS_SERVER_PROXY_APP_NAME,
        channel=DEFAULT_FILESYSTEM_CHARM_CHANNEL,
        config={
            "fsid": microceph_fsid,
            "sharepoint": "cephfs:/",
            "monitor-hosts": microceph_host,
            "auth-info": f"fs-client:{microceph_key}",
        },
    )
    juju.deploy(
        "filesystem-client",
        FILESYSTEM_CLIENT_APP_NAME,
        channel=DEFAULT_FILESYSTEM_CHARM_CHANNEL,
        config={"mountpoint": FILESYSTEM_CLIENT_MOUNTPOINT},
    )

    # filesystem-client integrations
    # slurmctld will exit Blocked status once integration is complete and StateSaveLocation directory is
    # mounted
    juju.integrate(FILESYSTEM_CLIENT_APP_NAME, CEPHFS_SERVER_PROXY_APP_NAME)
    juju.integrate(f"{FILESYSTEM_CLIENT_APP_NAME}:juju-info", f"{SLURMCTLD_APP_NAME}:juju-info")

    # Wait for all applications to reach active status.
    juju.wait(jubilant.all_active)


@pytest.mark.order(2)
def test_slurmctld_scale_up(juju: jubilant.Juju) -> None:
    """Test scaling up slurmctld by one unit."""
    controllers = _get_slurm_controllers(juju)
    logger.info("checking primary and backup controllers")
    assert_pinged(
        controllers,
        {
            "primary": "UP",
            "backup": "UP",
        },
    )

    logger.info("adding controller")
    juju.add_unit(SLURMCTLD_APP_NAME)
    # All Slurm apps must be waited for to allow new controller hostname to propagate
    juju.wait(lambda status: jubilant.all_active(status, *SLURM_APPS))

    # Failover order of existing controllers must not have changed
    # New unit must be the lowest priority backup
    new_controllers = _get_slurm_controllers(juju)
    assert len(new_controllers) == 3, f"expected 3 controllers, got {len(new_controllers)}"
    assert_hostname(
        new_controllers,
        controllers,
        {
            "primary": "primary",
            "backup1": "backup",
        },
    )
    assert_pinged(
        new_controllers,
        {
            "primary": "UP",
            "backup1": "UP",
            "backup2": "UP",
        },
    )


@pytest.mark.order(3)
def test_slurmctld_scale_down(juju: jubilant.Juju) -> None:
    """Test scaling down slurmctld by one unit."""
    controllers = _get_slurm_controllers(juju)

    logger.info("checking primary and 2 backup controllers")
    assert len(controllers) == 3, f"expected 3 controllers, got {len(controllers)}"
    assert_pinged(
        controllers,
        {
            "primary": "UP",
            "backup1": "UP",
            "backup2": "UP",
        },
    )

    logger.info("removing backup1 controller")
    juju.remove_unit(controllers["backup1"]["unit"])
    juju.wait(
        lambda status: len(status.apps[SLURMCTLD_APP_NAME].units) == 2
        and jubilant.all_active(status, *SLURM_APPS)
    )

    # Can take time for changes to propagate to login node. Retry if get stale controllers
    @tenacity.retry(
        wait=tenacity.wait.wait_exponential(multiplier=3, min=10, max=30),
        stop=tenacity.stop_after_attempt(10),
        reraise=True,
    )
    def retry_get_slurm_controllers(juju):
        new_controllers = _get_slurm_controllers(juju)
        assert "backup" in new_controllers
        return new_controllers

    new_controllers = retry_get_slurm_controllers(juju)
    assert_hostname(
        new_controllers,
        controllers,
        {
            "primary": "primary",
            "backup": "backup2",
        },
    )
    assert_pinged(
        new_controllers,
        {
            "primary": "UP",
            "backup": "UP",
        },
    )


@pytest.mark.order(4)
def test_slurmctld_service_failover(juju: jubilant.Juju) -> None:
    """Test failover to backup slurmctld after stopping primary service."""
    login_unit = f"{SACKD_APP_NAME}/0"
    controllers = _get_slurm_controllers(juju)
    slurmctld_service = SLURM_APPS[SLURMCTLD_APP_NAME]

    logger.info("checking primary and backup controllers")
    assert_pinged(
        controllers,
        {
            "primary": "UP",
            "backup": "UP",
        },
    )

    logger.info("stopping primary controller service")
    juju.exec(f"sudo systemctl stop {slurmctld_service}", unit=controllers["primary"]["unit"])

    logger.info("triggering failover")
    sinfo_result = juju.exec("sinfo", unit=login_unit, wait=30)
    assert_sinfo(sinfo_result)

    service_result = juju.exec(
        f"systemctl status {slurmctld_service}", unit=controllers["backup"]["unit"]
    )
    assert "slurmctld: Running as primary controller" in service_result.stdout


@pytest.mark.order(5)
def test_slurmctld_service_recover(juju: jubilant.Juju) -> None:
    """Test primary resumes control after restarting service."""
    login_unit = f"{SACKD_APP_NAME}/0"
    controllers = _get_slurm_controllers(juju)
    slurmctld_service = SLURM_APPS[SLURMCTLD_APP_NAME]

    logger.info("checking primary and backup controllers")
    assert_pinged(
        controllers,
        {
            "primary": "DOWN",
            "backup": "UP",
        },
    )

    logger.info("restarting primary controller service")
    juju.exec(f"sudo systemctl restart {slurmctld_service}", unit=controllers["primary"]["unit"])

    logger.info("testing recovery")
    sinfo_result = juju.exec("sinfo", unit=login_unit, wait=30)
    assert_sinfo(sinfo_result)

    primary_service_result = juju.exec(
        f"systemctl status {slurmctld_service}", unit=controllers["primary"]["unit"]
    )
    assert "slurmctld: Running as primary controller" in primary_service_result.stdout

    backup_service_result = juju.exec(
        f"systemctl status {slurmctld_service}", unit=controllers["backup"]["unit"]
    )
    assert "slurmctld: slurmctld running in background mode" in backup_service_result.stdout


@pytest.mark.order(6)
def test_slurmctld_unit_failover(juju: jubilant.Juju) -> None:
    """Test backup takeover after powering off primary machine."""
    login_unit = f"{SACKD_APP_NAME}/0"
    controllers = _get_slurm_controllers(juju)
    slurmctld_service = SLURM_APPS[SLURMCTLD_APP_NAME]

    logger.info("checking primary and backup controllers")
    assert_pinged(
        controllers,
        {
            "primary": "UP",
            "backup": "UP",
        },
    )

    logger.info("powering off primary machine")
    juju.exec("sudo poweroff", unit=controllers["primary"]["unit"])
    juju.wait(
        lambda status: status.machines[controllers["primary"]["machine"]].juju_status.current
        == "down"
    )

    logger.info("triggering failover")
    sinfo_result = juju.exec("sinfo", unit=login_unit, wait=30)
    assert_sinfo(sinfo_result)

    service_result = juju.exec(
        f"systemctl status {slurmctld_service}", unit=controllers["backup"]["unit"]
    )
    assert "slurmctld: Running as primary controller" in service_result.stdout


@pytest.mark.order(7)
def test_slurmctld_unit_recover(juju: jubilant.Juju) -> None:
    """Test primary resumes control after restarting powered-off machine."""
    login_unit = f"{SACKD_APP_NAME}/0"
    controllers = _get_slurm_controllers(juju)
    slurmctld_service = SLURM_APPS[SLURMCTLD_APP_NAME]

    logger.info("checking primary and backup controllers")
    assert_pinged(
        controllers,
        {
            "primary": "DOWN",
            "backup": "UP",
        },
    )
    assert_powered_off(juju, controllers["primary"]["machine"], controllers["primary"]["hostname"])

    logger.info("rebooting primary machine")
    subprocess.check_output(["/usr/sbin/lxc", "start", controllers["primary"]["hostname"]])
    juju.wait(
        lambda status: jubilant.all_active(status, SLURMCTLD_APP_NAME),
        error=jubilant.any_error,
    )

    logger.info("testing recovery")
    sinfo_result = juju.exec("sinfo", unit=login_unit, wait=30)
    assert_sinfo(sinfo_result)

    primary_service_result = juju.exec(
        f"systemctl status {slurmctld_service}", unit=controllers["primary"]["unit"]
    )
    assert "slurmctld: Running as primary controller" in primary_service_result.stdout

    backup_service_result = juju.exec(
        f"systemctl status {slurmctld_service}", unit=controllers["backup"]["unit"]
    )
    assert "slurmctld: slurmctld running in background mode" in backup_service_result.stdout


@pytest.mark.order(8)
def test_slurmctld_scale_up_degraded(juju: jubilant.Juju) -> None:
    """Test scaling up slurmctld by one unit while primary unit failed."""
    controllers = _get_slurm_controllers(juju)

    assert_pinged(controllers, {"primary": "UP"})

    logger.info("powering off primary machine")
    juju.exec("sudo poweroff", unit=controllers["primary"]["unit"])
    juju.wait(
        lambda status: status.machines[controllers["primary"]["machine"]].juju_status.current
        == "down"
    )

    logger.info("checking primary and backup controllers")
    controllers = _get_slurm_controllers(juju)
    assert_pinged(
        controllers,
        {
            "primary": "DOWN",
            "backup": "UP",
        },
    )
    assert_powered_off(juju, controllers["primary"]["machine"], controllers["primary"]["hostname"])

    logger.info("adding controller")
    juju.add_unit(SLURMCTLD_APP_NAME)

    def two_controllers_active(status: jubilant.Status) -> bool:
        """Return True if there are exactly 3 slurmctld units and 2 are active. False otherwise."""
        units = status.apps[SLURMCTLD_APP_NAME].units
        if len(units) != 3:
            return False

        active_count = sum(1 for unit in units.values() if unit.is_active)
        return active_count == 2

    juju.wait(two_controllers_active)

    new_controllers = _get_slurm_controllers(juju)
    assert len(new_controllers) == 3, f"expected 3 controllers, got {len(controllers)}"
    assert_hostname(
        new_controllers,
        controllers,
        {
            "primary": "primary",
            "backup1": "backup",
        },
    )
    assert_pinged(
        new_controllers,
        {
            "primary": "DOWN",
            "backup1": "UP",
            "backup2": "UP",
        },
    )


@pytest.mark.order(9)
def test_slurmctld_remove_failed_controller(juju: jubilant.Juju) -> None:
    """Test removing failed controller slurmctld unit."""
    status = juju.status()
    down = []
    not_down = []
    for unit, unit_status in status.apps[SLURMCTLD_APP_NAME].units.items():
        if status.machines[unit_status.machine].juju_status.current == "down":
            down.append(unit)
        else:
            not_down.append(unit)
    assert (
        len(down) == 1 and len(not_down) == 2
    ), f"expected 1 down controller and 2 others, got {len(down)} down and {len(not_down)} others"

    down_unit = down[0]
    logger.info("removing failed controller: '%s'", down_unit)
    juju.remove_unit(down_unit, force=True)  # force necessary for a failed unit
    juju.wait(lambda status: jubilant.all_active(status, *SLURM_APPS))

    @tenacity.retry(
        wait=tenacity.wait.wait_exponential(multiplier=3, min=10, max=30),
        stop=tenacity.stop_after_attempt(10),
        reraise=True,
    )
    def retry_get_slurm_controllers(juju):
        new_controllers = _get_slurm_controllers(juju)
        assert "backup" in new_controllers
        return new_controllers

    new_controllers = retry_get_slurm_controllers(juju)
    assert_pinged(
        new_controllers,
        {
            "primary": "UP",
            "backup": "UP",
        },
    )


@pytest.mark.order(10)
def test_slurmctld_remove_leader(juju: jubilant.Juju) -> None:
    """Test removing the leader slurmctld unit."""
    login_unit = f"{SACKD_APP_NAME}/0"
    controllers = _get_slurm_controllers(juju)

    logger.info("checking primary and backup controllers")
    assert_pinged(
        controllers,
        {
            "primary": "UP",
            "backup": "UP",
        },
    )

    keys = list(controllers.keys())
    mode0 = keys[0]
    mode1 = keys[1]
    if controllers[mode0]["leader"]:
        leader = mode0
        non_leader = mode1
    else:
        leader = mode1
        non_leader = mode0
    logger.info("leader controller identified as %s", controllers[leader]["unit"])

    logger.info("removing leader controller")
    juju.remove_unit(controllers[leader]["unit"])
    juju.wait(
        lambda status: len(status.apps[SLURMCTLD_APP_NAME].units) == 1
        and jubilant.all_active(status, *SLURM_APPS)
    )

    @tenacity.retry(
        wait=tenacity.wait.wait_exponential(multiplier=3, min=10, max=30),
        stop=tenacity.stop_after_attempt(10),
        reraise=True,
    )
    def retry_get_slurm_controllers(juju):
        logger.info("testing failover")
        sinfo_result = juju.exec("sinfo", unit=login_unit, wait=30)
        assert_sinfo(sinfo_result)

        new_controllers = _get_slurm_controllers(juju)
        assert len(new_controllers) == 1
        return new_controllers

    # HA is lost at this point. Cluster is operating on a single controller
    new_controllers = retry_get_slurm_controllers(juju)
    assert_hostname(new_controllers, controllers, {"primary": non_leader})
    assert_pinged(new_controllers, {"primary": "UP"})


@pytest.mark.order(11)
def test_slurmctld_sackd_scale_up(juju: jubilant.Juju) -> None:
    """Test scaling up slurmctld and sackd simultaneously."""
    new_login_unit = f"{SACKD_APP_NAME}/1"
    controllers = _get_slurm_controllers(juju)

    logger.info("checking controller")
    assert len(controllers) == 1, f"expected 1 controller, got {len(controllers)}"
    assert_pinged(controllers, {"primary": "UP"})

    logger.info("adding controller and login node")
    juju.add_unit(SLURMCTLD_APP_NAME)
    juju.add_unit(SACKD_APP_NAME)
    juju.wait(lambda status: jubilant.all_active(status, *SLURM_APPS))

    new_controllers = _get_slurm_controllers(juju, new_login_unit)
    assert len(new_controllers) == 2, f"expected 2 controllers, got {len(controllers)}"
    assert_hostname(new_controllers, controllers, {"primary": "primary"})
    assert_pinged(
        new_controllers,
        {
            "primary": "UP",
            "backup": "UP",
        },
    )


# Unreliable test. Sometimes the cluster can be recovered by adding a unit but sometimes slurmd and
# sackd experience DNS resolution issues for the downed controllers and fail to restart.
#
# @pytest.mark.order(12)
# def test_slurmctld_recover_failed_cluster(juju: jubilant.Juju) -> None:
#     """Test scaling up slurmctld while every other controller is failed."""
#     controllers = _get_slurm_controllers(juju)

#     logger.info("checking primary and backup controllers")
#     assert_pinged(
#         controllers,
#         {
#             "primary": "UP",
#             "backup": "UP",
#         },
#     )

#     logger.info("powering off controller machines")
#     juju.exec("sudo poweroff", unit=controllers["primary"]["unit"])
#     juju.exec("sudo poweroff", unit=controllers["backup"]["unit"])
#     juju.wait(
#         lambda status: status.machines[controllers["primary"]["machine"]].juju_status.current
#         == "down"
#         and status.machines[controllers["backup"]["machine"]].juju_status.current == "down"
#     )

#     logger.info("adding controller")
#     juju.add_unit(SLURMCTLD_APP_NAME)
#     juju.wait(
#         lambda status: any(
#             unit.is_active for unit in status.apps[SLURMCTLD_APP_NAME].units.values()
#         )
#     )

#     # sackd and slurmd likely fail as new controller is being set up. Wait for them to recover
#     juju.wait(lambda status: jubilant.all_active(status, SACKD_APP_NAME, SLURMD_APP_NAME))

#     new_controllers = _get_slurm_controllers(juju)
#     assert len(new_controllers) == 3, f"expected 3 controllers, got {len(controllers)}"
#     assert_hostname(
#         new_controllers,
#         controllers,
#         {
#             "primary": "primary",
#             "backup1": "backup",
#         },
#     )
#     assert_pinged(
#         new_controllers,
#         {
#             "primary": "DOWN",
#             "backup1": "DOWN",
#             "backup2": "UP",
#         },
#     )
