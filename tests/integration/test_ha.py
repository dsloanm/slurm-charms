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

import logging

import json
import jubilant
import pytest
import subprocess
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


# @pytest.mark.order(1)
# def test_deploy(juju: jubilant.Juju, base, sackd, slurmctld, slurmd, slurmdbd, slurmrestd) -> None:
#     """Test if the Slurm charms can successfully reach active status."""
#     # Ceph shared storage necessary for all controller instances to share StateSaveLocation data
#     juju.deploy(
#         "microceph",
#         MICROCEPH_APP_NAME,
#         constraints={"mem": "4G", "root-disk": "20G", "virt-type": "virtual-machine"},
#         storage={"osd-standalone": "loop,2G,3"},
#     )

#     # Deploy 2 slurmctld controllers in HA configuration
#     juju.deploy(
#         slurmctld,
#         SLURMCTLD_APP_NAME,
#         base=base,
#         channel=DEFAULT_SLURM_CHARM_CHANNEL if isinstance(slurmctld, str) else None,
#         constraints={"virt-type": "virtual-machine"},
#         config={"use-network-state": True, "slurm-conf-parameters": "SlurmctldTimeout=10\n"},
#         num_units=2,
#     )

#     # Deploy remaining Slurm and auxiliary services
#     juju.deploy(
#         sackd,
#         SACKD_APP_NAME,
#         base=base,
#         channel=DEFAULT_SLURM_CHARM_CHANNEL if isinstance(sackd, str) else None,
#     )
#     juju.deploy(
#         slurmd,
#         SLURMD_APP_NAME,
#         base=base,
#         channel=DEFAULT_SLURM_CHARM_CHANNEL if isinstance(slurmd, str) else None,
#     )
#     juju.deploy(
#         slurmdbd,
#         SLURMDBD_APP_NAME,
#         base=base,
#         channel=DEFAULT_SLURM_CHARM_CHANNEL if isinstance(slurmdbd, str) else None,
#     )
#     juju.deploy(
#         slurmrestd,
#         SLURMRESTD_APP_NAME,
#         base=base,
#         channel=DEFAULT_SLURM_CHARM_CHANNEL if isinstance(slurmrestd, str) else None,
#     )
#     juju.deploy("mysql", MYSQL_APP_NAME)

#     # Integrate applications together
#     # Applications are not usable until filesystem-client is later integrated with slurmctld
#     juju.integrate(SACKD_APP_NAME, SLURMCTLD_APP_NAME)
#     juju.integrate(SLURMD_APP_NAME, SLURMCTLD_APP_NAME)
#     juju.integrate(SLURMDBD_APP_NAME, SLURMCTLD_APP_NAME)
#     juju.integrate(SLURMRESTD_APP_NAME, SLURMCTLD_APP_NAME)
#     juju.integrate(MYSQL_APP_NAME, SLURMDBD_APP_NAME)

#     # Must wait for Microceph to become active before CephFS and filesystem-client can be set up
#     juju.wait(
#         lambda status: jubilant.all_active(status, "microceph"),
#         error=jubilant.any_error,
#     )

#     # Set up CephFS
#     microceph_unit = f"{MICROCEPH_APP_NAME}/0"
#     cephfs_setup = [
#         "microceph.ceph osd pool create cephfs_data",
#         "microceph.ceph osd pool create cephfs_metadata",
#         "microceph.ceph fs new cephfs cephfs_metadata cephfs_data",
#         "microceph.ceph fs authorize cephfs client.fs-client / rw",
#     ]
#     for cmd in cephfs_setup:
#         juju.exec(cmd, unit=microceph_unit)

#     # Gather necessary config from microceph to set up filesystem charms
#     microceph_host = juju.exec("hostname -I", unit=microceph_unit).stdout.strip()
#     microceph_fsid = juju.exec(
#         "microceph.ceph -s -f json | jq -r '.fsid'", unit=microceph_unit
#     ).stdout.strip()
#     microceph_key = juju.exec(
#         "microceph.ceph auth print-key client.fs-client", unit=microceph_unit
#     ).stdout
#     juju.deploy(
#         "cephfs-server-proxy",
#         CEPHFS_SERVER_PROXY_APP_NAME,
#         channel=DEFAULT_FILESYSTEM_CHARM_CHANNEL,
#         config={
#             "fsid": microceph_fsid,
#             "sharepoint": "cephfs:/",
#             "monitor-hosts": microceph_host,
#             "auth-info": f"fs-client:{microceph_key}",
#         },
#     )
#     juju.deploy(
#         "filesystem-client",
#         FILESYSTEM_CLIENT_APP_NAME,
#         channel=DEFAULT_FILESYSTEM_CHARM_CHANNEL,
#         config={"mountpoint": FILESYSTEM_CLIENT_MOUNTPOINT},
#     )

#     # filesystem-client integrations
#     # slurmctld will exit Blocked status once integration is complete and StateSaveLocation directory is
#     # mounted
#     juju.integrate(FILESYSTEM_CLIENT_APP_NAME, CEPHFS_SERVER_PROXY_APP_NAME)
#     juju.integrate(f"{FILESYSTEM_CLIENT_APP_NAME}:juju-info", f"{SLURMCTLD_APP_NAME}:juju-info")

#     # Wait for all applications to reach active status.
#     juju.wait(jubilant.all_active)

@tenacity.retry(
    wait=tenacity.wait.wait_exponential(multiplier=2, min=1, max=10),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
)
def _get_slurm_controllers(juju: jubilant.Juju) -> dict:
    """Return a dictionary of Slurmctld statuses allowing lookup by unit, mode, and hostname."""
    login_unit = f"{SACKD_APP_NAME}/0"
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
    ping_output = json.loads(juju.exec("scontrol ping --json", unit=login_unit).stdout)
    pings = ping_output["pings"]

    # Temp dictionary for more efficient lookup of pings by hostname
    pings_by_hostname = {ping["hostname"]: ping for ping in pings}

    slurm_controllers = {}
    for unit, unit_status in status.apps[SLURMCTLD_APP_NAME].units.items():
        hostname = status.machines[unit_status.machine].instance_id
        if hostname in pings_by_hostname:
            ping_data = pings_by_hostname[hostname]
            # Add unit name and machine number to output for convenience.
            ping_data["unit"] = unit
            ping_data["machine"] = unit_status.machine

            # Use multiple keys for the same data to allow lookup by unit, mode, and hostname.
            # All are guaranteed to be unique.
            #slurm_controllers[unit] = ping_data
            slurm_controllers[ping_data["mode"]] = ping_data
            #slurm_controllers[hostname] = ping_data

    return slurm_controllers

# @pytest.mark.order(2)
# def test_slurmctld_service_failover(juju: jubilant.Juju) -> None:
#     """Test failover to backup slurmctld after stopping primary service."""
#     #juju.model = "jubilant-63315ee1"
#     login_unit = f"{SACKD_APP_NAME}/0"
#     status = juju.status()
#     controllers = _get_slurm_controllers(juju, status)
#     slurmctld_service = SLURM_APPS[SLURMCTLD_APP_NAME]

#     logger.info("checking primary and backup controllers")
#     assert controllers["primary"]["pinged"] == "UP"
#     assert controllers["backup"]["pinged"] == "UP"

#     logger.info("stopping primary controller service")
#     juju.exec(f"sudo systemctl stop {slurmctld_service}", unit=controllers["primary"]["unit"])

#     logger.info("triggering failover")
#     sinfo_result = juju.exec("sinfo", unit=login_unit, wait=30)
#     assert sinfo_result.return_code == 0

#     service_result = juju.exec(f"systemctl status {slurmctld_service}", unit=controllers["backup"]["unit"])
#     assert any("slurmctld: Running as primary controller" in l for l in service_result.stdout.splitlines()[-3:]) # -3 to avoid checking log of previous tests

# @pytest.mark.order(3)
# def test_slurmctld_service_recover(juju: jubilant.Juju) -> None:
#     """Test primary resumes control after restarting service."""
#     #juju.model = "jubilant-63315ee1"
#     login_unit = f"{SACKD_APP_NAME}/0"
#     status = juju.status()
#     controllers = _get_slurm_controllers(juju, status)
#     slurmctld_service = SLURM_APPS[SLURMCTLD_APP_NAME]

#     logger.info("checking primary and backup controllers")
#     assert controllers["primary"]["pinged"] == "DOWN"
#     assert controllers["backup"]["pinged"] == "UP"

#     logger.info("restarting primary controller service")
#     juju.exec(f"sudo systemctl restart {slurmctld_service}", unit=controllers["primary"]["unit"])

#     logger.info("testing recovery")
#     sinfo_result = juju.exec("sinfo", unit=login_unit, wait=30)
#     assert sinfo_result.return_code == 0

#     primary_service_result = juju.exec(f"systemctl status {slurmctld_service}", unit=controllers["primary"]["unit"])
#     assert any("slurmctld: Running as primary controller" in l for l in primary_service_result.stdout.splitlines()[-3:])

#     backup_service_result = juju.exec(f"systemctl status {slurmctld_service}", unit=controllers["backup"]["unit"])
#     assert any("slurmctld: slurmctld running in background mode" in l for l in backup_service_result.stdout.splitlines()[-3:])

# @pytest.mark.order(4)
# def test_slurmctld_unit_failover(juju: jubilant.Juju) -> None:
#     """Test backup takeover after powering off primary machine."""
#     #juju.model = "jubilant-63315ee1"
#     login_unit = f"{SACKD_APP_NAME}/0"
#     status = juju.status()
#     controllers = _get_slurm_controllers(juju, status)
#     slurmctld_service = SLURM_APPS[SLURMCTLD_APP_NAME]

#     logger.info("checking primary and backup controllers")
#     assert controllers["primary"]["pinged"] == "UP"
#     assert controllers["backup"]["pinged"] == "UP"

#     logger.info("powering off primary machine")
#     juju.exec(f"sudo poweroff", unit=controllers["primary"]["unit"])
#     juju.wait(lambda status: status.machines[controllers["primary"]["machine"]].juju_status.current == "down")

#     logger.info("triggering failover")
#     sinfo_result = juju.exec("sinfo", unit=login_unit, wait=30)
#     assert sinfo_result.return_code == 0

#     service_result = juju.exec(f"systemctl status {slurmctld_service}", unit=controllers["backup"]["unit"])
#     assert any("slurmctld: Running as primary controller" in l for l in service_result.stdout.splitlines()[-3:])

# @pytest.mark.order(5)
# def test_slurmctld_unit_recover(juju: jubilant.Juju) -> None:
#     """Test primary resumes control after restarting powered-off machine."""
#     #juju.model = "jubilant-63315ee1"
#     login_unit = f"{SACKD_APP_NAME}/0"
#     status = juju.status()
#     controllers = _get_slurm_controllers(juju, status)
#     slurmctld_service = SLURM_APPS[SLURMCTLD_APP_NAME]

#     logger.info("checking primary and backup controllers")
#     assert controllers["primary"]["pinged"] == "DOWN"
#     assert controllers["backup"]["pinged"] == "UP"
#     assert status.machines[controllers["primary"]["machine"]].juju_status.current == "down"

#     logger.info("rebooting primary machine")
#     subprocess.check_output(["/usr/sbin/lxc", "start", controllers["primary"]["hostname"]])
#     juju.wait(
#         lambda status: jubilant.all_active(status, SLURMCTLD_APP_NAME),
#         error=jubilant.any_error,
#     )

#     logger.info("testing recovery")
#     sinfo_result = juju.exec("sinfo", unit=login_unit, wait=30)
#     assert sinfo_result.return_code == 0

#     primary_service_result = juju.exec(f"systemctl status {slurmctld_service}", unit=controllers["primary"]["unit"])
#     assert any("slurmctld: Running as primary controller" in l for l in primary_service_result.stdout.splitlines()[-3:])

#     backup_service_result = juju.exec(f"systemctl status {slurmctld_service}", unit=controllers["backup"]["unit"])
#     assert any("slurmctld: slurmctld running in background mode" in l for l in backup_service_result.stdout.splitlines()[-3:])

# @pytest.mark.order(6)
# def test_slurmctld_scale_up(juju: jubilant.Juju) -> None:
#     """Test scaling up slurmctld by one unit."""
#     juju.model = "jubilant-e301e6eb"
#     controllers = _get_slurm_controllers(juju)

#     logger.info("checking primary and backup controllers")
#     assert controllers["primary"]["pinged"] == "UP"
#     assert controllers["backup"]["pinged"] == "UP"

#     logger.info("adding controller")
#     juju.add_unit(SLURMCTLD_APP_NAME)
#     # Wait for all Slurm apps to allow new controller hostname to propagate
#     juju.wait(lambda status: jubilant.all_active(status, *SLURM_APPS))

#     # Failover order of existing controllers must not have changed
#     # New unit must be the lowest priority backup
#     new_controllers = _get_slurm_controllers(juju)
#     assert len(new_controllers) == 3
#     assert controllers["primary"]["hostname"] == new_controllers["primary"]["hostname"]
#     assert controllers["backup"]["hostname"] == new_controllers["backup1"]["hostname"]
#     for mode in new_controllers:
#         assert new_controllers[mode]["pinged"] == "UP"

@pytest.mark.order(6)
def test_slurmctld_scale_down(juju: jubilant.Juju) -> None:
    """Test scaling down slurmctld by one unit."""
    juju.model = "jubilant-e301e6eb"
    controllers = _get_slurm_controllers(juju)

    logger.info("checking primary and 2 backup controllers")
    assert len(controllers) == 3
    for mode in controllers:
        assert controllers[mode]["pinged"] == "UP"

    logger.info("removing backup1 controller")
    juju.remove_unit(controllers["backup1"]["unit"])
    juju.wait(lambda status: len(status.apps[SLURMCTLD_APP_NAME].units) == 2)
    juju.wait(lambda status: jubilant.all_active(status, *SLURM_APPS))

    new_controllers = _get_slurm_controllers(juju)
    assert new_controllers["primary"]["hostname"] == controllers["primary"]["hostname"]
    assert new_controllers["backup"]["hostname"] == controllers["backup2"]["hostname"]
    for mode in new_controllers:
        assert new_controllers[mode]["pinged"] == "UP"

@pytest.mark.order(7)
def test_slurmctld_remove_active_primary(juju: jubilant.Juju) -> None:
    """Test removing the active slurmctld controller."""

    # TODO: THIS TEST IS FAILING AS REMOVING THE PRIMARY/LEADER BLANKS THE SLURM.CONF.
    # LOOK INTO FIXING BY MOVING LEADER ACTIONS (GENERATING SLURM.CONF) FROM START HOOK INTO LEADER-ELECTED HOOK.

    juju.model = "jubilant-e301e6eb"
    login_unit = f"{SACKD_APP_NAME}/0"
    controllers = _get_slurm_controllers(juju)

    logger.info("checking primary and backup controllers")
    assert controllers["primary"]["pinged"] == "UP"
    assert controllers["backup"]["pinged"] == "UP"

    logger.info("removing primary controller")
    juju.remove_unit(controllers["primary"]["unit"])
    juju.wait(lambda status: len(status.apps[SLURMCTLD_APP_NAME].units) == 1)
    juju.wait(lambda status: jubilant.all_active(status, *SLURM_APPS))

    logger.info("testing failover")
    sinfo_result = juju.exec("sinfo", unit=login_unit, wait=30)
    assert sinfo_result.return_code == 0

    # HA is lost at this point. Cluster is operating on a single controller
    new_controllers = _get_slurm_controllers(juju)
    assert len(controllers) == 1
    assert new_controllers["primary"]["hostname"] == controllers["backup"]["hostname"]
    assert new_controllers["primary"]["pinged"] == "UP"