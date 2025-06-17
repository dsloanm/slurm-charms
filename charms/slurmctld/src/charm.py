#!/usr/bin/env python3
# Copyright 2020-2025 Omnivector, LLC
# See LICENSE file for licensing details.

"""SlurmctldCharm."""

import json
import logging
import secrets
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from constants import (
    CHARM_MAINTAINED_CGROUP_CONF_PARAMETERS,
    CHARM_MAINTAINED_SLURM_CONF_PARAMETERS,
    CLUSTER_NAME_PREFIX,
    PEER_RELATION,
)
from exceptions import IngressAddressUnavailableError
from hpc_libs.is_container import is_container
from hpc_libs.slurm_ops import SlurmctldManager, SlurmOpsError
from interface_influxdb import InfluxDB, InfluxDBAvailableEvent, InfluxDBUnavailableEvent
from interface_sackd import Sackd
from interface_slurmctld_peer import (
    SlurmctldAvailableEvent,
    SlurmctldDepartedEvent,
    SlurmctldPeer,
    SlurmctldPeerError,
)
from interface_slurmd import (
    PartitionAvailableEvent,
    PartitionUnavailableEvent,
    Slurmd,
    SlurmdAvailableEvent,
    SlurmdDepartedEvent,
)
from interface_slurmdbd import Slurmdbd, SlurmdbdAvailableEvent, SlurmdbdUnavailableEvent
from interface_slurmrestd import Slurmrestd, SlurmrestdAvailableEvent
from netifaces import interfaces
from ops import (
    ActionEvent,
    ActiveStatus,
    BlockedStatus,
    CharmBase,
    ConfigChangedEvent,
    InstallEvent,
    StartEvent,
    StoredState,
    UpdateStatusEvent,
    WaitingStatus,
    main,
)
from slurmutils.models import AcctGatherConfig, CgroupConfig, GRESConfig, GRESNode, SlurmConfig

from charms.grafana_agent.v0.cos_agent import COSAgentProvider

logger = logging.getLogger()


class SlurmctldCharm(CharmBase):
    """Slurmctld lifecycle events."""

    _stored = StoredState()

    def __init__(self, *args):
        """Init _stored attributes and interfaces, observe events."""
        super().__init__(*args)

        self._stored.set_default(
            default_partition=str(),
            jwt_key=str(),
            jwt_key_path=str(),
            munge_key=str(),
            new_nodes=[],
            nhc_params=str(),
            save_state_location=str(),
            slurm_installed=False,
            slurmdbd_host=str(),
            user_supplied_slurm_conf_params=str(),
            acct_gather_params={},
            job_profiling_slurm_conf={},
        )

        self._slurmctld = SlurmctldManager(snap=False)
        self._slurmctld_peer = SlurmctldPeer(self, PEER_RELATION)
        self._sackd = Sackd(self, "login-node")
        self._slurmd = Slurmd(self, "slurmd")
        self._slurmdbd = Slurmdbd(self, "slurmdbd")
        self._slurmrestd = Slurmrestd(self, "slurmrestd")
        self._grafana_agent = COSAgentProvider(
            self,
            metrics_endpoints=[{"path": "/metrics", "port": 9092}],
            metrics_rules_dir="./src/cos/alert_rules/prometheus",
            dashboard_dirs=["./src/cos/grafana_dashboards"],
            recurse_rules_dirs=True,
        )
        self._influxdb = InfluxDB(self, "influxdb")

        event_handler_bindings = {
            self.on.install: self._on_install,
            self.on.start: self._on_start,
            self.on.update_status: self._on_update_status,
            self.on.config_changed: self._on_config_changed,
            self._slurmctld_peer.on.slurmctld_available: self._on_slurmctld_changed,
            self._slurmctld_peer.on.slurmctld_departed: self._on_slurmctld_changed,
            self._slurmdbd.on.slurmdbd_available: self._on_slurmdbd_available,
            self._slurmdbd.on.slurmdbd_unavailable: self._on_slurmdbd_unavailable,
            self._slurmd.on.partition_available: self._on_write_slurm_conf,
            self._slurmd.on.partition_unavailable: self._on_write_slurm_conf,
            self._slurmd.on.slurmd_available: self._on_slurmd_available,
            self._slurmd.on.slurmd_departed: self._on_slurmd_departed,
            self._slurmrestd.on.slurmrestd_available: self._on_slurmrestd_available,
            self._influxdb._on.influxdb_available: self._on_influxdb_available,
            self._influxdb._on.influxdb_unavailable: self._on_influxdb_unavailable,
            self.on.show_current_config_action: self._on_show_current_config_action,
            self.on.drain_action: self._on_drain_nodes_action,
            self.on.resume_action: self._on_resume_nodes_action,
        }
        for event, handler in event_handler_bindings.items():
            self.framework.observe(event, handler)

    def _on_install(self, event: InstallEvent) -> None:
        """Perform installation operations for slurmctld."""
        self.unit.status = WaitingStatus("installing slurmctld")
        try:
            self._slurmctld.install()
            self._slurmctld.exporter.args = [
                "-slurm.collect-diags",
                "-slurm.collect-limits",
            ]

            if self.unit.is_leader():
                # Auth key can already be in the relation if this is a new unit that is elected
                # leader while being added to an existing deployment, e.g. in an HA setup while all
                # other units are down.
                try:
                    self._slurmctld.munge.key.set(self._slurmctld_peer.auth_key)
                    logger.debug("using existing auth key from peer relation")
                except SlurmctldPeerError:
                    # TODO: https://github.com/charmed-hpc/slurm-charms/issues/38 -
                    #  Use Juju Secrets instead of StoredState for exchanging keys between units.
                    self._slurmctld.munge.key.generate()

            self.unit.set_workload_version(self._slurmctld.version())
            self.slurm_installed = True
        except SlurmOpsError as e:
            logger.error(e.message)
            event.defer()
            return

    def _configure_checkpoint_mount(self, event: StartEvent) -> None:
        """Configure the network mount for StateSaveLocation data."""
        state_save_location = Path(CHARM_MAINTAINED_SLURM_CONF_PARAMETERS["StateSaveLocation"])
        if not state_save_location.is_mount():
            self.unit.status = BlockedStatus(f"waiting for {state_save_location} to be mounted")
            event.defer()
            return

        # In an HA setup, all instances require access to identical conf files.
        # To accomplish this, the leader migrates /etc/slurm to the shared storage then everyone replaces their /etc/slurm with a symlink to the new location.
        etc_target = state_save_location / "etc-slurm"
        # TODO: pull this from hpclibs? May be different for Slurm snap.
        etc_source = Path("/etc/slurm")
        if self.unit.is_leader() and not etc_target.exists() and etc_source.is_dir():
            logger.debug("migrating %s to %s", etc_source, etc_target)
            shutil.move(etc_source, etc_target)
        if not (etc_source.is_symlink() and etc_source.resolve() == etc_target):
            logger.debug("removing any existing %s", etc_source)
            try:
                shutil.rmtree(etc_source)
            except FileNotFoundError:
                pass

            logger.debug("symlinking %s to %s", etc_source, etc_target)
            etc_source.symlink_to(etc_target)

        # TODO: hpc-libs edits needed to fix permissions on mountpoint? Or change StateSaveLocation to a subdirectory of the mount point?
        shutil.chown(state_save_location, "slurm", "slurm")

    def _on_start(self, event: StartEvent) -> None:
        """Set cluster_name and write slurm.conf.

        Notes:
            - The start hook can execute multiple times in a charms lifecycle,
              for example, after a reboot of the underlying instance. This code safeguards
              against the potentiality of changing the cluster_name in subsequent start hook
              executions by applying logic that ensures the cluster_name is only set on the
              first execution of this hook, we log and return on any subsequent start hook
              event executions.
        """
        if self.config.get("use-network-state"):
            self._configure_checkpoint_mount(event)

        if self.unit.is_leader():
            if not self._slurmctld.jwt.path.exists():
                self._slurmctld.jwt.generate()
            self._on_write_slurm_conf(event)
        # Peers defer until leader writes out keys and configuration files
        else:
            if not self._slurmctld.config.path.exists():
                logger.debug("%s not found. deferring event", self._slurmctld.config.path)
                event.defer()
                return

            with self._slurmctld.config.edit() as config:
                logger.debug("checking for %s in slurmctld_hosts: %s", self.hostname, config.slurmctld_host)
                if self.hostname in config.slurmctld_host:
                    logger.debug("found %s in slurm.conf. setting controller ready", self.hostname)
                else:
                    logger.debug("%s not in %s. deferring event", self.hostname, self._slurmctld.config.path)
                    event.defer()
                    return

            if not self._slurmctld.jwt.path.exists():
                logger.debug("JWT key %s not found. deferring event", self._slurmctld.jwt.path)
                event.defer()
                return

        try:
            self._slurmctld.munge.service.restart()
            self._slurmctld.service.enable()
            self._slurmctld.service.restart()
            self._slurmctld.exporter.service.enable()
            self._slurmctld.exporter.service.restart()
            self._check_status()
        except SlurmOpsError as e:
            logger.error(e.message)
            event.defer()

    def _on_config_changed(self, event: ConfigChangedEvent) -> None:
        """Perform config-changed operations."""
        charm_config_nhc_params = str(self.config.get("health-check-params", ""))
        if (charm_config_nhc_params != self._stored.nhc_params) and (
            charm_config_nhc_params != ""
        ):
            logger.debug("## NHC user supplied params changed, sending to slurmd.")
            self._stored.nhc_params = charm_config_nhc_params
            # Send the custom NHC parameters to all slurmd.
            self._slurmd.set_nhc_params(charm_config_nhc_params)

        write_slurm_conf = False
        if charm_config_default_partition := self.config.get("default-partition"):
            if charm_config_default_partition != self._stored.default_partition:
                logger.debug("## Default partition configuration changed.")
                self._stored.default_partition = charm_config_default_partition
                write_slurm_conf = True

        if (
            charm_config_slurm_conf_params := self.config.get("slurm-conf-parameters")
        ) is not None:
            if charm_config_slurm_conf_params != self._stored.user_supplied_slurm_conf_params:
                logger.debug("## User supplied parameters changed.")
                self._stored.user_supplied_slurm_conf_params = charm_config_slurm_conf_params
                write_slurm_conf = True

        if write_slurm_conf:
            logger.debug("## Emitting write-slurm-config event.")
            self._on_write_slurm_conf(event)

    def _on_update_status(self, _: UpdateStatusEvent) -> None:
        """Handle update status."""
        self._check_status()

    def _on_show_current_config_action(self, event: ActionEvent) -> None:
        """Show current slurm.conf."""
        event.set_results({"slurm.conf": str(self._slurmctld.config.load())})

    def _on_slurmctld_changed(self, event: Union[SlurmctldAvailableEvent, SlurmctldDepartedEvent]) -> None:
        """Update slurmctld configuration and list of controllers on all Slurm services."""
        if not self._check_status():
            logger.debug("attempted slurmctld relation change while unit is not ready. deferring event")
            event.defer()
            return

        # Only slurm.conf update needed. New slurmctld controllers joining or leaving do not change any other conf file.
        self._on_write_slurm_conf(event)
        self._sackd.update_controllers()
        self._slurmd.update_controllers()

    def _on_slurmrestd_available(self, event: SlurmrestdAvailableEvent) -> None:
        """Check that we have slurm_config when slurmrestd available otherwise defer the event."""
        if self.unit.is_leader():
            if self._check_status():
                if self._stored.slurmdbd_host != "":
                    self._slurmrestd.set_slurm_config_on_app_relation_data(
                        str(self._slurmctld.config.load()),
                    )
                    return
                else:
                    logger.debug("Need slurmdbd for slurmrestd relation.")
                    event.defer()
                    return
            else:
                logger.debug("Cluster not ready yet, deferring event.")
                event.defer()

    def _on_slurmdbd_available(self, event: SlurmdbdAvailableEvent) -> None:
        self._stored.slurmdbd_host = event.slurmdbd_host
        self._on_write_slurm_conf(event)

    def _on_slurmdbd_unavailable(self, event: SlurmdbdUnavailableEvent) -> None:
        self._stored.slurmdbd_host = ""
        self._on_write_slurm_conf(event)

    def _on_influxdb_available(self, event: InfluxDBAvailableEvent) -> None:
        """Assemble the influxdb acct_gather.conf options."""
        logger.debug("## Generating acct gather configuration")

        acct_gather_params = {
            "ProfileInfluxDBDefault": ["ALL"],
            "ProfileInfluxDBHost": event.influxdb_host,
            "ProfileInfluxDBUser": event.influxdb_user,
            "ProfileInfluxDBPass": event.influxdb_pass,
            "ProfileInfluxDBDatabase": event.influxdb_database,
            "ProfileInfluxDBRTPolicy": event.influxdb_policy,
            "SysfsInterfaces": interfaces(),
        }
        logger.debug(f"## _stored.acct_gather_params: {acct_gather_params}")
        self._stored.acct_gather_params = acct_gather_params

        self._stored.job_profiling_slurm_conf = {
            "AcctGatherProfileType": "acct_gather_profile/influxdb",
            "AcctGatherInterconnectType": "acct_gather_interconnect/sysfs",
            "AccountingStorageTRES": ["ic/sysfs"],
            "AcctGatherNodeFreq": "30",
            "JobAcctGatherFrequency": {"task": "5", "network": "5"},
            "JobAcctGatherType": (
                "jobacct_gather/linux" if is_container() else "jobacct_gather/cgroup"
            ),
        }
        logger.debug(f"## SlurmConfProfilingData: {self._stored.job_profiling_slurm_conf}")
        self._on_write_slurm_conf(event)

    def _on_influxdb_unavailable(self, event: InfluxDBUnavailableEvent) -> None:
        """Clear the acct_gather.conf options on departed relation."""
        logger.debug("## Clearing acct_gather configuration")

        self._stored.acct_gather_params = {}
        self._stored.job_profiling_slurm_conf = {}
        self._on_write_slurm_conf(event)

    def _on_drain_nodes_action(self, event: ActionEvent) -> None:
        """Drain specified nodes."""
        nodes = event.params["nodename"]
        reason = event.params["reason"]

        logger.debug(f"#### Draining {nodes} because {reason}.")
        event.log(f"Draining {nodes} because {reason}.")

        try:
            cmd = f'scontrol update nodename={nodes} state=drain reason="{reason}"'
            subprocess.check_output(shlex.split(cmd))
            event.set_results({"status": "draining", "nodes": nodes})
        except subprocess.CalledProcessError as e:
            event.fail(message=f"Error draining {nodes}: {e.output}")

    def _on_resume_nodes_action(self, event: ActionEvent) -> None:
        """Resume specified nodes."""
        nodes = event.params["nodename"]

        logger.debug(f"#### Resuming {nodes}.")
        event.log(f"Resuming {nodes}.")

        try:
            cmd = f"scontrol update nodename={nodes} state=idle"
            subprocess.check_output(shlex.split(cmd))
            event.set_results({"status": "resuming", "nodes": nodes})
        except subprocess.CalledProcessError as e:
            event.fail(message=f"Error resuming {nodes}: {e.output}")

    def _on_slurmd_available(self, event: SlurmdAvailableEvent) -> None:
        """Triggers when a slurmd unit joins the relation."""
        self._update_gres_conf(event)
        self._on_write_slurm_conf(event)

    def _on_slurmd_departed(self, event: SlurmdDepartedEvent) -> None:
        """Triggers when a slurmd unit departs the relation.

        Notes:
            Lack of map between departing unit and NodeName complicates removal of node from gres.conf.
            Instead, rewrite full gres.conf with data from remaining units.
        """
        if not self.unit.is_leader():
            return

        if not self._check_status():
            logger.debug("slurmd departing while unit is not ready. deferring event")
            event.defer()
            return

        # Reconcile the new_nodes.
        new_nodes = self.new_nodes
        logger.debug(f"New nodes from stored state: {new_nodes}")

        active_nodes = self._slurmd.get_active_nodes()
        logger.debug(f"Active nodes: {active_nodes}")

        for node in new_nodes:
            if node not in active_nodes:
                logger.debug(f"Removing node: {node}")
                new_nodes.remove(node)

        # Set the remaining new nodes
        self.new_nodes = new_nodes

        self._refresh_gres_conf(event)
        self._on_write_slurm_conf(event)

    def _update_gres_conf(self, event: SlurmdAvailableEvent) -> None:
        """Write new nodes to gres.conf configuration file for Generic Resource scheduling.

        Warnings:
            * This function does not perform an `scontrol reconfigure`. It is expected
               that the function `_on_write_slurm_conf()` is called immediately following to do this.
        """
        if not self.unit.is_leader():
            return

        if not self._check_status():
            event.defer()
            return

        node_name = event.node_name
        gres_info = event.gres_info
        if gres_info and node_name:
            gres_nodes = []
            for resource in gres_info:
                node = GRESNode(NodeName=node_name, **resource)
                gres_nodes.append(node)

            with self._slurmctld.gres.edit() as config:
                config.nodes[node_name] = gres_nodes

    def _refresh_gres_conf(self, event: SlurmdDepartedEvent) -> None:
        """Write out current gres.conf configuration file for Generic Resource scheduling.

        Warnings:
            * This function does not perform an `scontrol reconfigure`. It is expected
               that the function `_on_write_slurm_conf()` is called immediately following to do this.
        """
        if not self.unit.is_leader():
            return

        if not self._check_status():
            event.defer()
            return

        gres_all_nodes = self._slurmd.get_all_gres_info()
        gres_conf = GRESConfig(Nodes=gres_all_nodes)
        self._slurmctld.gres.dump(gres_conf)

    def _assemble_acct_gather_config(self) -> Optional[AcctGatherConfig]:
        """Assemble and return the AcctGatherConfig."""
        if (acct_gather_stored_params := self._stored.acct_gather_params) != {}:
            return AcctGatherConfig(
                ProfileInfluxDBDefault=list(acct_gather_stored_params["ProfileInfluxDBDefault"]),
                ProfileInfluxDBHost=acct_gather_stored_params["ProfileInfluxDBHost"],
                ProfileInfluxDBUser=acct_gather_stored_params["ProfileInfluxDBUser"],
                ProfileInfluxDBPass=acct_gather_stored_params["ProfileInfluxDBPass"],
                ProfileInfluxDBDatabase=acct_gather_stored_params["ProfileInfluxDBDatabase"],
                ProfileInfluxDBRTPolicy=acct_gather_stored_params["ProfileInfluxDBRTPolicy"],
                SysfsInterfaces=list(acct_gather_stored_params["SysfsInterfaces"]),
            )
        return None

    def _on_write_slurm_conf(
        self,
        event: Union[
            ConfigChangedEvent,
            InfluxDBAvailableEvent,
            InfluxDBUnavailableEvent,
            StartEvent,
            SlurmctldAvailableEvent,
            SlurmctldDepartedEvent,
            SlurmdbdAvailableEvent,
            SlurmdbdUnavailableEvent,
            SlurmdDepartedEvent,
            SlurmdAvailableEvent,
            PartitionUnavailableEvent,
            PartitionAvailableEvent,
        ],
    ) -> None:
        """Check that we have what we need before we proceed."""
        logger.debug("### Slurmctld - _on_write_slurm_conf()")

        # Only the leader should write the config, restart, and scontrol reconf.
        if not self.unit.is_leader():
            return

        # If not ready, defer.
        if not self._check_status():
            event.defer()
            return

        if slurm_config := self._assemble_slurm_conf():
            self._slurmctld.service.stop()

            logger.debug(f"Writing slurm.conf: {slurm_config}")
            self._slurmctld.config.dump(slurm_config)

            self._slurmctld.acct_gather.path.unlink(missing_ok=True)
            # Slurmdbd is required to use profiling, so check for it here.
            if self._stored.slurmdbd_host != "":
                if (acct_gather_config := self._assemble_acct_gather_config()) is not None:
                    logger.debug(f"Writing acct_gather.conf: {acct_gather_config}")
                    self._slurmctld.acct_gather.dump(acct_gather_config)

            # Write out any cgroup parameters to /etc/slurm/cgroup.conf.
            if not is_container():
                cgroup_config = CHARM_MAINTAINED_CGROUP_CONF_PARAMETERS
                if user_supplied_cgroup_params := self._get_user_supplied_cgroup_parameters():
                    cgroup_config.update(user_supplied_cgroup_params)
                self._slurmctld.cgroup.dump(CgroupConfig(**cgroup_config))

            self._slurmctld.service.start()

            try:
                self._slurmctld.scontrol("reconfigure")
            except SlurmOpsError as e:
                logger.error(e)
                return

            # Transitioning Nodes
            #
            # 1) Identify transitioning_nodes by comparing the new_nodes in StoredState with the
            #    new_nodes that come from slurmd relation data.
            #
            # 2) If there are transitioning_nodes, resume them, and update the new_nodes in
            #    StoredState.
            new_nodes_from_stored_state = self.new_nodes
            new_nodes_from_slurm_config = self._get_new_node_names_from_slurm_config(slurm_config)

            transitioning_nodes: list = [
                node
                for node in new_nodes_from_stored_state
                if node not in new_nodes_from_slurm_config
            ]

            if len(transitioning_nodes) > 0:
                self._resume_nodes(transitioning_nodes)
                self.new_nodes = new_nodes_from_slurm_config.copy()

            # slurmrestd needs the slurm.conf file, so send it every time it changes.
            if self._slurmrestd.is_joined is not False and self._stored.slurmdbd_host != "":
                self._slurmrestd.set_slurm_config_on_app_relation_data(str(slurm_config))
        else:
            logger.debug("## Should write slurm.conf, but we don't have it. Deferring.")
            event.defer()

    def _assemble_profiling_params(self) -> Dict[Any, Any]:
        """Assemble and return the profiling parameters."""
        job_profiling_slurm_conf_parameters = {}
        if (profiling_params := self._stored.job_profiling_slurm_conf) != {}:
            for k, v in profiling_params.items():
                if k == "AccountingStorageTRES":
                    v = list(v)
                if k == "JobAcctGatherFrequency":
                    v = dict(v)
                job_profiling_slurm_conf_parameters[k] = v
        return job_profiling_slurm_conf_parameters

    def _assemble_slurm_conf(self) -> SlurmConfig:
        """Return the slurm.conf parameters."""
        user_supplied_parameters = self._get_user_supplied_parameters()

        slurmd_parameters = self._slurmd.get_new_nodes_and_nodes_and_partitions()

        def _assemble_slurmctld_parameters() -> dict[str, Any]:
            # Preprocess merging slurmctld_parameters if they exist in the context
            slurmctld_parameters = {"enable_configless": True}

            if (
                user_supplied_slurmctld_parameters := user_supplied_parameters.get(
                    "SlurmctldParameters", ""
                )
                != ""
            ):
                for opt in user_supplied_slurmctld_parameters.split(","):
                    k, v = opt.split("=", maxsplit=1)
                    slurmctld_parameters.update({k: v})

            return slurmctld_parameters

        accounting_params = {}
        profiling_parameters = {}
        if (slurmdbd_host := self._stored.slurmdbd_host) != "":
            accounting_params = {
                "AccountingStorageHost": slurmdbd_host,
                "AccountingStorageType": "accounting_storage/slurmdbd",
                "AccountingStoragePass": "/var/run/munge/munge.socket.2",
                "AccountingStoragePort": "6819",
            }
            # Need slurmdbd configured to use profiling
            profiling_parameters = self._assemble_profiling_params()
            logger.debug(f"## profiling_params: {profiling_parameters}")

        slurm_conf = SlurmConfig(
            ClusterName=self.cluster_name,
            SlurmctldHost=self.get_controllers(),
            SlurmctldParameters=_assemble_slurmctld_parameters(),
            ProctrackType="proctrack/linuxproc" if is_container() else "proctrack/cgroup",
            TaskPlugin=["task/affinity"] if is_container() else ["task/cgroup", "task/affinity"],
            **profiling_parameters,
            **accounting_params,
            **CHARM_MAINTAINED_SLURM_CONF_PARAMETERS,
            **slurmd_parameters,
            **user_supplied_parameters,
        )

        logger.debug(f"slurm.conf: {slurm_conf.dict()}")
        return slurm_conf

    def _get_user_supplied_parameters(self) -> Dict[Any, Any]:
        """Gather, parse, and return the user supplied parameters."""
        user_supplied_parameters = {}
        if custom_config := self.config.get("slurm-conf-parameters"):
            user_supplied_parameters = {
                line.split("=")[0]: line.split("=", 1)[1]
                for line in str(custom_config).split("\n")
                if not line.startswith("#") and line.strip() != ""
            }
        return user_supplied_parameters

    def _get_user_supplied_cgroup_parameters(self) -> Dict[Any, Any]:
        """Gather, parse, and return the user supplied cgroup parameters."""
        user_supplied_cgroup_parameters = {}
        if custom_cgroup_config := self.config.get("cgroup-parameters"):
            user_supplied_cgroup_parameters = {
                line.split("=")[0]: line.split("=", 1)[1]
                for line in str(custom_cgroup_config).split("\n")
                if not line.startswith("#") and line.strip() != ""
            }
        return user_supplied_cgroup_parameters

    def _get_new_node_names_from_slurm_config(
        self, slurm_config: SlurmConfig
    ) -> List[Optional[str]]:
        """Given the slurm_config, return the nodes that are DownNodes with reason 'New node.'."""
        new_node_names = []
        if down_nodes_from_slurm_config := slurm_config.down_nodes:
            for down_nodes_entry in down_nodes_from_slurm_config:
                for down_node_name in down_nodes_entry["DownNodes"]:
                    if down_nodes_entry["Reason"] == "New node.":
                        new_node_names.append(down_node_name)
        return new_node_names

    def _check_status(self) -> bool:  # noqa C901
        """Check for all relations and set appropriate status.

        This charm needs these conditions to be satisfied in order to be ready:
        - Slurmctld component installed
        - Munge running
        - If using network storage, it must be mounted
        """
        if self.config.get("use-network-state"):
            state_save_location = Path(CHARM_MAINTAINED_SLURM_CONF_PARAMETERS["StateSaveLocation"])
            if not state_save_location.is_mount():
                self.unit.status = BlockedStatus(
                    f"waiting for {state_save_location} to be mounted"
                )
                return False

        if self.slurm_installed is not True:
            self.unit.status = BlockedStatus(
                "failed to install slurmctld. see logs for further details"
            )
            return False

        # TODO: https://github.com/charmed-hpc/hpc-libs/issues/18 -
        #   Re-enable munge key validation check when supported by `slurm_ops` charm library.
        # if not self._slurmctld.check_munged():
        #     self.unit.status = BlockedStatus("Error configuring munge key")
        #     return False

        try:
            status = self.get_controller_status(self.hostname)
        except Exception as e:
            logger.warning("failed to query controller active status. reason: %s", e)
            status = ""

        self.unit.status = ActiveStatus(status)
        return True

    def get_controllers(self) -> list[str]:
        """Get hostnames for all controllers."""
        # Read the current list of controllers from the slurm.conf file and compare with the controllers currently in the peer relation.
        # File ordering must be preserved as it dictates which slurmctld instance is the primary and which are backups.
        from_file = []
        if self._slurmctld.config.path.exists():
            with self._slurmctld.config.edit() as config:
                if (hosts := config.slurmctld_host):
                    from_file = hosts
        from_peer = self._slurmctld_peer.controllers

        logger.debug("controllers from slurm.conf: %s, from peer relation: %s", from_file, from_peer)

        # Controllers in the file but not the peer relation have departed.
        # Controllers in the peer relation but not the file are newly added.
        current_controllers = [c for c in from_file if c in from_peer] + \
                              [c for c in from_peer if c not in from_file]

        logger.debug("current controllers: %s", current_controllers)
        return current_controllers

    def get_munge_key(self) -> Optional[str]:
        """Get the stored munge key."""
        return str(self._slurmctld.munge.key.get())

    def get_jwt_rsa(self) -> Optional[str]:
        """Get the stored jwt_rsa key."""
        return str(self._slurmctld.jwt.get())

    def get_controller_status(self, hostname : str) -> str:
        """Return the status of the given controller instance, e.g. 'primary - UP'"""
        # Example snippet of ping output:
        #   "pings": [
        #     {
        #       "hostname": "juju-829e74-84",
        #       "pinged": "DOWN",
        #       "latency": 123,
        #       "mode": "primary"
        #     },
        #     {
        #       "hostname": "juju-829e74-85",
        #       "pinged": "UP",
        #       "latency": 456,
        #       "mode": "backup1"
        #     },
        #     {
        #       "hostname": "juju-829e74-86",
        #       "pinged": "UP",
        #       "latency": 789,
        #       "mode": "backup2"
        #     }
        #   ],
        ping_output = json.loads(self._slurmctld.scontrol("ping", "--json"))
        logger.debug("scontrol ping output: %s", ping_output)

        for ping in ping_output["pings"]:
            if ping["hostname"] == hostname:
                return f"{ping['mode']} - {ping['pinged']}"

        return ""

    def _resume_nodes(self, nodelist: List[str]) -> None:
        """Run scontrol to resume the specified node list."""
        self._slurmctld.scontrol("update", f"nodename={','.join(nodelist)}", "state=resume")

    @property
    def cluster_name(self) -> str:
        """Return the cluster name. Generates a new name if the cluster name has not been set."""
        # Retrieve the cluster name from either charm config or slurm.conf if already set.
        # Otherwise, generate a new random name.
        if charm_config_cluster_name := str(self.config.get("cluster-name", "")):
            logger.debug("retrieved cluster name: %s from charm config", charm_config_cluster_name)
            return charm_config_cluster_name

        if self._slurmctld.config.path.exists():
            with self._slurmctld.config.edit() as config:
                if config.cluster_name:
                    logger.debug("retrieved cluster name: %s from slurm.conf", config.cluster_name)
                    return config.cluster_name

        new_name = f"{CLUSTER_NAME_PREFIX}-{secrets.token_urlsafe(3)}"
        logger.info("existing cluster name not found. generated new cluster name: %s", new_name)
        return new_name

    @property
    def new_nodes(self) -> list:
        """Return new_nodes from StoredState.

        Note: Ignore the attr-defined for now until this is fixed upstream.
        """
        return list(self._stored.new_nodes)  # type: ignore[call-overload]

    @new_nodes.setter
    def new_nodes(self, new_nodes: List[Any]) -> None:
        """Set the new nodes."""
        self._stored.new_nodes = new_nodes

    @property
    def hostname(self) -> str:
        """Return the hostname."""
        return self._slurmctld.hostname

    @property
    def _ingress_address(self) -> str:
        """Return the ingress_address from the peer relation if it exists."""
        if (peer_binding := self.model.get_binding(PEER_RELATION)) is not None:
            ingress_address = f"{peer_binding.network.ingress_address}"
            logger.debug(f"Slurmctld ingress_address: {ingress_address}")
            return ingress_address
        raise IngressAddressUnavailableError("Ingress address unavailable")

    @property
    def slurm_installed(self) -> bool:
        """Return slurm_installed from stored state."""
        return True if self._stored.slurm_installed is True else False

    @slurm_installed.setter
    def slurm_installed(self, slurm_installed: bool) -> None:
        """Set slurm_installed in stored state."""
        self._stored.slurm_installed = slurm_installed


if __name__ == "__main__":
    main.main(SlurmctldCharm)
