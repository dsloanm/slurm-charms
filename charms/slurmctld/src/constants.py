# Copyright 2025-2026 Vantage Compute Corporation
# Copyright 2024 Omnivector, LLC.
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

"""Constants used within the `slurmctld` charmed operator."""

from hpc_libs.is_container import is_container

OCI_RUNTIME_INTEGRATION_NAME = "oci-runtime"
PEER_INTEGRATION_NAME = "slurmctld-peer"
SACKD_INTEGRATION_NAME = "login-node"
SLURMD_INTEGRATION_NAME = "slurmd"
SLURMDBD_INTEGRATION_NAME = "slurmdbd"
SLURMRESTD_INTEGRATION_NAME = "slurmrestd"

HA_MOUNT_INTEGRATION_NAME = "mount"
HA_MOUNT_LOCATION = "/srv/slurmctld-statefs"

MAIL_INTEGRATION_NAME = "smtp"
MAILPROG_PATH = "/usr/bin/slurm-spool-mail"
SLURM_MAIL_CONFIG_PATH = "/etc/slurm-mail/slurm-mail.conf"

SLURMCTLD_PORT = 6817
PROMETHEUS_EXPORTER_PORT = 9092

CLUSTER_NAME_PREFIX = "charmed-hpc"

DEFAULT_CGROUP_CONFIG = {
    "constraincores": True,
    "constraindevices": True,
    "constrainramspace": True,
    "constrainswapspace": True,
    "signalchildrenprocesses": True,
}

ACCOUNTING_CONFIG_FILE = "slurm.conf.accounting"
PROFILING_CONFIG_FILE = "slurm.conf.profiling"
OVERRIDES_CONFIG_FILE = "slurm.conf.overrides"
DEFAULT_SLURM_CONFIG = {
    "authaltparameters": {"jwt_key": "/etc/slurm/jwt_hs256.key"},
    "authalttypes": ["auth/jwt"],
    "authtype": "auth/slurm",
    "credtype": "cred/slurm",
    "grestypes": ["gpu"],
    "maxnodecount": 65533,
    "metricstype": "metrics/openmetrics",
    "plugindir": ["/usr/lib/x86_64-linux-gnu/slurm-wlm"],
    "plugstackconfig": "/etc/slurm/plugstack.conf.d/plugstack.conf",
    "proctracktype": "proctrack/linuxproc" if is_container() else "proctrack/cgroup",
    "rebootprogram": "/usr/sbin/reboot --reboot",
    "selecttype": "select/cons_tres",
    "selecttypeparameters": {"cr_cpu_memory": True},
    "slurmctldparameters": {"enable_configless": True},
    "slurmctldport": SLURMCTLD_PORT,
    "slurmdport": 6818,
    "statesavelocation": "/var/lib/slurm/checkpoint",
    "slurmdspooldir": "/var/lib/slurm/slurmd",
    "slurmctldlogfile": "/var/log/slurm/slurmctld.log",
    "slurmdlogfile": "/var/log/slurm/slurmd.log",
    "slurmdpidfile": "/var/run/slurmd.pid",
    "slurmctldpidfile": "/var/run/slurmctld.pid",
    "slurmuser": "slurm",
    "slurmduser": "root",
    "taskplugin": ["task/affinity"] if is_container() else ["task/cgroup", "task/affinity"],
    "include": [ACCOUNTING_CONFIG_FILE, PROFILING_CONFIG_FILE, OVERRIDES_CONFIG_FILE],
}
DEFAULT_SLURM_MAIL_CONFIG = {
    "common": {"spoolDir": "/var/spool/slurm-mail"},
    "slurm-spool-mail": {
        "logFile": "/var/log/slurm-mail/slurm-spool-mail.log",
        "verbose": "false",
    },
    "slurm-send-mail": {
        "logFile": "/var/log/slurm-mail/slurm-send-mail.log",
        "verbose": "false",
        "arrayMaxNotifications": "0",
        "emailFromUserAddress": "root",
        "emailFromName": "Slurm Admin",
        "emailRegEx": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
        "emailSubject": "Job $CLUSTER.$JOB_ID: $STATE",
        "gecosNameField": "0",
        "validateEmail": "false",
        "datetimeFormat": "%d/%m/%Y %H:%M:%S",
        "sacctExe": "/usr/bin/sacct",
        "scontrolExe": "/usr/bin/scontrol",
        "smtpServer": "localhost",
        "smtpPort": "25",
        "smtpUseTls": "no",
        "smtpUseSsl": "no",
        "smtpUserName": "",
        "smtpPassword": "",
        "retryOnFailure": "yes",
        "retryDelay": "0",
        "tailExe": "/usr/bin/tail",
        "includeOutputLines": "0",
    },
}
DEFAULT_PROFILING_CONFIG = {
    "acctgatherprofiletype": "acct_gather_profile/influxdb",
    "acctgatherinterconnecttype": "acct_gather_interconnect/sysfs",
    "accountingstoragetres": ["ic/sysfs"],
    "acctgathernodefreq": 30,
    "jobacctgatherfrequency": {"task": 5, "network": 5},
    "jobacctgathertype": ("jobacct_gather/linux" if is_container() else "jobacct_gather/cgroup"),
}
DEFAULT_GRES_CONFIG = {"autodetect": "nvidia"}
