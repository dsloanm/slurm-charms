# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Slurmctld high availability (HA) features."""

import logging
import socket
import subprocess
from pathlib import Path

from constants import (
    CHECKPOINT_SYNC_SERVICE,
    CHECKPOINT_SYNC_TIMER,
    CSYNC2_CONF,
)

import charms.operator_libs_linux.v0.apt as apt
import charms.operator_libs_linux.v1.systemd as systemd

logger = logging.getLogger()


class HAOpsError(Exception):
    """Exception raised when a high availability operation failed."""

    @property
    def message(self) -> str:
        """Return message passed as argument to exception."""
        return self.args[0]


def install() -> None:
    """Install software and services necessary for a slurmctld high availability configuration."""
    pkgs = ["csync2"]
    try:
        apt.add_package(pkgs)
    except (apt.PackageNotFoundError, apt.PackageError) as e:
        raise HAOpsError(f"failed to install packages: {pkgs}. reason: {e}")

    # Include only self in initial list of hosts.
    conf = CSYNC2_CONF.replace("host;", f"host {socket.gethostname()};")
    Path("/etc/csync2.cfg").write_text(conf)

    # Service and timer disabled by default.
    Path("/etc/systemd/system/checkpoint-sync.service").write_text(CHECKPOINT_SYNC_SERVICE)
    Path("/etc/systemd/system/checkpoint-sync.timer").write_text(CHECKPOINT_SYNC_TIMER)
    systemd.daemon_reload()


def start_sync() -> None:
    """Start synchronization of StateSaveLocation across controller instances. Performs an explicit initial sync before starting the service timer."""
    try:
        subprocess.check_output(["/usr/sbin/csync2", "-xv"])
    except subprocess.CalledProcessError as e:
        raise HAOpsError(f"failed to perform initial StateSaveLocation sync. reason: {e}")

    try:
        systemd.service_enable("checkpoint-sync.timer")
        systemd.service_start("checkpoint-sync.timer")
    except systemd.SystemdError:
        logger.exception("failed to set up checkpoint synchronization")
        # TODO: raise exception


def stop_sync() -> None:
    """Stop synchronization of StateSaveLocation across slurmctld instances."""
    systemd.service_disable("checkpoint-sync.timer")
    systemd.service_stop("checkpoint-sync.timer")
    # TODO: try this and fall back to `systemctl kill --signal=9 checkpoint-sync.service` in case of timeout?
    systemd.service_stop("checkpoint-sync.service")


def set_controllers(hostnames, cfg_file="/etc/csync2.cfg") -> None:
    """Set the list of controllers being synchronized to the given hostnames."""
    cfg = Path(cfg_file)
    original = cfg.read_text().splitlines()
    new = []

    host_line = "host " + " ".join(hostnames) + ";\n"
    for line in original:
        line = line.strip()
        if line.startswith("host") and line.endswith(";"):
            new.append(host_line)
        else:
            new.append(line + '\n')

    cfg.write_text(''.join(new))

    # TODO: confirm this is needed
    # Also requires removing the entry from the csync2 database
    #try:
    #    subprocess.check_output(["/usr/sbin/csync2", "-Rv"])
    #except subprocess.CalledProcessError as e:
    #    raise HAOpsError(f"failed to remove controller from csync2 database. reason: {e}")


def generate_key(key_file="/etc/csync2.key") -> str:
    """Generate a key file used for authentication of csync2 groups.

    Returns:
        The generated key.

    Raises:
        HAOpsError: Raised if error is encountered during generation.
    """
    try:
        subprocess.check_output(["/usr/sbin/csync2", "-k", key_file])
    except subprocess.CalledProcessError as e:
        raise HAOpsError(f"failed to generate csync2 key. reason: {e}")

    return Path(key_file).read_text().rstrip()


def generate_cert(cert_file="/etc/csync2_ssl_cert.pem", key_file="/etc/csync2_ssl_key.pem",) -> tuple[str, str]:
    """Generate a 4096-bit RSA key file and a corresponding self-signed, 100 year x509 certificate used for authentication of csync2 peers.

    Returns:
        A tuple with the first element containing the certificate and the second element containing the generated key.

    Raises:
        HAOpsError: Raised if error is encountered during generation.
    """
    try:
        subprocess.check_output(["openssl", "req", "-x509", "-newkey", "rsa:4096",
                                "-keyout", key_file,
                                "-out", cert_file,
                                "-sha256", "-days", "36500", "-nodes", "-subj",
                                "/C=XX/ST=StateName/L=CityName/O=CompanyName/OU=CompanySectionName/CN=CommonNameOrHostname"])
    except subprocess.CalledProcessError as e:
        raise HAOpsError(f"failed to generate SSL key. reason: {e}")

    return (Path(cert_file).read_text().rstrip(), Path(key_file).read_text().rstrip())


def set_key(key, key_file="/etc/csync2.key") -> None:
    Path(key_file).write_text(key)


def set_cert(cert, key, cert_file="/etc/csync2_ssl_cert.pem", key_file="/etc/csync2_ssl_key.pem") -> None:
    Path(cert_file).write_text(cert)
    Path(key_file).write_text(key)
