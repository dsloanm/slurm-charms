# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Slurmctld high availability (HA) features."""

import logging
import socket
import subprocess
from pathlib import Path

from constants import (
    CHARM_MAINTAINED_SLURM_CONF_PARAMETERS,
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
    """Start synchronization of StateSaveLocation across controller instances."""
    try:
        systemd.service_enable("checkpoint-sync.timer")
        systemd.service_start("checkpoint-sync.timer")
    except systemd.SystemdError as e:
        raise HAOpsError(f"failed to set up checkpoint synchronization. reason: {e}")


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

    for line in original:
        line = line.strip()
        if line.startswith("host") and line.endswith(";"):

            current_hosts = line.rstrip(";").split()[1:]
            if set(current_hosts) != set(hostnames):
                new.append("host " + " ".join(hostnames) + ";\n")
            else:
                # Skip the rest of the function if controllers haven't changed.
                return

        else:
            new.append(line + '\n')

    cfg.write_text(''.join(new))

    # Any controller change require a refresh of the csync2 database.
    try:
        subprocess.check_output(["/usr/sbin/csync2", "-Rv"])
    except subprocess.CalledProcessError as e:
        raise HAOpsError(f"failed to refresh controllers in csync2 database. reason: {e}")

    # If a new controller has been added, the StateSaveLocation directory must be marked as dirty to enable an initial sync to the new host.
    # TODO: this could be made more efficient by explicitly syncing to the new host from only the active slurmctld instance, rather than marking as dirty on all hosts and letting csync2 autoresolve.
    try:
        subprocess.check_output(["/usr/sbin/csync2", "-mrv", CHARM_MAINTAINED_SLURM_CONF_PARAMETERS["StateSaveLocation"]])
    except subprocess.CalledProcessError as e:
        raise HAOpsError(f"failed to mark StateSaveLocation as dirty in csync2 database. reason: {e}")


def is_sync_complete(hostnames) -> bool:
    """Return True if the StateSaveLocation data on this host is synchronized with all other hosts. False if there are pending changes on other hosts.

    Ignores hosts that are unreachable.
    """
    # `csync2 -T` returns:
    #   * 2 if all hosts are in sync
    #   * 0 if any host is out of sync
    #   * 1 if there's an error
    #
    p = subprocess.run(["/usr/sbin/csync2", "-T"])
    if p.returncode == 2:
        return True
    if p.returncode == 0:
        return False

    # A code of 1 may indicate we are synchronized, e.g. if a single host is down but we are synchronized against the rest.
    # Check each hostname one at a time.
    my_hostname = socket.gethostname()
    for peer in hostnames:
        p = subprocess.run(["/usr/sbin/csync2", "-T", my_hostname, peer])
        if p.returncode == 0:
            return False

    # No hosts were found to be out of sync.
    return True


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
    key_path = Path(key_file)

    if not key_path.exists() or key_path.read_text() != key:
        key_path.write_text(key)
    else:
        logger.info("key file %s is already the given key. not updating", key_file)


def set_cert(cert, key, cert_file="/etc/csync2_ssl_cert.pem", key_file="/etc/csync2_ssl_key.pem") -> None:
    cert_path = Path(cert_file)
    key_path = Path(key_file)

    for path, data in [(cert_path, cert), (key_path, key)]:
        if not path.exists() or path.read_text() != data:
            path.write_text(data)
        else:
            logger.info("file %s is already the given value. not updating", path)
