# Copyright 2026 Canonical Ltd.
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

"""Slurmctld email notification features."""

import configparser
import logging
import shutil
from pathlib import Path
from typing import Literal, Optional

from constants import DEFAULT_SLURM_MAIL_CONFIG, MAILPROG_PATH, SLURM_MAIL_CONFIG_PATH
from hpc_libs.machine import apt

_logger = logging.getLogger(__name__)


class MailOpsError(Exception):
    """Exception raised when mail service operations fail."""

    @property
    def message(self) -> str:
        """Return the error message passed as argument to the exception."""
        return self.args[0]


def configure(
    server: Optional[str] = None,
    port: Optional[str] = None,
    use_tls: Optional[Literal["yes", "no"]] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    from_name: Optional[str] = None,
) -> Path:
    """Configure slurm-mail and return the mailer executable path.

    Args:
        server: hostname or IP address of the SMTP server.
        port: port number for the SMTP server.
        use_tls: "yes" to enable or "no" to disable transport layer security (TLS) for the connection.
        user: username for SMTP authentication.
        password: password for SMTP authentication.
        from_name: name to appear in the signature of sent emails.

    Returns:
        Path to the executable to be assigned to `MailProg` in slurm.conf.

    Raises:
        MailOpsError: if an error occurs during configuration.
    """
    config_options = {
        "smtpServer": server,
        "smtpPort": port,
        "smtpUseTls": use_tls,
        "smtpUserName": user,
        "smtpPassword": password,
        "emailFromName": from_name,
    }

    config_path = Path(SLURM_MAIL_CONFIG_PATH)
    if not config_path.exists():
        _initialize_config_file(config_path)

    config = configparser.RawConfigParser()
    # Preserve camelCase keys, such as smtpServer
    config.optionxform = str  # pyright: ignore[reportAttributeAccessIssue]
    config.read(config_path)

    # Ensure the required section exists, reinitialize if not
    section = "slurm-send-mail"
    if not config.has_section(section):
        _logger.warning(
            "configuration file damaged: missing required section '%s'. reinitializing", section
        )
        _initialize_config_file(config_path)
        config.read(config_path)

    # Determine configuration changes
    config_changed = False
    for config_key, new_value in config_options.items():
        if new_value is None:
            continue

        # Fall back to the value `None` if the key does not exist
        if config.get(section, config_key, fallback=None) != new_value:
            config.set(section, config_key, new_value)
            config_changed = True

    if not config_changed:
        _logger.info("no changes required to slurm-mail configuration")
        return Path(MAILPROG_PATH)

    _write_config_file(config_path, config)
    return Path(MAILPROG_PATH)


def install() -> None:
    """Ensure slurm-mail package is installed.

    Raises:
        MailOpsError: if an error occurs during package installation.
    """
    _initialize_config_file()

    try:
        apt.add_package("slurm-mail")
    except (apt.PackageNotFoundError, apt.PackageError) as e:
        raise MailOpsError(f"failed to install slurm-mail package. reason: {e}") from e


def uninstall() -> None:
    """Ensure slurm-mail package is uninstalled.

    Raises:
        MailOpsError: if an error occurs during package removal.
    """
    try:
        apt.remove_package("slurm-mail")
    except (apt.PackageNotFoundError, apt.PackageError) as e:
        raise MailOpsError(f"failed to uninstall slurm-mail package. reason: {e}") from e


def _initialize_config_file(
    config_path: Path = Path(SLURM_MAIL_CONFIG_PATH),
    default_values: dict = DEFAULT_SLURM_MAIL_CONFIG,
) -> None:
    """Initialize the slurm-mail configuration file with default values if it does not exist.

    Args:
        config_path: configuration file path. Defaults to SLURM_MAIL_CONFIG_PATH.
        default_values: default configuration values. Defaults to DEFAULT_SLURM_MAIL_CONFIG.

    Raises:
        MailOpsError: if an error occurs during configuration file initialization.
    """
    if config_path.exists():
        _logger.warning(
            "configuration file already exists: %s. skipping initialization", config_path
        )
        return

    _logger.info("configuration file not found: %s. creating new configuration file", config_path)

    default_config = configparser.RawConfigParser()
    # Preserve camelCase keys, such as smtpServer
    default_config.optionxform = str  # pyright: ignore[reportAttributeAccessIssue]
    default_config.read_dict(default_values)
    _write_config_file(config_path, default_config)


def _write_config_file(config_path: Path, config: configparser.RawConfigParser) -> None:
    """Write configuration to file with appropriate permissions and ownership.

    Args:
        config_path: configuration file path.
        config: ConfigParser object to write to the file.

    Raises:
        MailOpsError: if an error occurs during file writing or setting of permissions.
    """
    try:
        config_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise MailOpsError(f"failed to create parent directories for {config_path}") from e

    # Write to temp file and atomically replace original
    swap = config_path.with_stem("." + config_path.stem).with_suffix(config_path.suffix + ".swp")

    try:
        with swap.open("w") as f:
            config.write(f)

        # Ensure slurm group has read access. Necessary for slurmctld to run MailProg=slurm-spool-mail
        swap.chmod(0o640)
        shutil.chown(swap, "root", "slurm")
        swap.replace(config_path)
    except OSError as e:
        raise MailOpsError(
            f"failed to write, set permissions, ownership, or atomically replace config file {config_path}"
        ) from e
