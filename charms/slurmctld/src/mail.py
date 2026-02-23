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

from constants import MAILPROG_PATH, SLURM_MAIL_CONFIG_PATH
from hpc_libs.machine import apt

logger = logging.getLogger()


class MailOpsError(Exception):
    """Exception raised when mail service operations fail."""

    @property
    def message(self) -> str:
        """Return the error message passed as argument to the exception."""
        return self.args[0]


def configure(**kwargs) -> Path:
    """Configure slurm-mail and return the mailer executable path.

    Args:
        **kwargs: Configuration parameters. Supported options:
            server: hostname or IP address of the SMTP server.
            port: port number for the SMTP server.
            use_tls: enable/disable transport layer security (TLS) for the connection.
            user: username for SMTP authentication
            password: password for SMTP authentication
            from_name: name to appear in the signature of sent emails.

    Returns:
        Path to the executable to be assigned to `MailProg` in slurm.conf.

    Raises:
        FileNotFoundError: if the configuration file does not exist.
        OSError: if unable to set configuration file permissions or ownership.
        configparser.NoSectionError: if the slurm-send-mail section is missing from the config.
    """
    # Option name and formatter
    supported_options = {
        "server": ("smtpServer", str),
        "port": ("smtpPort", str),
        "use_tls": ("smtpUseTls", lambda x: "yes" if x else "no"),
        "user": ("smtpUserName", str),
        "password": ("smtpPassword", str),
        "from_name": ("emailFromName", str),
    }

    config_path = Path(SLURM_MAIL_CONFIG_PATH)
    if not config_path.exists():
        raise FileNotFoundError(f"configuration file not found: {config_path}")

    config = configparser.RawConfigParser()
    # Preserve camelCase keys, such as smtpServer
    config.optionxform = str  # pyright: ignore[reportAttributeAccessIssue]
    config.read(config_path)

    # Determine configuration changes
    section = "slurm-send-mail"
    config_changed = False
    for option, value in kwargs.items():
        if option not in supported_options:
            logger.warning("unknown configuration parameter: %s", option)
            continue

        if value is None:
            continue

        config_key, formatter = supported_options[option]
        new_value = formatter(value)
        # Fall back to the value `None` if the key does not exist
        if config.get(section, config_key, fallback=None) != new_value:
            config.set(section, config_key, new_value)
            config_changed = True

    if not config_changed:
        logger.info("no changes required to slurm-mail configuration")
        return Path(MAILPROG_PATH)

    # Write to temp file and automatically replace original
    swap = config_path.with_stem("." + config_path.stem).with_suffix(config_path.suffix + ".swp")
    with swap.open("w") as f:
        config.write(f)

    # Ensure slurm group has read access. Necessary for slurmctld to run MailProg=slurm-spool-mail
    try:
        swap.chmod(0o640)
        shutil.chown(swap, "root", "slurm")
    except OSError as e:
        e.add_note(f"failed to set permissions or ownership for {swap}")
        raise

    swap.replace(config_path)
    return Path(MAILPROG_PATH)


def install() -> None:
    """Ensure slurm-mail package is installed.

    Raises:
        MailOpsError: if an error occurs during package installation.
    """
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
