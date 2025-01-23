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

"""Manage RDMA packages on compute nodes."""

import logging

import charms.operator_libs_linux.v0.apt as apt

_logger = logging.getLogger(__name__)


class RDMAOpsError(Exception):
    """Exception raised when an RDMA package installation operation failed."""

    @property
    def message(self) -> str:
        """Return message passed as argument to exception."""
        return self.args[0]


def install() -> None:
    """Install RDMA packages.

    Raises:
        RDMAOpsError: Raised if error is encountered during package install.
    """
    install_packages = ["rdma-core", "infiniband-diags"]
    _logger.info("installing RDMA packages: %s", install_packages)
    try:
        apt.add_package(install_packages)
    except (apt.PackageNotFoundError, apt.PackageError) as e:
        raise RDMAOpsError(f"failed to install packages {install_packages}. reason: {e}")
