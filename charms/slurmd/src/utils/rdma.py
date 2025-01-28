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

    # Re-enable UCX/UCT.
    # The OpenMPI package from archive includes a Debian patch that disables UCX to silence
    # warnings when running on systems without RDMA hardware. This leads to the less performant
    # "ob1" method being selected on Infiniband-enabled systems. By default, OpenMPI selects UCX
    # when Infiniband devices are available. This default functionality is restored here.
    #
    # See:
    # https://sources.debian.org/src/openmpi/4.1.4-3/debian/patches/no-warning-unused.patch/#L24
    # https://github.com/open-mpi/ompi/issues/8367
    # https://github.com/open-mpi/ompi/blob/v4.1.x/README#L763
    path = "/etc/openmpi/openmpi-mca-params.conf"
    _logger.info("enabling OpenMPI UCX transport in %s", path)

    try:
        with open(path, "r") as f:
            lines = f.readlines()
    except FileNotFoundError:
        _logger.warn("%s not found. skipping OpenMPI UCX enablement", path)
        return

    # Current patch adds lines:
    #   btl = ^uct,openib,ofi
    #   pml = ^ucx
    #   osc = ^ucx,pt2pt
    # Remove "uct" and "ucx". Values must still begin with "^", e.g. `btl = ^openib,ofi`, to leave
    # openib and ofi disabled.
    new_lines = []
    for line in lines:
        if line.startswith(("btl =", "pml =", "osc =")):
            parts = line.split()
            values = parts[2].strip().lstrip("^").split(",")
            values = [v for v in values if v not in ("uct", "ucx")]
            # Remove line entirely if all values removed, e.g. "pml = ^ucx"
            if values:
                new_lines.append(f"{parts[0]} = ^{','.join(values)}\n")
        else:
            new_lines.append(line)

    with open(path, "w") as f:
        f.writelines(new_lines)
