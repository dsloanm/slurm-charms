# Copyright 2024 Canonical Ltd.
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

"""Manage GPU driver installation on compute node."""

import logging
import subprocess
from importlib import import_module

import charms.operator_libs_linux.v0.apt as apt

_logger = logging.getLogger(__name__)


class GPUInstallError(Exception):
    """Exception raised when a GPU driver installation operation failed."""

class GPUDriverDetector:
    """Detects GPU driver and kernel packages appropriate for the current hardware."""

    def system_packages(self) -> list:
        """Return a list of GPU drivers and kernel module packages for this node."""
        install_packages = [
            "python3-pynvml",
            "nvidia-headless-no-dkms-535-server",
            "linux-modules-nvidia-535-server-aws",
        ]

        # Brittle check using lspci for any Nvidia GPU.
        out = subprocess.check_output(["lspci"], text=True)
        for line in out.splitlines():
            line = line.lower()
            if "nvidia" in line:
                if "3d controller" in line or "vga compatible controller" in line:
                    return install_packages

        # Failed to find a GPU.
        return []


def autoinstall() -> None:
    """Autodetect available GPUs and install drivers.

    Raises:
        GPUInstallError: Raised if error is encountered during package install.
    """
    _logger.info("detecting GPUs")
    detector = GPUDriverDetector()
    install_packages = detector.system_packages()

    if len(install_packages) == 0:
        _logger.info("no GPUs detected")
        return

    _logger.info(f"installing GPU driver packages: {install_packages}")
    try:
        apt.add_package(install_packages)
    except (apt.PackageNotFoundError, apt.PackageError) as e:
        raise GPUInstallError(f"failed to install packages {install_packages}. reason: {e}")

    # TODO: This doesn't work. Fails to install kernel modules (e.g. linux-modules-nvidia-535-server-aws) - bugged?
    # TODO: handle install failures
    # r = subprocess.check_output(["ubuntu-drivers", "install", "--gpgpu", "--recommended"], stderr=subprocess.STDOUT, text=True)

    # TODO: just remove this. We don't want to depend on specific wordings of user-facing messages.
    # if r == 'No drivers found for installation.\n':
    #    _logger.info("no GPUs detected")

def get_gpus() -> dict:
    """Return the GPU devices on this node.

    TODO: Details of return type - dictionary with model name as its key.
    """
    gpu_info = {}

    # Return immediately if pynvml not installed...
    try:
        import pynvml
    except ModuleNotFoundError:
        return gpu_info

    # ...or Nvidia drivers not loaded.
    try:
        pynvml.nvmlInit()
    except pynvml.NVMLError_DriverNotLoaded:
        return gpu_info

    gpu_count = pynvml.nvmlDeviceGetCount()
    # Loop over all detected GPUs, gathering info by model.
    for i in range(gpu_count):
        handle = pynvml.nvmlDeviceGetHandleByIndex(i)

        # Make model name lowercase and replace whitespace with underscores to turn into GRES-compatible format,
        # e.g. "Tesla T4" -> "tesla_t4", which can be added as "Gres=gpu:tesla_t4:1"
        # Aims to follow convention set by Slurm autodetect:
        # https://slurm.schedmd.com/gres.html#AutoDetect
        model = pynvml.nvmlDeviceGetName(handle)
        model = "_".join(model.split()).lower()

        # Number for device path, e.g. if device is /dev/nvidia0, returns 0
        minor_number = pynvml.nvmlDeviceGetMinorNumber(handle)

        try:
            # Add minor number to set of existing numbers for this model.
            gpu_info[model].add(minor_number)
        except KeyError:
            # This is the first time we've seen this model. Create a new entry.
            gpu_info[model] = {minor_number}

    pynvml.nvmlShutdown()
    return gpu_info
