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
import tempfile
import textwrap
from importlib import import_module
from pathlib import Path

import charms.operator_libs_linux.v0.apt as apt

_logger = logging.getLogger(__name__)


class GPUInstallError(Exception):
    """Exception raised when a GPU driver installation operation failed."""


class GPUDriverDetect():
    """GPU and accelerator card detection and driver installation."""

    def __init__(self, *args, **kwargs):
        """Initialise detection attributes and interfaces."""
        # Install ubuntu-drivers tool if unavailable.
        # TODO: try->fail->install is a bad pattern. Find an alternative.
        try:
            self._detect = import_module("UbuntuDrivers.detect")
        except ModuleNotFoundError:
            try:
                apt.update()
                apt.add_package(["ubuntu-drivers-common", "python3-nvml")
            except (apt.PackageNotFoundError, apt.PackageError) as e:
                raise GPUInstallError(f"failed to install 'ubuntu-drivers-common'. reason: {e}")

            self._detect = import_module("UbuntuDrivers.detect")

        # ubuntu-drivers requires apt_pkg for package operations
        self._apt_pkg = import_module("apt_pkg")
        self._apt_pkg.init_config()
        self._apt_pkg.init_system()

    def _system_gpgpu_driver_packages(self) -> dict:
        """Detects the available GPGPU drivers for this node."""
        return self._detect.system_gpgpu_driver_packages()

    def _get_linux_modules_metapackage(self, driver) -> str:
        """Retrives the modules metapackage for the combination of current kernel and given driver.

        e.g. linux-modules-nvidia-535-server-aws for driver nvidia-driver-535-server
        """
        return self._detect.get_linux_modules_metapackage(self._apt_pkg.Cache(None), driver)

    def _system_packages(self) -> list:
        """Returns a list of GPU drivers and kernel module packages for this node."""
        # Detect only GPGPU drivers. Not general purpose graphics drivers.
        packages = self._system_gpgpu_driver_packages()

        # Gather list of driver and kernel modules to install.
        install_packages = []
        for driver_package in packages.keys():

            # Ignore drivers that are not recommended
            if packages[driver_package].get("recommended"):
                # Retrieve metapackage for this driver,
                # e.g. nvidia-headless-no-dkms-535-server for nvidia-driver-535-server
                driver_metapackage = packages[driver_package]["metapackage"]

                # Retrive modules metapackage for combination of current kernel and recommended driver,
                # e.g. linux-modules-nvidia-535-server-aws
                modules_metapackage = self._get_linux_modules_metapackage(driver_package)

                # Add to list of packages to install
                install_packages += [driver_metapackage, modules_metapackage]

        # TODO: do we want to check for nvidia here and add nvidia-fabricmanager-535 libnvidia-nscq-535 in case of nvlink? This is suggested as a manual step at https://documentation.ubuntu.com/server/how-to/graphics/install-nvidia-drivers/#optional-step. If so, how do we get the version number "-535" robustly?

        # TODO: what if drivers install but do not require a reboot? Should we "modprobe nvidia" manually? Just always reboot regardless?

        # Filter out any empty results as returning
        return [p for p in install_packages if p]

    def autoinstall(self) -> None:
        """Autodetect available GPUs and install drivers.

        Raises:
            GPUInstallError: Raised if error is encountered during package install.
        """
        _logger.info("detecting GPUs")
        install_packages = self._system_packages()

        if len(install_packages) == 0:
            _logger.info("no GPUs detected")
            return

        _logger.info(f"installing GPU driver packages: {install_packages}")
        try:
            apt.add_package(install_packages)
        except (apt.PackageNotFoundError, apt.PackageError) as e:
            raise GPUInstallError(f"failed to install packages {install_packages}. reason: {e}")

    # Subprocess attempt below
    # def __init__(self, *args, **kwargs):
    #     """Initialise detection attributes and interfaces."""
    #     # Install ubuntu-drivers tool if unavailable.
    #     # TODO: try->fail->install is a bad pattern. Find an alternative.
    #     pkgs = ["ubuntu-drivers-common", "python3-nvml"]
    #     try:
    #         apt.update()
    #         apt.add_package(pkgs)
    #     except (apt.PackageNotFoundError, apt.PackageError) as e:
    #         raise GPUInstallError(f"failed to install {pkgs} reason: {e}")
    #
    # def autoinstall(self) -> None:
    #     """Autodetect available GPUs and install drivers.
    #
    #     Raises:
    #         TODO:
    #     """
    #     _logger.info("detecting GPUs and installing any appropriate drivers")
    #
    #     # TODO: can we use the API given the licence?
    #     # TODO: handle install failures
    #     r = subprocess.check_output(["ubuntu-drivers", "install", "--gpgpu", "--recommended"], stderr=subprocess.STDOUT, text=True)
    #
    #     # TODO: do we want this?
    #     #if r == 'No drivers found for installation.\n':
    #     #    _logger.info("no GPUs detected")
    #
    #     # TODO: do we want to install nvidia-fabricmanager-535 libnvidia-nscq-535 in case of nvlink? This is suggested as a manual step at https://documentation.ubuntu.com/server/how-to/graphics/install-nvidia-drivers/#optional-step. If so, how do we get the version number "-535" robustly?
    #
    #     # TODO: what if drivers install but do not require a reboot? Should we "modprobe nvidia" manually? Just always reboot regardless?

    def get_gpus() -> dict:
        """Returns the GPU devices on this node.
           TODO: Details of return type - dictionary with model name as its key.
        """
        # TODO:
        #   * Add "NodeName=<nodename> Gres=gpu:<type>:<count>", e.g. "Gres=gpu:tesla:1" to node definition in slurm.conf
        #   * Add "Name=gpu Type=tesla File=/dev/nvidia0" to gres.conf
        #   * Add "gpu" to AccountingStorageTRES - check this.
        #   * For model names, "_".join(model.split()).lower, e.g. "Tesla T4" -> "Gres=gpu:tesla_t4:1"

        # Detect only Nvidia GPUs at the moment
        import pvnvml
        pynvml.nvmlInit

        gpu_count = pvnvml.nvmlDeviceGetCount()
        gpu_info = {}
        # Loop over all detected GPUs, gathering info
        for i in range(gpu_count):
            handle = pynvml.nvmlDevieGetHandleByIndex(i)

            # Make model name lowercase and replace whitespace with underscores to turn into GRES-compatible format,
            # e.g. "Tesla T4" -> "tesla_t4", which can be added as "Gres=gpu:tesla_t4:1"
            # Looks to follow convention set by Slurm autodetect:
            # https://slurm.schedmd.com/gres.html#AutoDetect
            model = pynvml.nvmlDeviceGetName(handle)
            model =  "_".join(model.split()).lower()

            # Number for device path, e.g. if device is /dev/nvidia0, returns 0
            minor_number = pynvml.nvmlDeviceGetMinorNumber(handle)

            try:
                # Add device file to list of existing device files for this model.
                gpu_info[model].add(minor_number)
            except KeyError:
                # This is the first time we've seen this model. Create a new entry.
                gpu_info[model] = set([minor_number])

        pynvml.nvmlShutdown()
        return gpu_info
