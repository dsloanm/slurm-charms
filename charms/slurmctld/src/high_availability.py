# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Slurmctld high availability (HA) features."""

import logging
import shutil
import subprocess
import time
from pathlib import Path

from constants import CHARM_MAINTAINED_SLURM_CONF_PARAMETERS

import charms.operator_libs_linux.v0.apt as apt
import charms.operator_libs_linux.v2.snap as snap
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
    try:
        # TODO: hold version?
        snap.add("microceph")
    except snap.SnapError as e:
        raise HAOpsError(f"failed to install `microceph` snap. reason: {e}")

    pkgs = ["autofs", "ceph-common"]
    try:
        apt.add_package(pkgs)
    except (apt.PackageNotFoundError, apt.PackageError) as e:
        raise HAOpsError(f"failed to install {pkgs} packages. reason: {e}")

    logger.debug("setting up Ceph keyring and configuration file")

    links = [
        ("/etc/ceph/ceph.keyring", "/var/snap/microceph/current/conf/ceph.keyring"),
        ("/etc/ceph/ceph.conf", "/var/snap/microceph/current/conf/ceph.conf")
    ]
    for link, target in links:
        link_path = Path(link)
        target_path = Path(target)

        if link_path.is_symlink():
            logger.info("%s is already a symlink. Skipping creation.", link_path)
            continue

        link_path.symlink_to(target_path)


def bootstrap() -> None:
    """Bootstrap a Ceph cluster and CephFS file system for holding StateSaveLocation contents."""
    # TODO: reconsider disk size and pool size/min_size constants.
    logger.debug("bootstrapping new Ceph cluster")
    ceph_setup = [
        "microceph cluster bootstrap",
        "microceph disk add loop,2G,1",
        "ceph osd pool create cephfs_meta",
        "ceph osd pool create cephfs_data",
        "ceph fs new checkpointFs cephfs_meta cephfs_data",
        "ceph osd pool set cephfs_data size 3",
        "ceph osd pool set cephfs_meta size 3",
        "ceph osd pool set cephfs_data min_size 1",
        "ceph osd pool set cephfs_meta min_size 1"
    ]

    for cmd in ceph_setup:
        try:
            subprocess.check_output(cmd.split())
        except subprocess.CalledProcessError as e:
            raise HAOpsError(f"failed to bootstrap microceph. command `{cmd}` failed with reason: {e}")


def add_disk(size="2G", count=1) -> None:
    """Add a loop file disk of the given size and count."""
    try:
        subprocess.check_output(["microceph", "disk", "add", f"loop,{size},{count}"])
    except subprocess.CalledProcessError as e:
        raise HAOpsError(f"failed to add loop file disk with size={size} and count={count} to Ceph cluster. reason: {e}")


def configure_mount(mountpoint="/mnt/ceph-ha") -> None:
    mountpoint = Path(mountpoint)
    logger.debug("creating mountpoint %s", mountpoint)
    mountpoint.mkdir(mode=0o755, parents=True, exist_ok=True)

    logger.debug("configuring autofs for %s", mountpoint)
    Path("/etc/auto.master.d/checkpoint.autofs").write_text("/- /etc/auto.checkpoint\n")
    Path("/etc/auto.checkpoint").write_text(f"{mountpoint} -fstype=ceph,name=admin,fs=checkpointFs ::/\n") #

    try:
        systemd.service_reload("autofs", restart_on_failure=True)
    except systemd.SystemdError as e:
        raise HAOpsError(f"failed to reload autofs service. reason: {e}")


def remake_checkpoint(ceph_checkpoint="/mnt/ceph-ha/checkpoint", user="slurm", group="slurm") -> None:
    """Create a new checkpoint directory on CephFS storage owned by the given user and group."""
    checkpoint = Path(ceph_checkpoint)
    logger.debug("creating checkpoint directory %s", checkpoint)
    checkpoint.mkdir(mode=0o755, parents=True, exist_ok=True)
    logger.debug("reowning checkpoint directory %s to %s:%s", checkpoint, user, group)
    shutil.chown(checkpoint, user, group)


def migrate(mountpoint="/mnt/ceph-ha") -> None:
    """Move an existing local StateSaveLocation to CephFS"""
    local = Path(CHARM_MAINTAINED_SLURM_CONF_PARAMETERS["StateSaveLocation"])
    target = Path(mountpoint)

    # Ensure CephFS is mounted before trying move
    if not target.is_mount():
        logger.debug("forcing mount of %s", mountpoint)
        # HACK FIXME: find better way
        subprocess.run(["ls", "-l", mountpoint])
        if not target.is_mount():
            raise HAOpsError(f"failed to mount {target}. check config")

    logger.debug("moving %s to %s", local, target)
    shutil.move(local, target)

    # shutil does not preserve ownership.
    logger.debug("reowning %s to slurm user", target)
    shutil.chown(target / local.name, "slurm", "slurm")
    for item in (target / local.name).rglob("*"):
        shutil.chown(item, "slurm", "slurm")


def symlink(ceph_checkpoint="/mnt/ceph-ha/checkpoint") -> None:
    """Replace existing StateSaveLocation with a symlink to the CephFS mount.

    WARNING: Removes any local data in StateSaveLocation!
    """
    local_checkpoint = Path(CHARM_MAINTAINED_SLURM_CONF_PARAMETERS["StateSaveLocation"])
    if local_checkpoint.is_symlink():
        logger.info("%s is already a symlink. skipping creation", local_checkpoint)
        return

    ceph_checkpoint = Path(ceph_checkpoint)
    logger.debug("symlinking %s to %s", local_checkpoint, ceph_checkpoint)
    if local_checkpoint.exists():
        # TODO: Dangerous. Reassess use of rmtree.
        logger.info("existing %s directory found. removing", local_checkpoint)
        shutil.rmtree(local_checkpoint)

    local_checkpoint.parent.mkdir(parents=True, exist_ok=True)
    local_checkpoint.symlink_to(ceph_checkpoint)


def add(hostname) -> str:
    """Return a MicroCeph token for the given hostname for joining the Ceph cluster."""
    try:
        token = subprocess.check_output(["microceph", "cluster", "add", hostname])
    except subprocess.CalledProcessError as e:
        raise HAOpsError(f"failed to generate a cluster joining token for {hostname}. reason: {e}")

    return token.decode("utf-8").strip().rstrip("\n")


def join(token) -> None:
    """Join the Ceph cluster using the given MicroCeph token."""
    # TODO: stop rejoin attempt.
    try:
         subprocess.check_output(["microceph", "cluster", "join", token])
    except subprocess.CalledProcessError as e:
        raise HAOpsError(f"failed to join Ceph cluster. reason: {e}")

