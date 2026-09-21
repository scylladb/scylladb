import glob
import logging
import os
import shutil
import subprocess
import time

from ccmlib.scylla_node import ScyllaNode

from .misc import safe_mkdtemp

logger = logging.getLogger(__name__)


def make_snapshot(  # noqa: PLR0913
    node: ScyllaNode,
    ks: str | None = None,
    cf: str = "",
    cf_param_name: str = "-cf",
    name: str | None = None,
    additional_options: list[str] | None = None,
) -> str:
    """Create snapshot for all keyspaces or for specified ks, ks.cf, with name

    Create snapshot for:
    - if ks is none, for all keyspaces
    - if ks is provided, create snapshot for all tables in keyspace
    - if ks and cf provided, create snapshot for ks.cf table only
    - if name is set, create snapshot with tag name, datetime otherwise

    and then copy created snapshots to temp directory

    :param node: Scylla Node instance where create snapshot
    :type node: ScyllaNode
    :param ks: keyspace name, defaults to None
    :type ks: str, optional
    :param cf: column factory name, defaults to None
    :type cf: str, optional
    :param name: tag name of snapshot, defaults to None
    :type name: str, optional
    :returns: path where all snapshots stored, temp directory
    :param additional_options: a list of additional options to be
    added to the nodetool snapshot command
    :type additional_options: list os strings
    :returns: path where all snapshots stored, temp directory
    :rtype: {str}
    """

    def _add_snapshot_dirs_to_list(_ks: str | None = None):
        tables = ["*/"]

        for table in tables:
            snapshot_dir_pattern = f"{node_dir}/data/"
            if _ks:
                snapshot_dir_pattern += f"{_ks}/"
                snapshot_dir_pattern += f"{table}"
                if name:
                    snapshot_dir_pattern += f"snapshots/{name}"
                else:
                    snapshot_dir_pattern += f"snapshots/*"
            else:
                snapshot_dir_pattern += f"/*/*/snapshots/*"
            _snapshot_dir = glob.glob(snapshot_dir_pattern)
            if _snapshot_dir:
                snapshot_dirs.extend(_snapshot_dir)
            else:
                snapshot_dirs.append("")

    logger.debug("Making snapshot....")
    snapshot_cmd = "snapshot "
    if ks:
        snapshot_cmd += f"{ks} "
        if cf:
            snapshot_cmd += f"{cf_param_name} {cf} "
        if name:
            snapshot_cmd += f"-t {name}"
    if additional_options:
        snapshot_cmd += " ".join(additional_options)

    logger.debug(f"Running snapshot cmd: {snapshot_cmd}")
    node.nodetool(snapshot_cmd)
    tmpdir = safe_mkdtemp()
    node_dir = node.get_path()

    # # Find the snapshot dir, it's different in various C* versions:
    snapshot_dirs = []
    if ks:
        for kspace in ks.split(","):
            _add_snapshot_dirs_to_list(_ks=kspace)
    else:
        _add_snapshot_dirs_to_list(_ks=ks)

    logger.debug(f"snapshot_dir is : {snapshot_dirs}")
    logger.debug(f"snapshot copy is : {tmpdir}")

    # # Copy files from the snapshot dir to existing temp dir
    for snapshot_dir in snapshot_dirs:
        save_dir = snapshot_dir.replace("/snapshots/", "/").replace(os.path.join(node_dir, "data/"), "")
        os.makedirs(os.path.join(tmpdir, save_dir), exist_ok=False)
        shutil.copytree(str(snapshot_dir), os.path.join(tmpdir, save_dir), dirs_exist_ok=True)
        logger.debug(f"Copied snapshot {snapshot_dir} to {os.path.join(tmpdir, save_dir)}")

    return tmpdir


def get_cf_snapshot_saved_dir(base_snapshot_dir: str, keyspace: str, table: str, name: str | None = None) -> str:
    """Get path to specified snapshot of ks.cf by name or first one

    return path to directory with sstables from snapshot store in
    base_snapshot_dir. base_snapshot_dir is a path to temp folder returned by
    make_snapshot method or any folder where all snapshots located
        - <base_snapshot_dir>/ks/cf-*/[name|any]/

    :param base_snapshot_dir: path to folder with snapshots
    :type base_snapshot_dir: str
    :param keyspace: keyspace name
    :type keyspace: str
    :param table: column family name
    :type table: str
    :param name: name of snapshot, defaults to None
    :type name: str, optional
    :returns: path to first matched snapshot dir for ks.cf by [name| of first one]
    :rtype: {str}
    """
    path_pattern = f"{base_snapshot_dir}/{keyspace}/{table}-*"
    if name:
        path_pattern += f"/{name}"
    else:
        path_pattern += f"/*/"
    dirs = glob.glob(path_pattern)
    logger.debug(f"snapshots for {keyspace}.{table} with name={name}: {dirs}")
    return dirs[0] if dirs else ""


def restore_snapshot_with_refresh(  # noqa: PLR0913
    snapshot_dir,
    node,
    keyspace,
    table,
    name=None,
    wait_for_mv=False,
    wait_for_mv_timeout=30,
):
    logger.debug("Restoring snapshot....")
    node_dir = node.get_path()
    restore_dir = glob.glob(f"{node_dir}/data/{keyspace}/{table}-*/upload/")[0]
    snapshot_dir = get_cf_snapshot_saved_dir(base_snapshot_dir=snapshot_dir, keyspace=keyspace, table=table, name=name)
    logger.debug(f"Copying from {snapshot_dir!s} to {restore_dir!s}")
    shutil.copytree(snapshot_dir, restore_dir, dirs_exist_ok=True)
    node.nodetool(f"refresh {keyspace} {table}")

    if wait_for_mv:
        staging_dir = glob.glob(f"{node_dir}/data/{keyspace}/{table}-*/staging")[0]
        if len(glob.glob(f"{staging_dir}/*")):
            logger.debug("Waiting for %s to become empty", staging_dir)
            started = time.time()
            while len(glob.glob(f"{staging_dir}/*")):
                if time.time() - started >= wait_for_mv_timeout:
                    raise TimeoutError(f"{staging_dir} is still not empty after {wait_for_mv_timeout} seconds")
                time.sleep(1)
            logger.debug("Waiting for %s to become empty", staging_dir)


def get_table_description(node, ks, cf):
    table_desc = node.run_cqlsh(f"describe table {ks}.{cf}", return_output=True)
    return table_desc[0]
