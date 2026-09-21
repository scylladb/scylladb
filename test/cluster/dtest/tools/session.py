import logging
from time import sleep

from cassandra.cluster import NoHostAvailable, Session

logger = logging.getLogger(__name__)


def wait_reconnection(session, num_attempts=5, sleep_time=5, allowed_exceptions=None):
    """
    this method needs to be used to wait for the existing session to reconnect after cluster/node stop/start.

    with no_boot_speedups test cases was slow and the Session object had enough
    time to detect that nodes are down and reconnecting, but now after cluster/node stop/start we need to wait.
    """
    for i in range(num_attempts):
        try:
            results = session.execute("SELECT * FROM system.local")
            logger.debug("wait_reconnection: query results: %s", list(results))
            logger.debug("wait_reconnection: connection established")
            return
        except NoHostAvailable:
            pass
        except Exception as e:  # noqa: BLE001
            if allowed_exceptions and isinstance(e, allowed_exceptions):
                logger.debug("wait_reconnection: allowed exception raised: %s", e)
                return
        logger.debug("wait_reconnection: attempt %s of %s failed, sleep for %s secs and retry", i + 1, num_attempts, sleep_time)
        sleep(sleep_time)
    logger.warning("wait_reconnection: no host available")


def get_supported_features(session: Session) -> list[str]:
    """
    helper function to get supported_features from a running cluster,
    if you need from a specific node use `patient_exclusive_cql_connection` session
    """
    result = session.execute("SELECT supported_features FROM system.local WHERE key='local'").one()
    # NOTE: since row_factory can be different on different tests, we need to support multiple options
    if isinstance(result, dict):
        result = result["supported_features"]
    elif isinstance(result, tuple):
        result = result[0]
    else:
        raise NotImplementedError(f"unsupported row_factory={session.row_factory}")
    return result.split(",")


def get_enabled_features(session: Session) -> list[str]:
    """
    helper function to get supported_features from a running cluster,
    if you need from a specific node use `patient_exclusive_cql_connection` session
    """
    result = session.execute("SELECT value FROM system.scylla_local WHERE key='enabled_features'").one()
    # NOTE: since row_factory can be different on different tests, we need to support multiple options
    if isinstance(result, dict):
        result = result["value"]
    elif isinstance(result, tuple):
        result = result[0]
    else:
        raise NotImplementedError(f"unsupported row_factory={session.row_factory}")
    return result.split(",")
