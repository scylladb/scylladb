import logging
import multiprocessing as mp
import traceback
import uuid
from collections import defaultdict
from decimal import Decimal
from os import getpid
from random import choice, randint, random, randrange, seed
from time import sleep, time

import psutil
import pytest
from cassandra import (
    ConsistencyLevel,
    OperationTimedOut,
    ReadFailure,
    Unavailable,
    WriteFailure,
)

from dtest_class import Tester, create_ks
from tools.cluster_topology import generate_cluster_topology_based_rf

logger = logging.getLogger(__name__)
# NOTE: This code does not work with Cassandra as its LWT queries don't return previous values
#
# TODO: reverse bic/ban to numbers for easier debugging

# Banking Test
KEYSPACE = "bnk"
BICS = 100  # How many banks
BANS = 1000
TOTAL_ACCOUNTS = BICS * BANS
TOTAL_TRANSFERS = 1000  # Total transfers to test
RF = 3
NODES = min(4, psutil.cpu_count(logical=False))  # More nodes make test much slower - TODO
MAX_WORKERS = 200  # Workers block so make more than cores/threads
BALANCE_INIT_MIN = 1000
BALANCE_INIT_MAX = 10000
LOCK_RETRY_SLEEP_INITIAL = 0.001  # Start sleeping for 10ms  (note it should grow over error duration)
LOCK_RETRY_SLEEP_FACTOR = 0.003  # sleep+factor*random, should be bigger than nemesis duration
LOCK_RETRY_SLEEP_MAX = 3  # Maximum sleep for lock retry
MAX_LOCK_RETRIES = 20
TRANSFER_MIN = 800
TRANFER_MAX = 1200
NEMESIS_ERROR_DURATION = 0.001  # How long an error injection on a given node lasts
TRANSFER_CLIENT_TTL = 5  # Seconds duration of client set on transfer
INSERT_BATCH_LEN = min(100, BANS)  # How many accounts to create per batch (accounts divisible by x)
assert BANS % INSERT_BATCH_LEN == 0 and INSERT_BATCH_LEN <= 100
RECOVERY_LAST_RETRY_LIMIT = 10  # After workers done, retry recovery at most X times

ERROR_INJECTIONS = [
    # TODO: test with timeouts
    # "paxos_prepare_timeout",
    "paxos_error_before_save_promise",
    "paxos_error_after_save_promise",
    # "paxos_accept_proposal_timeout",
    "paxos_error_before_save_proposal",
    "paxos_error_after_save_proposal",
    "paxos_error_before_learn",
    # "paxos_state_learn_timeout",
    "paxos_timeout_after_save_decision",
]

# NOTE: could use iso3166 but it would add a dependency, also 8 fit in 3 bits
COUNTRY_CODE = ["AR", "BR", "FR", "GB", "IL", "PL", "RU", "US"]

CREATE_KS = f"""CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
WITH REPLICATION = {{ 'class': 'NetworkTopologyStrategy', 'replication_factor' : {RF} }}
AND DURABLE_WRITES=true"""

USE_KS = f"""USE {KEYSPACE}"""

CREATE_SETTINGS_TAB = f"""
CREATE TABLE {KEYSPACE}.settings (
    key TEXT, -- arbitrary setting name
    value TEXT, -- arbitrary setting value
    PRIMARY KEY((key))
)"""

CREATE_ACCOUNTS_TAB = f"""
CREATE TABLE {KEYSPACE}.accounts (
    bic TEXT, -- bank identifier code
    ban TEXT, -- bank account number within the bank
    balance DECIMAL, -- account balance
    pending_transfer UUID, -- will be used later
    pending_amount DECIMAL, -- will be used later
    PRIMARY KEY((bic, ban))
)"""

CREATE_TRANSFERS_TAB = f"""
CREATE TABLE {KEYSPACE}.transfers (
    transfer_id UUID, -- transfers UUID
    src_bic TEXT, -- source bank identification code
    src_ban TEXT, -- source bank account number
    dst_bic TEXT, -- destination bank identification code
    dst_ban TEXT, -- destination bank account number
    amount DECIMAL, -- transfer amount
    state TEXT, -- 'new', 'locked', 'completed'
    client_id UUID, -- the client performing the transfer
    PRIMARY KEY (transfer_id)
)"""

CREATE_CHECK_TAB = f"""
CREATE TABLE {KEYSPACE}.check (
    name TEXT,
    amount DECIMAL,
    PRIMARY KEY(name)
)"""

INSERT_SETTING = f"""
INSERT INTO {KEYSPACE}.settings (key, value) VALUES (?, ?)
"""

FETCH_SETTING = f"""
SELECT value FROM {KEYSPACE}.settings WHERE key = ?
"""

INSERT_ACCOUNT = f"""
INSERT INTO {KEYSPACE}.accounts (bic, ban, balance, pending_amount) VALUES (?, ?, ?, 0)
"""  # NOTE: non-LWT, we are not load testing, check if inserted row count matches

INSERT_TRANSFER = f"""
INSERT INTO {KEYSPACE}.transfers
  (transfer_id, src_bic, src_ban, dst_bic, dst_ban, amount, state)
  VALUES (?, ?, ?, ?, ?, ?, 'new')
  IF NOT EXISTS
"""  # note new transfer, actually

SET_TRANSFER_STATE = f"""
UPDATE {KEYSPACE}.transfers
  SET state = ?
  WHERE transfer_id = ?
  IF amount != NULL AND client_id = ?
"""

SET_TRANSFER_CLIENT = f"""
UPDATE {KEYSPACE}.transfers USING TTL {TRANSFER_CLIENT_TTL}
  SET client_id = ?
  WHERE transfer_id = ?
  IF amount != NULL AND client_id = NULL AND state != NULL
"""

SET_TRANSFER_CLIENT_REFRESH = f"""
UPDATE {KEYSPACE}.transfers USING TTL {TRANSFER_CLIENT_TTL}
  SET client_id = ?
  WHERE transfer_id = ?
  IF amount != NULL AND client_id = ?
"""

# Always check the row exists to not accidentally add a transfer
CLEAR_TRANSFER_CLIENT = f"""
UPDATE {KEYSPACE}.transfers
  SET client_id = NULL
  WHERE transfer_id = ?
  IF amount != NULL AND client_id = ?
"""

DELETE_TRANSFER = f"""
DELETE FROM {KEYSPACE}.transfers
  WHERE transfer_id = ?
  IF client_id = ?
"""

DELETE_TRANSFER_ORPHANED = f"""
DELETE FROM {KEYSPACE}.transfers
  WHERE transfer_id = ?
  IF client_id = NULL
"""

FETCH_TRANSFER = f"""
SELECT src_bic, src_ban, dst_bic, dst_ban, amount, state
  FROM {KEYSPACE}.transfers
  WHERE transfer_id = ?
"""

FETCH_TRANSFER_CLIENT = f"""
SELECT client_id
  FROM {KEYSPACE}.transfers
  WHERE transfer_id = ?
"""

# Cassandra/Scylla don't handle IF client_id = NUll queries
# correctly. But NULLs are implicitly converted to mintimeuuids
# during comparison. Use one bug to workaround another.
# WHERE client_id < minTimeuuid('1979-08-12 21:35+0000')
FETCH_DEAD_TRANSFERS = f"""
SELECT transfer_id
  FROM {KEYSPACE}.transfers
  ALLOW FILTERING
"""

# Condition balance column:
# 1) To avoid accidentally inserting a new account here
# 2) To get it back (Scylla only)
LOCK_ACCOUNT = f"""
UPDATE {KEYSPACE}.accounts
  SET pending_transfer = ?, pending_amount = ?
  WHERE bic = ? AND ban = ?
  IF balance != NULL AND pending_amount != NULL AND pending_transfer = NULL
"""

# Condition balance column simply to get it back
UNLOCK_ACCOUNT = f"""
UPDATE {KEYSPACE}.accounts
  SET pending_transfer = NULL, pending_amount = 0
  WHERE bic = ? AND ban = ?
  IF balance != NULL AND pending_transfer = ?
"""

FETCH_BALANCE = f"""
SELECT balance, pending_amount
  FROM {KEYSPACE}.accounts
  WHERE bic = ? AND ban = ?
"""

# Update balance and clear pending transfer (unlock)
UPDATE_BALANCE = f"""
UPDATE {KEYSPACE}.accounts
  SET pending_amount = 0, balance = ?
  WHERE bic = ? AND ban = ?
  IF balance != NULL AND pending_transfer = ?
"""

CHECK_BALANCE = f"""
SELECT SUM(balance) FROM {KEYSPACE}.accounts
"""

PERSIST_TOTAL = f"""
UPDATE {KEYSPACE}.check SET amount = ?  WHERE name = 'total'
"""

FETCH_TOTAL = f"""
SELECT amount FROM {KEYSPACE}.check WHERE name = 'total'
"""

COUNT_ACCOUNT = f"""
SELECT count(1) FROM {KEYSPACE}.accounts
"""

DROP_KS = f"""
DROP KEYSPACE IF EXISTS {KEYSPACE}
"""


class SetupError(Exception):
    pass


def t4(x):
    """Return trailing 4 chars of string representation"""
    return str(x)[-4:]


def t5(x):
    """Return trailing 5 chars of string representation"""
    return str(x)[-5:]


# Recipe from itertools docs, pick n at a time; warning: skips last if not even!


def grouped(iterable, n):
    "s -> (s0,s1,s2,...sn-1), (sn,sn+1,sn+2,...s2n-1), (s2n,s2n+1,s2n+2,...s3n-1), ..."
    return zip(*[iter(iterable)] * n)


class Account:
    """Representation of an account within a transfer"""

    __hash__ = None

    def __init__(self, bic_n=None, ban_n=None):
        """Initialize account from numeric bic and ban, using proper encoding"""
        self.bic = self.create_bic(bic_n if bic_n else randrange(0, BICS))
        self.ban = self.create_ban(ban_n if ban_n else randrange(0, BANS))
        self.balance = None
        self.pending_amount = None
        self.found = False  # Set when found on DB, so it needs to be unlocked later

    def __eq__(self, other):
        return (self.bic, self.ban) == (other.bic, other.ban)

    def __ne__(self, other):
        return (self.bic, self.ban) != (other.bic, other.ban)

    def __lt__(self, other):
        return (self.bic, self.ban) < (other.bic, other.ban)

    def __repr__(self):
        return f"Account({t4(self.bic)}:{t4(self.ban)} balance {self.balance} pending amount {self.pending_amount})"

    @classmethod
    def create_bic(cls, bic_n):
        """Create deterministic/sequential Bank Identification Code"""
        # Bank code 4, country 2 (3b), (location 2, branch 3) (4b)
        assert 0 <= (bic_n >> (4 + 3)) <= 0x1FFF
        return f"{bic_n >> (4 + 3):04x}-{COUNTRY_CODE[(bic_n >> 4) & 7]}-{bic_n & 0xF:05x}"

    @classmethod
    def create_ban(cls, ban_n):
        """Generate deterministic/sequential UUID"""
        assert 0 <= ban_n <= 0xFFFFFFFFFFFF  # hex 12 chars, no negative sign
        return f"00000000-0000-0000-0000-{ban_n:012x}"


class Transfer:
    def __init__(self, id_int=None, id_uuid=None, set_amount=True):
        if id_int:
            self.id = uuid.UUID(f"00000000-0000-0000-0000-{id_int:012x}")
        elif id_uuid:
            self.id = id_uuid  # Recovered
        else:
            self.id = uuid.uuid4()  # Random
        self.amount = Decimal(randrange(TRANSFER_MIN, TRANFER_MAX)) if set_amount else None
        self.src = Account()
        while True:
            # Pick a different account as destination (unlikely same)
            self.dst = Account()
            if self.src != self.dst:
                break
        self.state = "new"

    def init_accounts(self):
        self.src.pending_amount = -self.amount
        self.dst.pending_amount = self.amount

    def __repr__(self):
        return f"Transfer(id={t5(self.id)}, amount={self.amount}, {self.src}, {self.dst}, {self.state})"


class TrackingAccount:
    """Representation of an account for consistency checks (Oracle)"""

    def __init__(self, bic, ban, balance):
        self.bic = bic
        self.ban = ban
        self.balance = balance
        self.tx_id = None

    def set_transfer(self, tx_id):
        assert not self.tx_id or self.tx_id == tx_id, f"set_transfer: on {t4(self.bic)}:{t4(self.ban)} current transfer id {self.tx_id}, setting {t5(tx_id)}"
        self.tx_id = tx_id

    def clear_transfer(self):
        self.tx_id = None

    def begin_debit(self, tx_id):
        self.set_transfer(tx_id)

    def complete_debit(self, tx_id, amount):
        self.clear_transfer()
        self.balance -= amount

    def begin_credit(self, tx_id):
        self.set_transfer(tx_id)

    def complete_credit(self, tx_id, amount):
        self.clear_transfer()
        self.balance += amount

    def __repr__(self):
        return f"TrackingAccount(bic={t4(self.bic)}, ban={t4(self.ban)})"


class Oracle:
    """State consistency tracking (independent of DB) shared across all workers"""

    def __init__(self, session):
        manager = _mp_fork.Manager()
        self.lock = manager.Lock()  # https://github.com/PyCQA/pylint/issues/3313 pylint: disable=no-member
        self.accounts = manager.dict()
        self.transfers = manager.dict()
        # Load accounts from DB
        ret = session.execute("SELECT bic, ban, balance FROM accounts")
        assert ret and len(ret.current_rows)
        for row in ret:
            bic, ban, balance = row.bic, row.ban, row.balance
            self.accounts[bic + ban] = TrackingAccount(bic, ban, balance)

    def lookup_accounts(self, client_id, src, dst):
        src = self.accounts.get(src.bic + src.ban, None)
        assert src, f"{t4(client_id)} lookup_accounts: could not find src {src}"
        dst = self.accounts.get(dst.bic + dst.ban, None)
        assert dst, f"{t4(client_id)} lookup_accounts:could not find dst {dst}"
        return src, dst

    # Worker or recovery process
    def begin_transfer(self, client_id, session, t, amount):
        with self.lock:
            if t.id in self.transfers:
                logger.info(f"{t4(client_id)} {t5(t.id)} begin_transfer: Double execution of the same transfer")
                # Have processed this transfer already (i.e. recovery after balance update)
                return

            src, dst = self.lookup_accounts(client_id, t.src, t.dst)
            if src and dst and src.balance > amount:
                src.begin_debit(t.id)
                dst.begin_credit(t.id)

    # Worker or recovery process
    def complete_transfer(self, client_id, session, t):
        # Lock while both accounts are being updated
        with self.lock:
            if t.id in self.transfers:
                return  # Have processed this transfer already
            self.transfers[t.id] = True
            src, dst = self.lookup_accounts(client_id, t.src, t.dst)
            logger.info(f"{t4(client_id)} {t5(t.id)} oracle.complete_transfer: before {src.balance} {dst.balance}")
            if src and dst and t.amount <= src.balance:
                src.complete_debit(t.id, t.amount)
                dst.complete_credit(t.id, t.amount)
                logger.info(f"{t4(client_id)} {t5(t.id)} oracle.complete_transfer: updated {t}")
                logger.info(f"{t4(client_id)} {t5(t.id)} oracle.complete_transfer: after {src.balance} {dst.balance}")
            else:
                logger.info(f"{t4(client_id)} {t5(t.id)} oracle.complete_transfer: not updated {t}")

            self.accounts.update({src.bic + src.ban: src, dst.bic + dst.ban: dst})

    # One process
    def find_broken_accounts(self, sessions):
        """Perform full scan on every node to find inconsistent accounts"""
        start_oracle = time()
        expected = dict(((acct.bic, acct.ban), acct.balance) for acct in self.accounts.values())
        suspect = set()
        for session in sessions:
            ret = session.execute(f"SELECT bic, ban, balance FROM {KEYSPACE}.accounts ALLOW FILTERING")
            for row in ret.current_rows:
                if expected[(row.bic, row.ban)] != row.balance:
                    suspect.add((row.bic, row.ban))
        # Now query every node for the suspect accounts
        stmt = session.prepare(f"SELECT balance, pending_transfer, pending_amount FROM {KEYSPACE}.accounts WHERE bic = ? AND ban = ?")
        stmt.consistency_level = ConsistencyLevel.SERIAL
        broken = []
        for bic, ban in suspect:
            ret = session.execute(stmt, [bic, ban])
            if expected[(bic, ban)] != ret[0].balance:
                broken.append([bic, ban, expected[(bic, ban)], ret[0].balance])
        broken.sort()
        if not broken:
            logger.info(f"oracle: no broken accounts")
        else:
            logger.info(f"oracle: {len(broken)} broken accounts:")
            logger.info("bic,                        ban,                   expected, actual, difference")
            for (
                bic,
                ban,
                expected,
                actual,
            ) in broken:
                logger.info(f"{bic}, {ban}, {expected},     {actual},     {expected - actual}")
        logger.info(f"oracle: consistency check {time() - start_oracle:.02f} seconds")


def node_affinity(node_pids):
    """Set up node affinity to avoid bouncing processes
    This only works for nodes <= physical cpu cores
    """

    python_proc = psutil.Process(pid=getpid())
    threads = psutil.cpu_count()
    cores = psutil.cpu_count(logical=False)
    threads_per_core = threads // cores

    assert len(node_pids) <= threads - 1
    # Run Python dtest *and its workers* in last thread of last core
    python_proc.cpu_affinity(range(len(node_pids) * threads_per_core, threads - 1))
    logger.info(f"python pid {python_proc.pid}, affinity {python_proc.cpu_affinity()}")
    logger.info(f"nodes {len(node_pids)}, cores {cores}")
    for i in range(len(node_pids)):
        node_proc = psutil.Process(pid=node_pids[i])
        node_proc.cpu_affinity([i * threads_per_core])
        logger.info(f"node.pid {node_pids[i]} new affinity {node_proc.cpu_affinity()}")


# Send raised exception to parent through a pipe
# https://www.programmersought.com/article/88231439877/
# Use "fork" context explicitly: Python 3.14 defaults to "forkserver" which
# requires pickling the target, but self (Tester) contains unpicklable objects
# like ScyllaCluster with thread locks.
_mp_fork = mp.get_context("fork")


class Process(_mp_fork.Process):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._pconn, self._cconn = _mp_fork.Pipe()
        self._exception = None

    def run(self):
        try:
            _mp_fork.Process.run(self)
            self._cconn.send(None)
        except Exception as e:  # noqa: BLE001
            tb = traceback.format_exc()
            # NOTE: the exception can't be pickled so just send the trace
            self._cconn.send(tb)

    @property
    def exception(self):
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception


@pytest.mark.dtest_full
@pytest.mark.dtest_heavy
@pytest.mark.dtest_debug
@pytest.mark.scylla_mode("!release")
class TestLWTBankingLoad(Tester):
    """Emulate a series of money transfers and perform validation"""

    def prepare(self, num_nodes=NODES):
        """Set up cluster, schema, tables"""
        cluster = self.cluster
        cluster.set_configuration_options(
            values={
                "hinted_handoff_enabled": False,
            }
        )
        jvm_args = ["--smp", str(num_nodes)]
        if not cluster.nodelist():
            cluster.populate(generate_cluster_topology_based_rf(dc_num=1, nodes=num_nodes, rf=RF))
            cluster.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=jvm_args)

        node_list = cluster.nodelist()
        for node in node_list:
            assert len(node.all_pids) == 1
        node_affinity([node.pid for node in node_list])

        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        self.create_schema(session)
        self.create_prepared_queries(session)

        self.ignore_log_patterns.extend(
            [
                "exception during mutation write",
                "injected_error",
                "Failed to remove mutations from batchlog",
                "mutation_write_timeout_exception",
                "mutation_write_failure_exception",
            ]
        )
        return session

    def create_schema(self, session):
        logger.info("Creating schema...")
        create_ks(session=session, name=KEYSPACE, rf=RF)
        session.execute(CREATE_SETTINGS_TAB)
        session.execute(CREATE_ACCOUNTS_TAB)
        session.execute(CREATE_TRANSFERS_TAB)
        session.execute(CREATE_CHECK_TAB)

    def create_prepared_queries(self, session):
        logger.info("Create prepared queries")

        self.insert_setting_stmt = session.prepare(INSERT_SETTING)
        self.fetch_setting_stmt = session.prepare(FETCH_SETTING)
        self.insert_account_stmt = session.prepare(INSERT_ACCOUNT)
        self.insert_transfer_stmt = session.prepare(INSERT_TRANSFER)
        self.insert_transfer_stmt.consistency_level = ConsistencyLevel.QUORUM
        self.set_transfer_client_stmt = session.prepare(SET_TRANSFER_CLIENT)
        self.set_transfer_client_refresh_stmt = session.prepare(SET_TRANSFER_CLIENT_REFRESH)
        self.set_transfer_state_stmt = session.prepare(SET_TRANSFER_STATE)
        self.clear_transfer_client_stmt = session.prepare(CLEAR_TRANSFER_CLIENT)
        self.delete_transfer_stmt = session.prepare(DELETE_TRANSFER)
        self.delete_transfer_orphaned_stmt = session.prepare(DELETE_TRANSFER_ORPHANED)
        self.fetch_transfer_stmt = session.prepare(FETCH_TRANSFER)
        self.fetch_transfer_stmt.consistency_level = ConsistencyLevel.SERIAL
        self.fetch_transfer_client_stmt = session.prepare(FETCH_TRANSFER_CLIENT)
        self.fetch_transfer_client_stmt.consistency_level = ConsistencyLevel.SERIAL
        self.fetch_dead_transfers_stmt = session.prepare(FETCH_DEAD_TRANSFERS)
        self.lock_account_stmt = session.prepare(LOCK_ACCOUNT)
        self.unlock_account_stmt = session.prepare(UNLOCK_ACCOUNT)
        self.fetch_balance_stmt = session.prepare(FETCH_BALANCE)
        self.fetch_balance_stmt.consistency_level = ConsistencyLevel.SERIAL
        self.update_balance_unlock_stmt = session.prepare(UPDATE_BALANCE)
        self.check_balance_stmt = session.prepare(CHECK_BALANCE)
        self.check_balance_stmt.consistency_level = ConsistencyLevel.SERIAL
        self.persist_total_stmt = session.prepare(PERSIST_TOTAL)
        self.fetch_total_stmt = session.prepare(FETCH_TOTAL)
        self.fetch_total_stmt.consistency_level = ConsistencyLevel.SERIAL
        self.count_account_stmt = session.prepare(COUNT_ACCOUNT)
        self.count_account_stmt.consistency_level = ConsistencyLevel.QUORUM

    def initial_settings(self, session, settings=None):
        if settings is None:
            settings = {}
        logger.info("Saving initial settings...")
        for key, val in settings.items():
            session.execute(self.insert_setting_stmt, [str(key), str(val)])

    def populate_worker(self, worker_n, slice_max):
        """Populate database with initial accounts with balance"""

        # Do batch inserts to speed up account setup
        bic_start = worker_n * slice_max  # Starting bic
        bic_end = min((worker_n + 1) * slice_max, BICS)  # Last worker has smaller slice
        # Slow environments take very long to come up, wait up to 600 seconds (10 minutes)
        session = self.patient_cql_connection(choice(self.cluster.nodelist()), timeout=600)
        account_stmt_cql = ["BEGIN BATCH\n"] + [INSERT_ACCOUNT] * INSERT_BATCH_LEN + ["APPLY BATCH"]
        account_stmt = session.prepare("".join(account_stmt_cql))
        account_stmt.consistency_level = ConsistencyLevel.QUORUM

        def new_random_balance():
            """Create a random starting balance for a bank account"""
            return Decimal(randint(BALANCE_INIT_MIN, BALANCE_INIT_MAX))

        for bic_n in range(bic_start, bic_end):
            bic = Account().create_bic(bic_n)
            for ban_n_batch in grouped(range(BANS), INSERT_BATCH_LEN):
                # Flatten INSERT_BATCH_LEN rows
                bic = Account.create_bic(bic_n)
                session.execute(account_stmt, [x for row in [(bic, Account.create_ban(ban_n), new_random_balance()) for ban_n in ban_n_batch] for x in row])

    def populate_accounts(self):
        """Populate accounts with pool for workers"""
        # Bunch of bics for each worker
        populate_procs = []
        slice_max = -(-BICS // MAX_WORKERS)  # Max slice size for a given worker
        workers = -(-BICS // slice_max)  # Max amount of workers with even workload

        logger.info(f"populate {BICS}*{BANS} = {BICS * BANS}, workers {workers}, {slice_max} max each")
        # NOTE: mp.Pool doesn't like functions in dtest/Tester/etc, but Process works fine.
        for worker_n in range(workers):
            proc = Process(target=self.populate_worker, args=(worker_n, slice_max))
            proc.start()  # Start right away
            populate_procs.append(proc)

        for idx, proc in enumerate(populate_procs):
            proc.join()
            if proc.exception:
                logger.error(proc.exception)
                raise SetupError(f"Setup worker {idx} failed: {proc.exception}")

    def fetch_account_balance(self, session, account):
        try:
            ret = session.execute(self.fetch_balance_stmt, [account.bic, account.ban])
        except (OperationTimedOut, Unavailable, ReadFailure) as exc:
            logger.info(f"fetch_account_balance: error account {account.bic} {account.ban} {exc}")
            return False

        account.balance = ret[0].balance
        account.pending_amount = ret[0].pending_amount
        account.found = True
        # logger.info(f"fetch_account_balance: {account}")
        return True

    def set_transfer_state(self, client_id, session, t, state):
        # logger.info(f"{t4(client_id)} {t5(t.id)} set_transfer_state: attempting to set {state}")
        try:
            ret = session.execute(self.set_transfer_state_stmt, [state, t.id, client_id])
        except (OperationTimedOut, WriteFailure, Unavailable) as exc:
            logger.info(f"{t4(client_id)} {t5(t.id)} set_transfer_state: timed out setting state {state}: {exc}")
            return False

        if not ret[0].applied:
            if hasattr(ret[0], "client_id"):
                logger.info(f"{t4(client_id)} {t5(t.id)} set_transfer_state: previous id {ret[0].client_id} {ret[0]}")
            else:
                logger.info(f"{t4(client_id)} {t5(t.id)} set_transfer_state: previous id not found?")
            return False
        else:
            t.state = state
            # logger.info(f"{t4(client_id)} {t5(t.id)} set_transfer_state: successfully set state {state} {ret[0]} ******** ({self.node_id})")
            return True

    # In case we failed for whatever reason try to clean up the transfer client
    # to allow speedy recovery
    def clear_transfer_client(self, client_id, session, tx_id):
        # logger.info(f"{t4(client_id)} clear_transfer_client: clearing {tx_id}")

        try:
            ret = session.execute(self.clear_transfer_client_stmt, [tx_id, client_id])
        except (OperationTimedOut, WriteFailure, Unavailable) as exc:
            logger.info(f"{t4(client_id)} {t5(tx_id)} clear_transfer_client: query failure {exc}")
            return False
        if not ret[0].applied:
            if not hasattr(ret[0], "client_id") or not ret[0].client_id:
                # The transfer is gone, do not complain
                logger.info(f"{t4(client_id)} {tx_id} clear_transfer_client: the transfer is already gone")
                return True
            else:
                logger.info(f"{t4(client_id)} {t5(tx_id)} clear_transfer_client: client id mismatch {ret[0].client_id} != {tx_id}")
                return False
        return True

    def complete_transfer(self, client_id, session, recovery_queue, t):  # noqa: PLR0912
        """Once accounts are properly locked perform the actual transfer"""

        assert t.state in ("locked", "completed"), f"{t4(client_id)} {t5(t.id)} Incorrect transfer state {t.state}"

        # logger.info(f"{t4(client_id)} {t5(t.id)} complete_transfer: state: {t.state}")

        if t.state == "locked":
            # logger.info(f"{t4(client_id)} {t5(t.id)} complete_transfer: locked")
            if self.oracle:
                self.oracle.begin_transfer(client_id, session, t, t.amount)

            # NOTE: difference with lightest
            for acct in [t.src, t.dst]:
                assert acct.found, f"{t4(client_id)} {t5(t.id)} complete_transfer: account {acct} not found"

            # Calcualte the destination state
            # logger.info(f"{t4(client_id)} {t5(t.id)} complete_transfer: Calculating balances")
            t.src.balance += t.src.pending_amount  # pending amount is negative already
            t.dst.balance += t.dst.pending_amount
            # NOTE: when called from recover the pending amounts could be 0 and already applied
            if t.src.balance >= 0:
                # logger.info(f"{t4(client_id)} {t5(t.id)} complete_transfer: Moving funds")

                # From now on we can ignore 'applied' - the record may
                # not be applied only if someone completed our transfer or
                # 30 seconds have elapsed.
                for acct in [t.src, t.dst]:
                    # logger.info(f"{t4(client_id)} {t5(t.id)} complete_transfer: going to update {acct} pending = 0")
                    try:
                        ret = session.execute(self.update_balance_unlock_stmt, [acct.balance, acct.bic, acct.ban, t.id])
                    except (OperationTimedOut, WriteFailure, Unavailable) as exc:
                        logger.info(f"{t4(client_id)} {t5(t.id)} complete_transfer: failed to update balance for {acct} {exc}")
                        logger.info(f"{t4(client_id)} {t5(t.id)} complete_transfer: adding {t} to the recovery queue +++++++++++++++++++")
                        self.clear_transfer_client(client_id, session, t.id)  # Leave to recovery
                        recovery_queue.put((t.id, client_id))
                        return "failed_to_complete"

                    # logger.info(f"{t4(client_id)} {t5(t.id)} complete_transfer: updated balance for {t4(acct.bic)} {t4(acct.ban)} to {acct.balance} {ret[0]}")

                    # NOTE: Update success only implies LWT quorum updated, not all nodes.
                    #       Non-serial and/or full scans will not show the updated account.

            else:
                logger.info(f"{t4(client_id)} {t5(t.id)} complete_transfer: Insufficient funds for {t4(acct.bic)} {t4(acct.ban)}: {t.src.balance} -> {t.src.pending_amount}")
                self.stats["not_enough_balance"] += 1

            if self.oracle:
                self.oracle.complete_transfer(client_id, session, t)

            if not self.set_transfer_state(client_id, session, t, "completed"):
                logger.info(f"{t4(client_id)} {t5(t.id)} complete_transfer: failed to set state completed")
                logger.info(f"{t4(client_id)} {t5(t.id)} complete_transfer: adding {t} to the recovery queue +++++++++++++++++++")
                self.clear_transfer_client(client_id, session, t.id)  # clear for recovery
                recovery_queue.put((t.id, client_id))
                return False

        for account in [t.src, t.dst]:
            # logger.info(f"{t4(client_id)} {t5(t.id)} Unlocking {t4(account.bic)} {t4(account.ban)}")
            try:
                res = session.execute(self.unlock_account_stmt, [account.bic, account.ban, t.id])
            except (OperationTimedOut, WriteFailure, Unavailable) as exc:
                logger.info(f"{t4(client_id)} {t5(t.id)} complete_transfer: Failed to unlock account {t4(account.bic)} {t4(account.ban)}: {exc}")
                res = False
            if res and not res[0].applied:
                logger.info(f"{t4(client_id)} {t5(t.id)} complete_transfer: Failed to unlock account {t4(account.bic)} {t4(account.ban)} {res[0]}")
            if not res or not res[0].applied:
                logger.info(f"{t4(client_id)} {t5(t.id)} complete_transfer: adding {t} to the recovery queue +++++++++++++++++++")
                self.clear_transfer_client(client_id, session, t.id)  # clear for recovery
                recovery_queue.put((t.id, client_id))
                return False

        ret = self.delete_transfer(client_id, session, t.id)
        if not ret:
            logger.info(f"{t4(client_id)} {t5(t.id)} complete_transfer: could not delete, adding {t} to the recovery queue +++++++++++++++++++")
            self.clear_transfer_client(client_id, session, t.id)  # clear for recovery
            recovery_queue.put((t.id, client_id))
        return True

    def lock_accounts(self, client_id, session, recovery_queue, t, wait=True):  # noqa: PLR0911, PLR0915
        """Set accounts with in progress values for transfer id and pending amount to credit/debit"""

        # Locking from recovery
        if t.state == "completed":
            # logger.info(f"{t4(client_id)} {t5(t.id)} lock_accounts: already completed ****")
            return True

        if t.state == "locked":
            # The transfer is already locked.
            # Fetch balance to find out if the account exists or not
            # logger.info(f"{t4(client_id)} {t5(t.id)} lock_accounts: already locked ****")
            if not self.fetch_account_balance(session, t.src):
                return False
            if not self.fetch_account_balance(session, t.dst):
                return False
            return True

        # Upon failure to take lock on the second account, we should try to rollback
        # lock on the first to avoid deadlocks. We shouldn't, however, accidentally
        # rollback the lock if we haven't taken it - in this case lock0
        # and lock1 both may have been taken, and the transfer have progressed
        # to moving the funds, so rolling back the lock would break isolation.

        # Always lock accounts in lexicographical order to avoid livelocks
        sleep_duration = LOCK_RETRY_SLEEP_INITIAL

        # logger.info(f"{t4(client_id)} {t5(t.id)} lock_accounts: Loop start ****")
        retries = 0

        acct1, acct2 = sorted([t.src, t.dst])

        def lock_account(account):
            try:
                # UPDATE {KEYSPACE}.accounts
                #   SET pending_transfer = ?, pending_amount = ?
                #   WHERE bic = ? AND ban = ?
                #   IF balance != NULL AND pending_amount != NULL and pending_transfer = NULL
                res = session.execute(self.lock_account_stmt, [t.id, account.pending_amount, account.bic, account.ban])
            except (OperationTimedOut, WriteFailure, Unavailable) as exc:
                logger.info(f"{t4(client_id)} {t5(t.id)} lock_accounts: lock query failed {account}: {exc}****")
                return False
            if res[0].applied or res[0].pending_transfer == t.id:
                # Either locked or already locked (prev lock query reported failure but it went through)
                account.balance = res[0].balance
                # logger.info(f"{t4(client_id)} {t5(t.id)} lock_accounts: locked {account} "
                #      f"pending_amount {account.pending_amount} res: {res[0]} ****")
                return True
            else:
                logger.info(f"{t4(client_id)} {t5(t.id)} lock_accounts: lock failed {account} {res[0]}****")
                return False

        def unlock_account(account, t, client_id):
            try:
                unlock_res = session.execute(self.unlock_account_stmt, [account.bic, account.ban, t.id])
            except (OperationTimedOut, WriteFailure, Unavailable) as exc:
                logger.info(f"""{t4(client_id)} {t5(t.id)} unlock_accounts: query error "{exc}" {account} ****""")
                unlock_res = False
            if not unlock_res or not unlock_res[0].applied:
                logger.info(f"{t4(client_id)} {t5(t.id)} unlock_accounts: error unlocking {account} ****")
                return False
            logger.info(f"{t4(client_id)} {t5(t.id)} unlock_accounts: unlocked {account} ****")
            account.found = False
            account.balance = None
            return True

        while True:
            if lock_account(acct1):
                if lock_account(acct2):
                    acct1.found = acct2.found = True
                    if self.set_transfer_state(client_id, session, t, "locked"):
                        # logger.info(f"{t4(client_id)} {t5(t.id)} lock_accounts: done LOCKED")
                        return True
                    else:
                        logger.info(f"{t4(client_id)} {t5(t.id)} lock_accounts: done FAIL (to set state)")
                        if unlock_account(acct1, t, client_id) and unlock_account(acct2, t, client_id) and self.delete_transfer(client_id, session, t.id):
                            return False  # Failed but doesn't need to fix accounts or transfer
                        # Either accounts need unlocking or transfer needs to be deleted
                        logger.info(f"{t4(client_id)} {t5(t.id)} lock_accounts: adding {t} to the recovery queue +++++++++++++++++++")
                        self.clear_transfer_client(client_id, session, t.id)  # Leave to recovery
                        recovery_queue.put((t.id, client_id))
                        return False
                elif not unlock_account(acct1, t, client_id):  # Unsets found and balance
                    logger.info(f"{t4(client_id)} {t5(t.id)} lock_accounts: adding {t} to the recovery queue +++++++++++++++++++")
                    self.clear_transfer_client(client_id, session, t.id)  # Leave to recovery
                    recovery_queue.put((t.id, client_id))
                    return False

            # Could not lock sleep before retrying
            # But first reset client id to prevent expire while sleeping
            self.set_transfer_client_refresh(client_id, session, t.id)
            sleep(sleep_duration)
            # logger.info(f"{t4(client_id)} {t5(t.id)} lock_accounts: Restarting after sleeping {sleep_duration:.7}")
            sleep_duration += LOCK_RETRY_SLEEP_FACTOR * random()  # Random retry backoff
            sleep_duration = min(sleep_duration, LOCK_RETRY_SLEEP_MAX)
            retries += 1
            if retries > MAX_LOCK_RETRIES:
                logger.info(f"{t4(client_id)} {t5(t.id)} lock_accounts: max retries reached {MAX_LOCK_RETRIES}")
                self.clear_transfer_client(client_id, session, t.id)  # Leave to recovery
                recovery_queue.put((t.id, client_id))
                return False

    def register_transfer(self, client_id, session, t):
        """Register a new transfer in the database"""
        try:
            ret = session.execute(self.insert_transfer_stmt, [t.id, t.src.bic, t.src.ban, t.dst.bic, t.dst.ban, t.amount])
        except (OperationTimedOut, WriteFailure, Unavailable) as exc:
            logger.info(f"{t4(client_id)} {t5(t.id)} register_transfer: query failed: {exc}")
            return False

        if not ret[0].applied:
            # NOTE: does this ever happen?
            logger.info(f"{t4(client_id)} {t5(t.id)} register_transfer: failed {ret[0]}")
            return False

        # If timed out t.state is unchanged
        ret = self.set_transfer_client(client_id, session, t.id)
        if ret:
            logger.info(f"{t4(client_id)} {t5(t.id)} register_transfer: success **** ({self.node_id})")
            return True
        else:
            logger.info(f"{t4(client_id)} {t5(t.id)} register_transfer: failed to set client")
            return False

    # Accept interfaces to allow nil client id
    def set_transfer_client(self, client_id, session, tx_id):
        """Temporarily set a client id for a transfer"""
        # NOTE: it might already be set, in case of failure check existing client_id
        try:
            ret = session.execute(self.set_transfer_client_stmt, [client_id, tx_id])
        except (OperationTimedOut, WriteFailure, Unavailable) as exc:
            logger.info(f"{t4(client_id)} {t5(tx_id)} set_transfer_client: query failed {exc}")
            return False
        if not ret[0].applied:
            if not ret[0].client_id:
                logger.info(f"{t4(client_id)} {t5(tx_id)} set_transfer_client: Failed to set client: no such transfer {ret[0]}")
            elif ret[0].client_id != client_id:
                # The still has set original worker client_id (no TTL)
                logger.info(f"{t4(client_id)} {t5(tx_id)} set_transfer_client: previous id {t4(ret[0].client_id)} {ret[0]}")
            else:
                # NOTE: careful with possible race condition due to expiring TTL
                logger.info(f"{t4(client_id)} {t5(tx_id)} set_transfer_client: ???? {ret[0]}")
                return True  # Already set   client_id == prev client id

        else:  # applied
            # logger.info(f"{t4(client_id)} {t5(tx_id)} set_transfer_client: success {ret[0]}")
            return True

    # Accept interfaces to allow nil client id
    def set_transfer_client_refresh(self, client_id, session, tx_id):
        """Refresh client id for a transfer (update TTL)"""
        try:
            ret = session.execute(self.set_transfer_client_refresh_stmt, [client_id, tx_id, client_id])
        except (OperationTimedOut, WriteFailure, Unavailable) as exc:
            logger.info(f"{t4(client_id)} {t5(tx_id)} set_transfer_client_refresh: failed {exc}")
            return
        if not ret[0].applied:
            logger.info(f"{t4(client_id)} {t5(tx_id)} set_transfer_client_refresh: failed {ret[0]}")

    def delete_transfer(self, client_id, session, tx_id):
        """Delete transfer from pending"""
        try:
            res = session.execute(self.delete_transfer_stmt, [tx_id, client_id])
        except (OperationTimedOut, WriteFailure, Unavailable):
            logger.info(f"{t4(client_id)} {t5(tx_id)} delete_transfer: deletion timed out")
            return False
        if res[0].applied:
            # logger.info(f"{t4(client_id)} {t5(tx_id)} delete_transfer: deleted transfer")
            return True
        elif res[0].client_id is None:
            logger.info(f"{t4(client_id)} {t5(tx_id)} delete_transfer: client_id not set, ttl? {res[0]}")
            try:
                res = session.execute(self.delete_transfer_orphaned_stmt, [tx_id])
            except (OperationTimedOut, WriteFailure, Unavailable) as exc:
                res = False
            if not res or not res[0].applied:
                return False
            return True
        else:
            logger.info(f"{t4(client_id)} {t5(tx_id)} delete_transfer: failed to delete transfer {res[0]} {time():.02f}")
            return False

    def make_transfer(self, client_id, session, recovery_queue, t):
        """Make a transfer"""

        # logger.info(f"{t4(client_id)} {t5(t.id)} make_transfer: {t.src} to {t.dst} for {t.amount}")
        if not self.register_transfer(client_id, session, t):
            return "failed_to_register"

        if self.lock_accounts(client_id, session, recovery_queue, t, wait=True):
            if self.complete_transfer(client_id, session, recovery_queue, t):
                return "success"
            else:
                return "failed_to_complete"
        else:
            return "failed_to_lock"

    def pay_worker(self, worker_id, transfers, oracle, recovery_queue, stats_queue):
        """Worker performing payments (transfers)"""
        self.oracle = oracle  # None if not set
        self.stats = defaultdict(int)
        seed(getpid())  # Different seed for each worker
        client_id = uuid.uuid4()
        self.node_id = randint(0, len(self.cluster.nodelist()) - 1)
        session = self.patient_cql_connection(self.cluster.nodelist()[self.node_id])
        logger.info(f"{t4(client_id)} starting on node {self.node_id}")
        worker_transfers = 0  # Transfers done by this worker
        while True:
            with transfers.get_lock():
                if transfers.value == TOTAL_TRANSFERS:
                    break  # Done
                id_int = transfers.value  # decrementing fixed ids
                transfers.value += 1

            # logger.info(f"{t4(client_id)} making transfer for {src_bic} {src_ban} -> {dst_bic} {dst_ban} for {amount}")
            worker_transfers += 1

            start = time()
            t = Transfer(id_int=id_int)
            t.init_accounts()
            transfer_result = self.make_transfer(client_id, session, recovery_queue, t)
            elapsed = time() - start
            self.update_worker_stats(elapsed, transfer_result)

        stats_queue.put(self.stats)  # Send stats to parent

        logger.info(
            f"""{t4(client_id)} worker finished: pass {self.stats["success"]} """
            f"""no balance {self.stats["not_enough_balance"]} """
            f"""register fail {self.stats["failed_to_register"]} """
            f"""lock fail {self.stats["failed_to_lock"]} """
            f"""complete fail {self.stats["failed_to_complete"]} """
            f"""total {worker_transfers} on {self.node_id}"""
        )

    def update_worker_stats(self, elapsed, transfer_result):
        self.stats[transfer_result] += 1
        self.stats["pay_total"] += elapsed
        self.stats["pay_max"] = max(elapsed, self.stats["pay_max"])
        if self.stats["pay_min"] == 0 or elapsed < self.stats["pay_min"]:
            self.stats["pay_min"] = elapsed

    def run_pay_workers(self, session, transfers, oracle, recovery_queue, stats_queue):
        """Worker jobs doing random payments"""

        payment_procs = []
        # Maximum workers - 1 for nemesis

        logger.info(f"payment workers {MAX_WORKERS} for {TOTAL_TRANSFERS} transfers")
        for worker_n in range(MAX_WORKERS):
            proc = _mp_fork.Process(target=self.pay_worker, args=(worker_n, transfers, oracle, recovery_queue, stats_queue))
            proc.start()  # Start right away
            payment_procs.append(proc)

        for proc in payment_procs:
            proc.join()

    def recovery_worker(self, oracle, recovery_queue, stats_queue):
        self.oracle = oracle  # None if not set
        client_id = uuid.uuid4()
        self.node_id = randint(0, len(self.cluster.nodelist()) - 1)
        session = self.patient_cql_connection(self.cluster.nodelist()[self.node_id])
        logger.info(f"recovery_worker: starting on {self.node_id}")

        self.stats = defaultdict(int)

        last_retry_limit = 0

        # Runs in sub-process
        while True:
            tx_id, prev_cli = recovery_queue.get()
            if tx_id is False:
                if recovery_queue.qsize() == 0:
                    break  # Workers are done and no more recoveries left
                # Still some recoveries left, but don't loop forever
                last_retry_limit += 1
                if last_retry_limit > RECOVERY_LAST_RETRY_LIMIT:
                    break  # Stop retrying
                recovery_queue.put((False, False))
                continue

            # logger.info(f"{t4(client_id)} recovery_worker: recovering {t5(tx_id)}")
            # logger.info(f"{t4(client_id)} recovering {t5(tx_id)}")
            if not self.recover_transfer(client_id, prev_cli, session, recovery_queue, tx_id):
                logger.info(f"{t4(client_id)} {t5(tx_id)} failed to recover")
                self.stats["recovery_failed"] += 1
            self.stats["recovered"] += 1

        logger.info(f"""recovery_worker: finished {self.stats["recovered"]}, failed {self.stats["recovery_failed"]}""")
        stats_queue.put(self.stats)  # Send stats to parent

    def recover_transfer(self, client_id, prev_cli, session, recovery_queue, tx_id):
        t = Transfer(id_uuid=tx_id, set_amount=False)

        try:
            ret = session.execute(self.fetch_transfer_stmt, [tx_id])
        except (OperationTimedOut, Unavailable, ReadFailure) as exc:
            # Ignore possible error, we will retry
            logger.info(f"{t4(client_id)} recover_transfer: {t5(tx_id)} not found when fetching for recovery {exc}")
            return False

        # logger.info(f"{t4(client_id)} recover_transfer: {t.state} {t} **************************")

        try:
            logger.info(f"{t4(client_id)} recover_transfer: {t.state} {t} {ret[0]} ***************** {self.node_id}")
        except IndexError:
            logger.info(f"{t4(client_id)} recover_transfer: ERROR RECOVERING {tx_id} ***************** {self.node_id}")
            return False
        assert ret[0].amount is not None, f"Transfer amount not set?!?! {ret[0]}"
        t.src.bic = ret[0].src_bic
        t.src.ban = ret[0].src_ban
        t.dst.bic = ret[0].dst_bic
        t.dst.ban = ret[0].dst_ban
        t.amount = ret[0].amount
        t.state = ret[0].state

        if not self.set_transfer_client(client_id, session, tx_id):
            logger.info(f"{t4(client_id)} recover_transfer: Failed to set client for {t5(tx_id)} re-adding to recovery queue")
            self.clear_transfer_client(client_id, session, tx_id)  # clear for next recovery?
            recovery_queue.put((tx_id, client_id))
            return False

        t.init_accounts()

        if not self.lock_accounts(client_id, session, recovery_queue, t, wait=False):
            logger.info(f"{t4(client_id)} recover_transfer: Failed to lock accounts for {t5(tx_id)}")
            self.clear_transfer_client(client_id, session, tx_id)  # clear for next recovery
            return False

        if not self.complete_transfer(client_id, session, recovery_queue, t):
            logger.info(f"{t4(client_id)} recover_transfer: Failed to complete {t5(tx_id)}")
            return False

        # logger.info(f"{t4(client_id)} recover_transfer: recovered {t5(tx_id)}")
        return True

    def nemesis(self, stop, sleep_sec=NEMESIS_ERROR_DURATION):
        """Nemesis worker injecting random errors in random nodes, one at a time"""

        node_len = len(self.cluster.nodelist())

        count = 0
        while not stop.is_set():
            node_id = randrange(0, node_len)
            error_name = choice(ERROR_INJECTIONS)
            # logger.info(f"nemesis enabling {error_name} on {node_id}")
            self.disable_errors(node_id)  # First disable other errors, avoid piling up

            self.enable_error(error_name, node_id, one_shot=True)
            sleep(sleep_sec)
            count += 1

        logger.info(f"Nemesis ran for {count} iterations")

    def test_bank_with_nemesis(self, with_oracle=True):
        start = time()
        session = self.prepare()
        logger.info(f"Prepare ran in {int(time() - start)} seconds")

        start = time()
        self.populate_accounts()
        result = session.execute(self.count_account_stmt)
        logger.info(f"workers done populating database {time() - start:.02f} seconds {result[0].count} accounts - {BICS * BANS}")
        assert result[0].count == BICS * BANS

        settings = {
            "bics": BICS,
            "bans": BANS,
            "accounts": BICS * BANS,
            "workers": MAX_WORKERS,
            "oracle": with_oracle,
        }

        self.initial_settings(session, settings)

        stop = _mp_fork.Event()  # Signal nemesis/recovery to stop
        logger.info(f"====================== starting ===============================")
        oracle = Oracle(session) if with_oracle else None
        recovery_queue = _mp_fork.Queue()
        stats_queue = _mp_fork.Queue()
        recovery_proc = _mp_fork.Process(target=self.recovery_worker, args=(oracle, recovery_queue, stats_queue))
        recovery_proc.start()
        nemesis_proc = _mp_fork.Process(target=self.nemesis, args=(stop,))
        nemesis_proc.start()
        start_pay = time()
        transfers = _mp_fork.Value("i", 0)  # Shared counter
        self.run_pay_workers(session, transfers, oracle, recovery_queue, stats_queue)  # Returns when all done and joined
        pay_time = time() - start_pay
        logger.info(f"pay workers {pay_time:.02f} seconds, {(BICS * BANS) / pay_time:.02f} transfers/second")
        stop.set()  # Stop nemesis
        recovery_queue.put((False, False))  # Stop recovery
        nemesis_proc.join()
        recovery_proc.join()

        if oracle:
            sessions = [self.patient_cql_connection(node) for node in self.cluster.nodelist()]
            oracle.find_broken_accounts(sessions)

        # Aggregate stats
        stats = defaultdict(int)
        while not stats_queue.empty():
            w_stats = stats_queue.get()
            for k, v in w_stats.items():
                if k in ["pay_total", "retries", "retry_limit_reached", "success", "not_enough_balance", "failed_to_complete", "failed_to_lock", "failed_to_register", "recovery_failed", "recovered"]:
                    stats[k] += v
            stats["pay_total"] += w_stats["pay_total"]
            stats["pay_max"] = max(stats["pay_max"], w_stats["pay_max"])
            if stats["pay_min"] == 0 or stats["pay_min"] > w_stats["pay_min"]:
                stats["pay_min"] = w_stats["pay_min"]

        if all(k in stats for k in ["pay_total", "pay_max", "pay_min"]):
            # print avg
            logger.info(f"""pay: {stats["pay_max"]:.02f} max, {stats["pay_min"]:.06f} min, """ f"""{stats["pay_total"] / TOTAL_TRANSFERS:.06f} avg""")

        logger.info(f"""lock retries {stats["retries"]}, limit reached {stats["retry_limit_reached"]}""")
        logger.info(f"""no balance {stats["not_enough_balance"]} (counts as success)""")
        logger.info(f"""recovery failures {stats["recovery_failed"]}, total {stats["recovered"]}""")

        logger.info(
            f"""success {stats["success"]} """
            f"""register fail {stats["failed_to_register"]} """
            f"""lock fail {stats["failed_to_lock"]} """
            f"""complete fail {stats["failed_to_complete"]} """
            f"""recovery left {recovery_queue.qsize()} """
        )
