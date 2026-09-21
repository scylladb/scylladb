import itertools
import logging
import random
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests
from cassandra import InvalidRequest, OperationTimedOut, ReadTimeout
from cassandra.cluster import NoHostAvailable, Session
from cassandra.protocol import SyntaxException

from dtest_class import Tester, create_ks, get_ip_from_node, read_barrier, retry_till_success, wait_for
from tools.data import create_c1c2_table, insert_c1c2
from tools.sla import Role, ServiceLevel, User
from tools.units import ScyllaDuration

logger = logging.getLogger(__name__)


class SLATester(Tester):
    def prepare(self, nodes: int = 1, smp=1, additional_config=None) -> Session:
        config = {"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator", "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer", "role_manager": "org.apache.cassandra.auth.CassandraRoleManager"}

        if additional_config is not None:
            config = config | additional_config

        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes)
        jvm_args = ["--smp", str(smp)]
        self.cluster.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=jvm_args)
        session = self.patient_cql_connection(self.cluster.nodelist()[0], user="cassandra", password="cassandra")
        return session

    @staticmethod
    def populate_data(session: Session, number_of_keys: int, replication_factor: int = 1):
        create_ks(session=session, name="ks", rf=replication_factor)
        create_c1c2_table(session=session)
        insert_c1c2(session=session, n=number_of_keys)

    @staticmethod
    def create_entity_with_service_level(entity, service_level: ServiceLevel):
        service_level.create()
        entity.create()
        entity.attach_service_level(service_level=service_level)
        return entity


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.next_gating
class TestSLA(SLATester):
    @staticmethod
    def _validate_sla(service_level: ServiceLevel):
        listed_sl = service_level.list_service_level()
        assert service_level == listed_sl, f"Expected created service level {service_level.name} to be equal to {listed_sl.name}, but it was not. \nExpected: {service_level}\nActual: {listed_sl}"

    def validate_sl_list(self, session: Session, expected_service_levels: list[ServiceLevel]):
        """
        Validates if the provided SL list is the same as the list of
        all the Service Levels in the db.
        If an empty list is provided or the expected_service_levels
        is None, it will query the db using a 'dummy' SL and check
        if no other SLs exist.
        If the provided list is not empty, it will query the db
        using the first SL in the list and check the SLs listed
        in the db against those provided as expected_service_levels.
        """
        if not expected_service_levels:
            dummy_sl = ServiceLevel(session=session, name="dummy").create()
            all_service_levels = [sl for sl in dummy_sl.list_all_service_levels(skip_driver=True) if (sl.name != '"dummy"')]
            assert not all_service_levels, f"Expected to find no service levels, but found some: {all_service_levels}"
        else:
            first_sl = expected_service_levels[0]
            full_service_levels_list = first_sl.list_all_service_levels(skip_driver=True)
            assert len(full_service_levels_list) == len(expected_service_levels)

            for sl in expected_service_levels:
                self._validate_sla(service_level=sl)

    @staticmethod
    def validate_attached_slas_list(session: Session, entity: Role | User, expected_service_levels: list[ServiceLevel]):
        """
        Checks whether a given entity's attached SL list is equal to
        the provided expected_service_levels list.
        """
        rows = entity.list_user_role_attached_service_levels()

        assert len(rows) == len(expected_service_levels), f"Actual number of attached service levels is different than expected. Expected list: {expected_service_levels}\nActual list: {rows}"
        for row in rows:
            sl = ServiceLevel(session=session, name=row.service_level)
            expected_service_level = [item for item in expected_service_levels if item.name == sl.name]
            assert len(expected_service_level) == 1, f"Did not find the expected service level: {sl} in the attached service level list: {expected_service_levels}"
            assert sl.list_service_level() == expected_service_level[0], "Listed attached service level did not match expected service level."

    @pytest.mark.parametrize(argnames=["sla_name"], argvalues=[["sla1"], ["Sla1"]])
    def test_sla(self, sla_name: str):
        """
        Create an SL with 100 shares using different strings as names.
        Validate that the SL created in the db is the same as
        the test model (i.e. same name and attributes).
        """
        session = self.prepare()
        sl = ServiceLevel(session=session, name=sla_name, shares=100).create()

        self.validate_sl_list(session=session, expected_service_levels=[sl])

    @pytest.mark.parametrize(
        argnames=["entity_class", "entity_name", "entity_pass", "entity_login"],
        argvalues=[[Role, "role1", None, False], [User, "user1", None, False], [Role, "auth_role", "auth", True], [User, "auth_user", None, False]],
        ids=["attach_to_role", "attach_to_user", "attach_to_auth_role", "attach_to_auth_user"],
    )
    def test_sla_attached_to_entity(self, entity_class, entity_name: str, entity_pass: str, entity_login: bool):
        """
        1. Create SL with 100 shares.
        2. Create an entity (Role / User) with authentication
        settings.
        3. Attach SL to entity.
        Assert that SL attached to the entity is the same as the
        expected by the test model (i.e. same name and attributes).
        """
        session = self.prepare()

        sl = ServiceLevel(session=session, name="sla1", shares=100)
        entity_kwargs = {"session": session, "name": entity_name, "password": entity_pass}

        if entity_login:
            entity_kwargs["login"] = entity_login

        entity = entity_class(**entity_kwargs)
        self.create_entity_with_service_level(entity=entity, service_level=sl)

        self.validate_sl_list(session=session, expected_service_levels=[sl])
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[sl])

    @pytest.mark.parametrize(
        argnames=["entity_class", "entity_name", "entity_pass", "entity_login"],
        argvalues=[[Role, "auth_role", "password", True], [User, "auth_user", "password", False]],
        ids=[
            "attach_to_role",
            "attach_to_user",
        ],
    )
    @pytest.mark.use_cassandra_stress
    def test_sla_usage(self, entity_class, entity_name: str, entity_pass: str, entity_login: bool):
        """
        1. Create SL with 100 shares.
        2. Create an entity (Role / User) with authentication
        settings.
        3. Attach SL to entity.
        run quick c-s to validate the role/user can be used
        """
        session = self.prepare()

        sl = ServiceLevel(session=session, name="sla1", shares=100)
        entity_kwargs = {
            "session": session,
            "name": entity_name,
            "password": entity_pass,
            "superuser": True,
        }

        if entity_login:
            entity_kwargs["login"] = entity_login

        entity = entity_class(**entity_kwargs)
        self.create_entity_with_service_level(entity=entity, service_level=sl)

        self.validate_sl_list(session=session, expected_service_levels=[sl])
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[sl])

        node1 = self.cluster.nodelist()[0]
        user = "cassandra"
        password = "cassandra"
        cmd = "write cl=ALL n=10 -mode cql3 native user={user} password={password}"

        node1.stress(cmd.format(user=user, password=password).split())
        node1.stress(cmd.format(user=entity.name, password=entity.password).split())

    @pytest.mark.require("scylladb/scylla-enterprise#2163")
    def test_sla_no_shares(self):
        """
        1. Create SL without specifying the number of shares.
        2. Create a Role.
        3. Attach the SL to the Role.
        4. Validate that the SL attached to the Role has the default
        value for service shares (i.e. 1000).
        """
        session = self.prepare()

        expected_sl = ServiceLevel(session=session, name="sla1")
        actual_sl = ServiceLevel(session=session, name="sla1", shares=None)
        role = Role(session=session, name="role1")
        self.create_entity_with_service_level(entity=role, service_level=actual_sl)

        self.validate_sl_list(session=session, expected_service_levels=[expected_sl])
        self.validate_attached_slas_list(session=session, entity=role, expected_service_levels=[expected_sl])

    @pytest.mark.parametrize(argnames=["entity_class", "entity_name"], argvalues=[[Role, "test_role"], [User, "test_user"]], ids=["with_role", "with_user"])
    def test_replace_sla(self, entity_class, entity_name: str):
        """
        1. Create 2 SLs with different number of service shares.
        2. Create a test entity (Role / User).
        2. Attach first SL to the test Role.
        3. Validate that both SLs exist and only the first is
        attached to the test entity.
        4. Replace the attached SL by:
        - detaching the attached SL from the entity
        - attaching the second SL to the entity
        5.Validate that both SLs exist and only the second one is
        attached to the test entity.
        """
        session = self.prepare()
        sl_50 = ServiceLevel(session=session, name="sla50", shares=50).create()
        sl_300 = ServiceLevel(session=session, name="sla300", shares=300).create()
        sls = [sl_50, sl_300]

        entity = entity_class(session=session, name=entity_name).create()

        entity.attach_service_level(service_level=sl_50)

        self.validate_sl_list(session=session, expected_service_levels=sls)
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[sl_50])

        entity.attach_another_sla_to_role(service_level=sl_300)

        self.validate_sl_list(session=session, expected_service_levels=sls)
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[sl_300])

    @pytest.mark.require("scylladb/scylla-enterprise#2163")
    @pytest.mark.parametrize(argnames=["entity_class", "entity_name"], argvalues=[[Role, "test_role"], [User, "test_user"]], ids=["with_role", "with_user"])
    def test_update_assigned_sla_service_shares(self, entity_class, entity_name: str):
        """
        1. Create entity.
        2. Create SL with default service shares value.
        3. Validate that the attached SL has the default service shares value.
        3. Update the attached SL with a different shares value.
        4. Validate that the attached SL has the updated service shares value.
        """
        session = self.prepare()
        default_sl = ServiceLevel(session=session, name="test_sla")
        sl = ServiceLevel(session=session, name="test_sla", shares=None)
        entity = entity_class(session=session, name=entity_name)
        self.create_entity_with_service_level(entity=entity_class(session=session, name=entity_name), service_level=sl)

        self.validate_sl_list(session=session, expected_service_levels=[default_sl])
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[default_sl])

        sl.alter(new_shares=500)

        self.validate_sl_list(session=session, expected_service_levels=[sl])
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[sl])

    @pytest.mark.parametrize(argnames=["entity_class", "entity_name"], argvalues=[[Role, "test_role"], [User, "test_user"]], ids=["with_role", "with_user"])
    def test_attach_2_slas_to_role(self, entity_class, entity_name: str):
        """
        1. Create SL with 100 shares.
        2. Create entity (User / Role) with the SL created in (1).
        3. Validate that the SL created in (1) exists and is attached
        to the entity.
        4. Create another SL with 200 shares.
        5. Attach the SL created in (4) to the entity.
        6. Validate that the SL created in (4) exists and is attached
        to the entity and that the SL created in (1) is no longer
        attached to the entity.
        """
        session = self.prepare()

        sl100 = ServiceLevel(session=session, name="sla1", shares=100)
        entity = entity_class(session=session, name=entity_name)
        self.create_entity_with_service_level(entity=entity, service_level=sl100)

        self.validate_sl_list(session=session, expected_service_levels=[sl100])
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[sl100])

        sl200 = ServiceLevel(session=session, name="sla2", shares=200).create()
        entity.attach_service_level(service_level=sl200)

        self.validate_sl_list(session=session, expected_service_levels=[sl100, sl200])
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[sl200])

    def test_chaos_sl_creation_altering_deletion(self):  # noqa: PLR0915
        """
        chaos test: create/alter service levels in the parallel threads
        """

        def create_sl_in_loop(session, random_range, name_iter: Iterator[int], service_level_name: str | None = None, timeout: int = 300):  # noqa: PLR0912
            end_time = time.time() + timeout
            i = 0
            while time.time() < end_time:
                sl_name = f"sl{service_level_name or next(name_iter)}"
                i += 1
                sl = None
                try:
                    logger.info(f"Iteration {i}")
                    # Prevent service level creation in exactly same time from all threads
                    time.sleep(random.randint(1, 10))
                    new_sl = ServiceLevel(session=session, name=sl_name, shares=random.randint(*random_range))
                    # Under this concurrency a group0 write can retry past the client timeout, see SCYLLADB-4652.
                    sl = retry_till_success(new_sl.create, timeout=120, bypassed_exception=OperationTimedOut)

                    # Wait for a service level is propagated to all nodes
                    read_barrier(session)

                    retry_till_success(sl.alter, new_shares=random.randint(*random_range), timeout=120, bypassed_exception=OperationTimedOut)
                except NoHostAvailable as exc:
                    if "no more scheduling groups exist" in str(exc.errors):
                        logger.warning(str(exc))
                    elif "concurrent modification" in str(exc.errors):
                        logger.warning(str(exc))
                    else:
                        raise
                except InvalidRequest as exc:
                    if f"The service level '{sl_name}' doesn't exist" in str(exc):
                        logger.warning(f"It is possible that service level '{sl_name}' has been removed by another thread. Error: {exc!s}")
                    elif "no more scheduling groups exist" in str(exc):
                        logger.warning(str(exc))
                    else:
                        logger.error(f"Exception {exc!s}, service level: {sl.name if sl else 'None'}.")
                        raise
                except Exception as exc:
                    logger.error(f"Exception {exc!s}, service level: {sl.name if sl else 'None'}. Exception type: {type(exc)}. Is it NoHostAvailable: {type(exc) is NoHostAvailable}.")
                    raise
                finally:
                    if sl:
                        try:
                            retry_till_success(sl.drop, timeout=120, bypassed_exception=OperationTimedOut)
                        except NoHostAvailable as exc:
                            if "concurrent modification" in str(exc.errors):
                                logger.warning(str(exc))
                            else:
                                raise
                        except InvalidRequest as exc:
                            if f"The service level '{sl_name}' doesn't exist" in str(exc):
                                logger.warning(f"It is possible that service level '{sl_name}' has been removed by another thread. Error: {exc!s}")
                            else:
                                raise

        self.prepare(nodes=4, smp=4)
        session_n1 = self.exclusive_cql_connection(self.cluster.nodelist()[0], user="cassandra", password="cassandra")
        session_n2 = self.exclusive_cql_connection(self.cluster.nodelist()[1], user="cassandra", password="cassandra")
        session_n3 = self.exclusive_cql_connection(self.cluster.nodelist()[2], user="cassandra", password="cassandra")
        session_n4 = self.exclusive_cql_connection(self.cluster.nodelist()[3], user="cassandra", password="cassandra")

        threads = []
        with ThreadPoolExecutor(max_workers=10) as tp:
            name_iter = itertools.count(start=3000)
            # Create and drop same named service level
            threads.append(tp.submit(create_sl_in_loop, session=session_n4, random_range=(10, 1000), name_iter=name_iter, service_level_name="_reuse_1"))
            # Create and drop same named service level
            threads.append(tp.submit(create_sl_in_loop, session=session_n3, random_range=(10, 1000), name_iter=name_iter, service_level_name="_reuse_2"))
            threads.append(tp.submit(create_sl_in_loop, session=session_n2, random_range=(1, 20), name_iter=name_iter))
            threads.append(tp.submit(create_sl_in_loop, session=session_n1, random_range=(1, 20), name_iter=name_iter))
            threads.append(tp.submit(create_sl_in_loop, session=session_n3, random_range=(50, 500), name_iter=name_iter))
            threads.append(tp.submit(create_sl_in_loop, session=session_n4, random_range=(500, 1000), name_iter=name_iter))
            threads.append(tp.submit(create_sl_in_loop, session=session_n1, random_range=(1, 20), name_iter=name_iter))
            threads.append(tp.submit(create_sl_in_loop, session=session_n3, random_range=(50, 500), name_iter=name_iter))
            threads.append(tp.submit(create_sl_in_loop, session=session_n4, random_range=(500, 1000), name_iter=name_iter))
            threads.append(tp.submit(create_sl_in_loop, session=session_n4, random_range=(500, 1000), name_iter=name_iter))

            for thread in threads:
                thread.result(timeout=480)

    @pytest.mark.parametrize(argnames=["entity_class", "entity_name"], argvalues=[[Role, "test_role"], [User, "test_user"]], ids=["with_role", "with_user"])
    def test_drop_sl_and_attach_new_to_role(self, entity_class, entity_name: str):
        """
        1. Create SL with 100 shares.
        2. Create entity (User / Role) with the SL created in (1).
        3. Validate that the SL created in (1) exists and is attached
        to the entity.
        4. Create another SL with 200 shares.
        5. Attach the SL created in (4) to the entity.
        6. Validate that the SL created in (4) exists and is attached
        to the entity and that the SL created in (1) is no longer
        attached to the entity.
        """
        session = self.prepare()

        sl100 = ServiceLevel(session=session, name="sla1", shares=100)
        entity = entity_class(session=session, name=entity_name)
        self.create_entity_with_service_level(entity=entity, service_level=sl100)

        self.validate_sl_list(session=session, expected_service_levels=[sl100])
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[sl100])

        sl100.drop()

        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[])

        sl200 = ServiceLevel(session=session, name="sla2", shares=200).create()
        entity.attach_service_level(service_level=sl200)

        self.validate_sl_list(session=session, expected_service_levels=[sl200])
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[sl200])


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestSLANegativeTests(SLATester):
    def test_update_not_existing_sla(self):
        """
        1. Create a ServiceLevel instance (but without creating the
        SL in the db).
        2. Attempt to alter the SL.
        3. Validate that an InvalidRequest error is request with the
        expected error message.
        """
        session = self.prepare()
        sl = ServiceLevel(session=session, name="sla1")
        expected_error = rf"""The service level '{sl.name.replace('"', "")}' doesn't exist."""

        with pytest.raises(InvalidRequest, match=expected_error):
            sl.alter(new_shares=100)

    def test_create_sla_with_more_1000_shares(self):
        """
        Create SL with an invalid value of shares: 1001.
        """
        self._wrong_shares(shares=1001)

    def test_create_sla_with_0_shares(self):
        """
        Create SL with an invalid value of shares: 0.
        """
        self._wrong_shares(shares=0)

    def test_create_sla_with_negative_shares(self):
        """
        Create SL with an invalid value of shares: -1.
        """
        self._wrong_shares(shares=-1)

    def _wrong_shares(self, shares):
        session = self.prepare()
        expected_error = r"'SHARES' can only take values of 1-1000 \(given %d\)" % shares

        with pytest.raises(SyntaxException, match=expected_error):
            ServiceLevel(session=session, name="sla1", shares=shares).create()


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestSLATimeouts(SLATester):
    KEY_NUM = 1000

    @pytest.mark.parametrize(
        argnames=("duration"),
        argvalues=[
            ScyllaDuration(milliseconds=1000000),
            ScyllaDuration(hours=5),
            ScyllaDuration(hours=23, minutes=2, seconds=2, milliseconds=2),
        ],
        ids=("1000000ms", "5h", "23h2m2s2ms"),
    )
    def test_timeout_valid_values(self, duration: ScyllaDuration):
        """
        Create s Service Level with a valid timeout value.
        """
        read_query = "SELECT * FROM ks.cf"
        session = self.prepare()
        node = self.cluster.nodelist()[0]
        self.populate_data(session=session, number_of_keys=self.KEY_NUM)
        role = Role(session=session, name="test_role", password="test_role", login=True).create()
        sl = ServiceLevel(session=session, name="sl1", timeout=duration, shares=None).create()
        role.attach_service_level(sl)
        grant_select_query = f"GRANT SELECT ON KEYSPACE ks TO {role.name};"
        session.execute(grant_select_query)
        listed_sl = sl.list_service_level()

        assert sl == listed_sl

        new_session = self.patient_cql_connection(node=node, user=role.name, password=role.password)
        query_result = new_session.execute(read_query).all()
        logger.debug("Query result: %s", query_result)

        assert query_result

    @pytest.mark.require("scylladb/scylladb#10285")
    @pytest.mark.parametrize(
        argnames=("scylla_yaml_timeout", "sl_timeout", "query_timeout"),
        argvalues=[
            (100, ScyllaDuration(milliseconds=0), None),
            (None, ScyllaDuration(milliseconds=100), "0ms"),
        ],
        ids=[
            "service_level_wins_over_scylla_yaml",
            "query_timeout_wins_over_service_level_timeout",
        ],
    )
    def test_sla_timeout_priority(self, scylla_yaml_timeout, sl_timeout: ScyllaDuration, query_timeout):
        """
        Request timeouts can be set using 3 different methods:
        1. scylla.yaml config file entry
        2. Service Level definition
        3. Per-query timeout defined in the CQL statement

        In terms of prioritization:
        3 > 2 > 1

        This test checks if 2 scenarios are true:
        - 3 > 2
        - 2 > 1

        Test steps:
        1) Populate db with some data.
        2) Create a test Role.
        3) Open a new session for the test Role.
        and query the data.
        4) Change the timeout value using the given method.
        5) Open a new CQL session and attempt the same query as
        in (2).
        6) Assert that a RequestTimeout error was raised.
        """
        if query_timeout:
            read_query = f"SELECT * FROM ks.cf USING TIMEOUT {query_timeout}"
        else:
            read_query = "SELECT * FROM ks.cf"

        session = self.prepare()
        node = self.cluster.nodelist()[0]
        self.populate_data(session=session, number_of_keys=self.KEY_NUM)
        sl = ServiceLevel(session=session, name="sl1", timeout=ScyllaDuration(milliseconds=100), shares=None).create()
        role1 = Role(session=session, name="role1", password="role1", login=True).create()
        role1.attach_service_level(sl)

        grant_select_query = f"GRANT SELECT ON KEYSPACE ks TO {role1.name};"
        session.execute(grant_select_query)
        user_session = self.patient_cql_connection(self.cluster.nodelist()[0], user=role1.name, password=role1.password)

        pre_read_result = user_session.execute("SELECT * FROM ks.cf").all()
        assert len(pre_read_result) == self.KEY_NUM

        # update scylla yaml
        if scylla_yaml_timeout:
            mark = node.mark_log()
            self.cluster.stop()
            self.cluster.set_configuration_options(values={"read_request_timeout_in_ms": scylla_yaml_timeout})
            self.cluster.start(wait_other_notice=True, wait_for_binary_proto=True)
            node.watch_log_for(exprs=["cql_server_controller - Starting listening for CQL clients"], from_mark=mark)

        if sl_timeout:
            new_session = self.patient_cql_connection(node=node, user="cassandra", password="cassandra")
            sl.session = new_session
            sl.alter(new_timeout=sl_timeout)

        with pytest.raises(ReadTimeout):
            new_user_session = self.patient_cql_connection(node=node, user=role1.name, password=role1.password)
            read_result = new_user_session.execute(read_query)
            logger.debug("Read result: %s", len(read_result.all()))


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestSLTimeoutsNegative(SLATester):
    @pytest.mark.parametrize(
        argnames=["timeout", "expected_exception_msg"],
        argvalues=[
            [ScyllaDuration(days=1), "Timeout values cannot be expressed in days/months"],
            [ScyllaDuration(months=2), "Timeout values cannot be expressed in days/months"],
            [ScyllaDuration(nanoseconds=1000), "Timeout values must be expressed in millisecond granularity"],
            [ScyllaDuration(milliseconds=-1200), "Timeout values must be nonnegative"],
        ],
        ids=[
            "using_days_as_values",
            "using_months_as_values",
            "nanosecond_value",
            "negative_timeout_value",
        ],
    )
    def test_invalid_timeout_values(self, timeout: ScyllaDuration, expected_exception_msg: str):
        session = self.prepare()

        with pytest.raises(InvalidRequest) as exc:
            ServiceLevel(session=session, name="sl1", timeout=timeout, shares=None).create()

        assert exc.match(f".*{expected_exception_msg}.*")


@pytest.mark.dtest_full
class TestSLAConfig(SLATester):
    @staticmethod
    def connection_in_scheduling_group(role_session, node, role_name, sg_name):
        role_session.execute("select * from ks.cf")
        response = requests.get(f"http://{get_ip_from_node(node=node)}:{node.api_port}/service_levels/count_connections")
        sg_connections_map = response.json()
        # Sample result of /service_levels/count_connections:
        # {'sl:test_sl': {'test_role': 2}, 'sl:default': {'cassandra': 1}}
        logger.debug(f"scheduling group connections map: {sg_connections_map}")
        return role_name in sg_connections_map.get(sg_name, {})

    @staticmethod
    def query_runs_in_scheduling_group(session, sg_name):
        result = session.execute("select * from ks.cf", trace=True)
        trace = result.get_query_trace()
        semaphore_events = 0
        for e in trace.events:
            logger.debug(f"trace message: {e}")

            # The connection can still be running under sl:driver until the switch to the
            # role's service level fully propagates, so retry until every event runs under it.
            if sg_name not in e.thread_name:
                return False

            # Verify reader concurrency semaphore name for the events that report one.
            if "[reader concurrency semaphore" in e.description:
                semaphore_events += 1
                assert f"[reader concurrency semaphore {sg_name}]" in e.description, f"Query on {e.source} was not executed with semaphore for {sg_name} scheduling group"

        # A read query must go through the reader concurrency semaphore, so keep retrying
        # if the trace did not capture it yet instead of passing without verifying it.
        return semaphore_events > 0

    # This test validates if service levels/scheduling groups configuration works properly
    # Checks:
    #   - connections' scheduling group (using '/service_levels/count_connections' endpoint)
    #   - scheduling group in `thread_name` field in tracing logs
    #   - semaphore (semaphore's name from tracing logs)
    def test_sla_configuration(self):
        nodes = 3
        session = self.prepare(nodes=nodes, additional_config={"service_levels_interval_ms": 500})
        self.populate_data(session=session, number_of_keys=10)

        role = Role(session=session, name="test_role", password="test_role", login=True, superuser=True)
        sl = ServiceLevel(session=session, name="test_sl", shares=500)
        self.create_entity_with_service_level(entity=role, service_level=sl)

        # Wait for service levels to be propageted
        time.sleep(1)

        role_sessions = []
        for node in self.cluster.nodelist():
            role_sessions.append(self.patient_exclusive_cql_connection(node, user=role.name, password=role.password))

        # A new connection starts in the sl:driver scheduling group and only switches to the
        # role's service level once the server processes a user request on it (a query touching
        # a non-system keyspace, scylladb/scylladb#30277). Run a query on each session and wait
        # for its connection to be classified under the role's scheduling group on every node.
        for node, role_session in zip(self.cluster.nodelist(), role_sessions):
            wait_for(
                lambda: self.connection_in_scheduling_group(role_session, node, role.name, sl.sg_name),
                timeout=30,
                text=f"Role {role.name} connections to switch to {sl.sg_name} scheduling group on node {node.name}",
            )

        # Execute a query with tracing from every node and verify that
        # - the query is run with correct scheduling group (this information can be taken from field 'thread_name' in tracing events)
        # - the query uses correct semaphore for its scheduling group (name of reader concurrency semaphore is in a trace event description)
        # The count_connections endpoint reports the connection's scheduling group before queries
        # actually start running under it: the group is switched only after the triggering request
        # finishes, and it is tracked per shard, so a query may still run under sl:driver even after
        # the endpoint reports the role's group. Retry the traced query until it runs under it.
        for session in role_sessions:
            wait_for(
                lambda: self.query_runs_in_scheduling_group(session, sl.sg_name),
                timeout=30,
                text=f"Query to run under {sl.sg_name} scheduling group",
            )
