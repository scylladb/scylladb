import logging
from dataclasses import dataclass, field, fields
from typing import Union

from cassandra.cluster import Session

from tools.units import ScyllaDuration

logger = logging.getLogger(__name__)


@dataclass
class ServiceLevelAttributes:
    shares: int = None
    timeout: ScyllaDuration = None
    workload_type: str = None
    query_string: str = field(init=False, repr=False)

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if key != "query_string":
            self._generate_query_string()

    def __post_init__(self):
        self._generate_query_string()

    def _generate_query_string(self):
        attr_strings = []

        for item in fields(self):
            value = getattr(self, item.name) if item.repr else None
            if value is not None:
                if item.type is ScyllaDuration:
                    attr_strings.append(f" AND {item.name} = '{value.to_query_string()}'")
                elif item.type is str:
                    attr_strings.append(f" AND {item.name} = '{value}'")
                else:
                    attr_strings.append(f" AND {item.name} = {value}")
        if attr_strings:
            attr_strings[0] = attr_strings[0].replace(" AND", " WITH")
        else:
            self.query_string = ""
            return

        if len(attr_strings) > 1:
            self.query_string = "".join(attr_strings)
        else:
            self.query_string = attr_strings[0]


class ServiceLevel:
    # The class provide interface to manage SERVICE LEVEL
    def __init__(  # noqa: PLR0913
        self,
        session,
        name: str,
        shares: int = 1000,
        timeout: ScyllaDuration = None,
        workload_type: str | None = None,
        verbose=True,
    ):
        self.session = session
        self._name = f'"{name}"'
        self.verbose = verbose
        self._created = False
        self._sl_attributes = ServiceLevelAttributes(shares=shares, timeout=timeout, workload_type=workload_type)
        self._sg_name = f"sl:{name}"

    def __hash__(self):
        return hash((self.name, self.shares, self.timeout, self.workload_type))

    @classmethod
    def from_dict(cls, session: Session, sl_dict):
        return ServiceLevel(session=session, name=sl_dict["name"], shares=sl_dict["shares"], timeout=sl_dict["timeout"], workload_type=sl_dict["workload_type"])

    @classmethod
    def from_row(cls, session: Session, row):
        row_dict = row._asdict()
        row_dict["name"] = row_dict.pop("service_level")
        return ServiceLevel.from_dict(session=session, sl_dict=row_dict)

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name) -> None:
        self._name = name
        self._sg_name = f"sl:{self._name[1:-1]}"

    @property
    def sg_name(self) -> str:
        return self._sg_name

    @property
    def shares(self) -> int:
        return self._sl_attributes.shares

    @shares.setter
    def shares(self, service_level_shares) -> None:
        self._sl_attributes.shares = service_level_shares

    @property
    def created(self) -> bool:
        return self.created

    @created.setter
    def created(self, created: bool) -> None:
        self._created = created

    @property
    def timeout(self) -> ScyllaDuration:
        return self._sl_attributes.timeout

    @timeout.setter
    def timeout(self, timeout: int) -> None:
        self._sl_attributes.timeout = timeout

    @property
    def workload_type(self) -> str:
        return self._sl_attributes.workload_type

    @workload_type.setter
    def workload_type(self, workload_type: str) -> None:
        self._sl_attributes.workload_type = workload_type

    def __eq__(self, other) -> bool:
        own_timeout_duration = self.timeout.get_duration() if isinstance(self.timeout, ScyllaDuration) else None
        other_timeout_duration = other.timeout.get_duration() if isinstance(other.timeout, ScyllaDuration) else None

        own_shares = self.shares or 1000
        other_shares = other.shares or 1000

        return all((own_shares == other_shares, own_timeout_duration == other_timeout_duration, self.workload_type == other.workload_type))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}: name: {self.name}, attributes: {self._sl_attributes}"

    def create(self, if_not_exists=True) -> ServiceLevel:
        query = f"CREATE SERVICE_LEVEL {'IF NOT EXISTS' if if_not_exists else ''} {self.name}{self._sl_attributes.query_string}"
        if self.verbose:
            logger.debug("Create service level query: %s", query)
        self.session.execute(query)
        logger.debug("Service level %s has been created", self.name)
        self.created = True
        return self

    def alter(self, new_shares: int | None = None, new_timeout: ScyllaDuration = None, new_workload_type: str | None = None) -> None:
        sla = ServiceLevelAttributes(shares=new_shares, timeout=new_timeout, workload_type=new_workload_type)
        query = f"ALTER SERVICE_LEVEL {self.name} {sla.query_string}"
        if self.verbose:
            logger.debug("Change service level query: %s", query)
        self.session.execute(query)
        logger.debug("Service level %s has been altered", self.name)
        self.shares = new_shares

    def drop(self, if_exists=True) -> None:
        query = f"DROP SERVICE_LEVEL {'IF EXISTS' if if_exists else ''} {self.name}"
        if self.verbose:
            logger.debug("Drop service level query: %s", query)
        self.session.execute(query)
        logger.debug("Service level %s has been dropped", self.name)
        self.created = False

    def list_service_level(self) -> ServiceLevel | list | None:
        query = f"LIST SERVICE_LEVEL {self.name}"
        if self.verbose:
            logger.debug("List service level query: %s", query)
        res = self.session.execute(query).all()
        assert len(res) <= 1, "Received %s service levels when expecting to receive only 1" % len(res)

        if len(res) == 0:
            return []

        result_dict = res[0]._asdict()
        parsed_result_dict = self._parse_row_dict(result_dict)

        return ServiceLevel.from_dict(session=self.session, sl_dict=parsed_result_dict)

    def list_all_service_levels(self, skip_driver) -> list[ServiceLevel]:
        query = "LIST ALL SERVICE_LEVELS"
        if self.verbose:
            logger.debug("List all service levels query: %s", query)
        res_list = self.session.execute(query).all()
        output = []

        for res in res_list:
            result_dict = res._asdict()
            parsed_result_dict = self._parse_row_dict(result_dict)
            if skip_driver and parsed_result_dict["name"] == "driver":
                continue
            output.append(ServiceLevel.from_dict(session=self.session, sl_dict=parsed_result_dict))
        return output

    def _parse_row_dict(self, row_dict) -> dict:
        row_dict["name"] = row_dict.pop("service_level")
        timeout_value = row_dict["timeout"]
        if timeout_value and timeout_value != "null":
            row_dict["timeout"] = ScyllaDuration.from_duration(timeout_value)

        row_dict["shares"] = row_dict.get("shares")

        return row_dict


class UserRoleBase:
    # Base class for ROLES and USERS
    AUTHENTICATION_ENTITY = ""

    def __init__(self, session, name, password=None, superuser=None, verbose=False, **kwargs):
        self._name = name
        self.password = password
        self.session = session
        self.superuser = superuser
        self.verbose = verbose
        self._attached_service_level = None

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name) -> None:
        self._name = name

    @property
    def attached_service_level(self):
        return self._attached_service_level

    @property
    def attached_service_level_name(self):
        return self._attached_service_level.name

    def attach_service_level(self, service_level: ServiceLevel) -> None:
        query = f"ATTACH SERVICE_LEVEL {service_level.name} TO {self.name}"
        if self.verbose:
            logger.debug("Attach service level query: %s", query)
        self.session.execute(query)
        logger.debug(f"Service level {service_level.name} has been attached to {self.name} role")
        self._attached_service_level = service_level

    def detach_service_level(self) -> None:
        query = f"DETACH SERVICE_LEVEL FROM {self.name}"
        if self.verbose:
            logger.debug("Detach service level query: %s", query)
        self.session.execute(query)
        logger.debug("The service level has been detached from %s role", self.name)
        self._attached_service_level = None

    def grant_me_to(self, grant_to: UserRoleBase) -> None:
        query = f"GRANT {self.name} to {grant_to.name}"
        if self.verbose:
            logger.debug("GRANT role query: %s", query)
        self.session.execute(query)
        logger.debug(f"Role {self.name} has been granted to {grant_to.name}")

    def revoke_me_from(self, revoke_from: UserRoleBase) -> None:
        query = f"REVOKE ROLE {self.name} FROM {revoke_from.name}"
        if self.verbose:
            logger.debug("REVOKE role query: %s", query)
        self.session.execute(query)
        logger.debug(f"Role {self.name} has been revoked from {revoke_from.name}")

    def attach_another_sla_to_role(self, service_level) -> None:
        self.detach_service_level()
        self.attach_service_level(service_level=service_level)

    def list_user_role_attached_service_levels(self) -> list:
        query = f"LIST ATTACHED SERVICE_LEVEL OF {self.name}"
        if self.verbose:
            logger.debug("List attached service level(s) query: %s", query)
        return self.session.execute(query).all()

    def list_all_attached_service_levels(self) -> list:
        query = "LIST ATTACHED ALL SERVICE_LEVELS"
        if self.verbose:
            logger.debug("List attached service level(s) query: %s", query)
        return self.session.execute(query).all()

    def drop(self, if_exists=True) -> None:
        query = f"DROP {self.AUTHENTICATION_ENTITY} {'IF EXISTS' if if_exists else ''} {self.name}"
        if self.verbose:
            logger.debug(f"Drop {self.AUTHENTICATION_ENTITY} query: {query}")

        self.session.execute(query)
        logger.debug(f"{self.AUTHENTICATION_ENTITY} {self.name} has been dropped")


class Role(UserRoleBase):
    # The class provide interface to manage ROLES
    AUTHENTICATION_ENTITY = "ROLE"

    def __init__(  # noqa: PLR0913
        self,
        session,
        name,
        password=None,
        login=False,
        superuser=False,
        options_dict=None,
        verbose=True,
    ):
        super().__init__(session, name, password, superuser, verbose)
        self.login = login
        self.options_dict = options_dict

    def create(self) -> Role:
        # Example: CREATE ROLE bob WITH PASSWORD = 'password_b'AND LOGIN = true AND SUPERUSER = true;
        # Example: CREATE ROLE carlos WITH OPTIONS = {'custom_option1': 'option1_value', 'custom_option2': 99};
        role_options = {}
        for opt in ["password", "login", "superuser", "options_dict"]:
            if hasattr(self, opt):
                value = getattr(self, opt)
                if value:
                    role_options[opt.replace("_dict", "")] = f"'{value}'" if opt == "password" else value
        role_options_str = " AND ".join([f"{opt} = {val}" for opt, val in role_options.items()])
        if role_options_str:
            role_options_str = f" WITH {role_options_str}"

        query = f"CREATE ROLE {self.name}{role_options_str}"
        if self.verbose:
            logger.debug("CREATE role query: %s", query)
        self.session.execute(query)
        logger.debug("Role %s has been created", self.name)
        return self


class User(UserRoleBase):
    # The class provide interface to manage USERS
    AUTHENTICATION_ENTITY = "USER"

    def __init__(self, session, name, password=None, superuser=None, verbose=True):
        super().__init__(session, name, password, superuser, verbose)

    def create(self) -> User:
        password = f" PASSWORD '{self.password if self.password else ''}'"
        superuser = "" if self.superuser is None else " SUPERUSER" if self.superuser else " NOSUPERUSER"
        user_options_str = f"{password}{superuser}"
        if user_options_str:
            user_options_str = f" WITH {user_options_str}"

        query = f"CREATE USER {self.name}{user_options_str}"
        if self.verbose:
            logger.debug("Create user query: %s", query)

        self.session.execute(query)
        logger.debug("User %s has been created", self.name)
        return self
