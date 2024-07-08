from enum import StrEnum


class Command(StrEnum):
    LIST_MODULES = "list_modules",
    LIST_TASKS = "list_tasks",
    WATCH_TASKS = "watch_tasks"
    GET_STATUS = "get_status",
    WAIT = "wait",
    GET_TREE = "get_tree",
    ABORT = "abort",
    SET_TTL = "set_ttl"
