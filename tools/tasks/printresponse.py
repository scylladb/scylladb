from command import Command
from requests import Response
from tabulate import tabulate


def print_response(res: Response, cmd: Command):
    if res.status_code == 200:
        print(res.status_code)
        data = res.json()
        match cmd:
            case Command.LIST_MODULES:
                for module in data:
                    print(module)
            case Command.LIST_TASKS | Command.WATCH_TASKS:
                print_table(data)
            case Command.GET_STATUS | Command.WAIT:
                print_json(data)
            case Command.GET_TREE:
                print_table(data)
            case Command.ABORT:
                print("Task aborted")
            case Command.SET_TTL:
                print(data)
            
    else:
        print(res.status_code, " ", res.reason)
        print_json(res.json())

def print_json(data):
    for item in data.items():
        print(item[0], ": ", item[1])

def print_table(data):
    if not len(data):
        return

    headers = make_headers(data[0])
    table = [headers]
    for row in data:
        table.append(make_table_row(row, headers))

    print(tabulate(table, headers='firstrow', tablefmt='fancy_grid'))

def make_headers(row) -> list[str]:
    return [item[0] for item in row.items()]

def make_table_row(row, headers: list[str]):
    return [row[h] for h in headers]
