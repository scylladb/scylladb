# Running tests with pytest

To run test with pytest execute 
```bash
pytest test/ldap
```
To execute only one file, provide the path filename
```bash
pytest test/unit/role_manager_test.cc
```
Since it's a normal path, autocompletion works in the terminal out of the box.

To provide a specific mode, use the next parameter `--mode dev`,
if parameter isn't provided pytest tries to use `ninja mode_list` to find out the compiled modes.

Parallel execution is controlled by `pytest-xdist` and the parameter `-n auto`.
This command starts tests with the number of workers equal to CPU cores.
