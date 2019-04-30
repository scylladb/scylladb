# Tests for the CRUD item operations: PutItem, GetItem, UpdateItem, DeleteItem


# Basic test for creating a new item with a random name, and reading it back.
def test_basic_put_and_get(test_table):
    # TODO: random key
    response = test_table.put_item(
        Item={'p': 'hello', 'c': 'hi'})
    print(response)
