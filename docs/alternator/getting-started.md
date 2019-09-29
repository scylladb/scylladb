# Getting Started With ScyllaDB Alternator
---
## Installing Scylla
Before you can start using ScyllaDB Alternator, you will have to have an up
and running scylla cluster configured to expose the alternator port.
This section will guide you through the steps for setting up the cluster:
### Get Scylla with alternator support from a docker:
1. Alternator's image name is: `scylladb/scylla-nightly:alternator`
2. follow the steps in the [Scylla official download web page](https://www.scylladb.com/download/open-source/#docker)
   add to every "docker run" command: `-p 8000:8000` before the image name and `--alternator-port=8000` at the end.
   ie: (image name in this example is scylladb/scylla)  `docker run --name scylla -d scylladb/scylla-nightly:alternator` becomes
   `docker run --name scylla -d -p 8000:8000 scylladb/scylla-nightly:alternator --alternator-port=8000`

## Testing Scylla's DynamoDB API support:
### Running AWS Tic Tac Toe demo app to test the cluster:
1. Follow the instructions on the [AWS github page](https://github.com/awsdocs/amazon-dynamodb-developer-guide/blob/master/doc_source/TicTacToe.Phase1.md)
2. Enjoy your tic-tac-toe game :-)

### Setting up the python environment
Run the following commands on your machine, this will install boto3 python library
which also contains drivers for DynamoDB:

```
sudo pip install --upgrade boto3
```
### Runnning some simple scripts:
The following is a 3 scripts test that creates a table named _usertable_ writes the
famous hello world record to it, and then, reads it back.

1. Put the following **create table** example script in a python file and run it (changing local host
to the address of your docker node if you are using docker):
```python
import boto3
dynamodb = boto3.resource('dynamodb',endpoint_url='http://localhost:8000',
                  region_name='None', aws_access_key_id='None', aws_secret_access_key='None')

dynamodb.create_table(
    AttributeDefinitions=[
    {
        'AttributeName': 'key',
        'AttributeType': 'S'
    },
    ],
    BillingMode='PAY_PER_REQUEST',
    TableName='usertable',
    KeySchema=[
    {
        'AttributeName': 'key',
        'KeyType': 'HASH'
    },
    ])
```

2. Put the following **write** example script in a python file and run it (changing local host
to the address of your docker node if you are using docker):

```python
import boto3
dynamodb = boto3.resource('dynamodb',endpoint_url='http://localhost:8000',
                  region_name='None', aws_access_key_id='None', aws_secret_access_key='None')

dynamodb.batch_write_item(RequestItems={
    'usertable': [
        {
             'PutRequest': {
                 'Item': {
                     'key': 'test', 'x' : {'hello': 'world'}
                 }
             },
        }
    ]
})
```

3. Put the following **read** example script in a python file and run it (changing local host
to the address of your docker node if you are using docker):
```python
import boto3
dynamodb = boto3.resource('dynamodb',endpoint_url='http://localhost:8000',
                  region_name='None', aws_access_key_id='None', aws_secret_access_key='None')

print(dynamodb.batch_get_item(RequestItems={
    'usertable' : { 'Keys': [{ 'key': 'test' }] }
}))
```

You should see the record you inserted in step 2 along with some http info printed to screen.
