# Getting Started With ScyllaDB Alternator

## Installing ScyllaDB
Before you can start using ScyllaDB Alternator, you will have to have an up
and running a ScyllaDB cluster configured to expose the Alternator port.
This section will guide you through the steps for setting up the cluster:
### Get ScyllaDB with Alternator support from a docker:
1. Download the latest stable ScyllaDB image for Docker, by running
   `docker pull scylladb/scylla:latest`
2. To run this Docker image, follow the instructions in
   <https://hub.docker.com/r/scylladb/scylla/>, but add to every `docker run`
   command a `-p 8000:8000` before the image name and
   `--alternator-port=8000 --alternator-write-isolation=always` at the end.
   The "alternator-port" option specifies on which port Scylla will listen for
   the (unencrypted) DynamoDB API, and the "alternator-write-isolation" chooses
   whether or not Alternator will use LWT for every write.
   For example,
   `docker run --name scylla -d -p 8000:8000 scylladb/scylla:latest --alternator-port=8000 --alternator-write-isolation=always`.
   The `--alternator-https-port=...` option can also be used to enable
   Alternator on an encrypted (HTTPS) port. Note that in this case, the files
   `/etc/scylla/scylla.crt` and `/etc/scylla/scylla.key` must be inserted into
   the image, containing the SSL certificate and key to use.

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
### Running some simple scripts:
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
