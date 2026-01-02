# Network optimization in Alternator

In some deployments, the network between the application and the ScyllaDB
cluster is limited in bandwidth, or is expensive. This document surveys
some mechanisms that can be used to reduce this network traffic.

## Compression of requests

When the application sends a large request - notably a `PutItem` or
`BatchWriteItem` - we can reduce network usage by compressing the content
of this request.

Alternator's request protocol (the DynamoDB API) is based on HTTP or HTTPS,
so can be compressed using the standard HTTP compression support.
The standard HTTP 1.1 mechanism for compressing a request is sending the
header `Content-Encoding: gzip` and compressing the request's body using
the gzip algorithm. Alternator currently supports two standard compression
types (see RFC 9110): `gzip` and `deflate`. Other compression types such
as `zstd` (see RFC 8878) are not yet supported.

Note that HTTP's compression only compresses the request's _body_ - not the
request _headers_ - so it is beneficial to avoid sending unnecessary headers
in the request, as they will not be compressed. See more on this below.

To use compressed requests, the client library (SDK) used by the application
should be configured to actually compress requests. Amazon's AWS SDKs support
this feature in some languages, but not in others, and may automatically
compress requests longer than a certain size. Check their website
<https://docs.aws.amazon.com/sdkref/latest/guide/feature-compression.html>
for more details for the specific SDK you are using. ScyllaDB also publishes
extensions for these SDKs which may have better support for compressed
requests (and other features mentioned in this document), so again please
consult the documentation of the specific SDK that you are using.

## Compression of responses

Some types of requests, notably `GetItem` and `BatchGetItem`, can have small
requests but large responses, so it can be beneficial to compress those
responses. The standard HTTP mechanism for doing this is that the client
provides a header like `Accept-Encoding: gzip` in the request, signalling
that it is ready to accept a gzip'ed response. Alternator is then free
to decide if it wants to compress the response - or not. If it does
compress the response body, it sets a header `Content-Encoding: gzip`
in the response.

Currently, Alternator supports response compression with either `gzip`
or `deflate` encodings. If the client requests response compression
(via the `Accept-Encoding` header), then by default Alternator compresses
responses over 4 KB in length (leaving smaller responses uncompressed),
and uses compression level 6 (where 1 is the fastest, 9 is best compression).
These defaults can be changed with the configuration options
`alternator_response_compression_threshold_in_bytes` and
`alternator_response_gzip_compression_level`, respectively.

To use compressed responses, the client library (SDK) used by application
should be configured to send an `Accept-Encoding: gzip` header and to
understand the potentially-compressed response. Although DynamoDB does
support compressed responses, it is not clear if any of Amazon's AWS SDKs can
use it. ScyllaDB publishes extensions for these SDKs which may have better
support for compressed responses (and other features mentioned in this
document), so please consult the documentation of the specific SDK that
you are using to check if it can make use of the response compression
feature that the server supports.

## Header reduction

As explained above, the HTTP headers cannot be compressed, so both client
(SDK) and server (Alternator) should avoid sending unnecessary headers in
their requests and responses.

The Alternator server sends headers like the following:
```
Content-Length: 2
Content-Type: application/x-amz-json-1.0
Date: Tue, 30 Dec 2025 20:00:01 GMT
Server: Seastar httpd
```

This is a bit over 100 bytes. Most of it is necessary, but the `Date`
and `Server` headers are not strictly necessary and a future version of
Alternator will most likely make them optional (or remove them altogether).

The request headers add significantly larger overhead, and AWS SDKs add
even more than necessary. Here is an example:
```
Content-Length: 300
amz-sdk-request: attempt=1
amz-sdk-invocation-id: caf3eb0e-8138-4cbd-adff-44ac357de04e
X-Amz-Date: 20251230T200829Z
host: 127.15.196.80:8000
User-Agent: Boto3/1.42.12 md/Botocore#1.42.12 md/awscrt#0.27.2 ua/2.1 os/linux#6.17.12-300.fc43.x86_64 md/arch#x86_64 lang/python#3.14.2 md/pyimpl#CPython m/Z,N,e,D,P,b cfg/retry-mode#legacy Botocore/1.42.12 Resource
Content-Type: application/x-amz-json-1.0
Authorization: AWS4-HMAC-SHA256 Credential=cassandra/20251230/us-east-1/dynamodb/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=34100a8cd044ef131c1ae025c91c6fc3468507c28449615cdb2edb4d82298be0"
X-Amz-Target: DynamoDB_20120810.CreateTable
Accept-Encoding: identity
```

There is a lot of waste here: Some headers like `amz-sdk-invocation-id` are
not needed at all. Others like `User-Agent` are useful for debugging but the
200-byte string sent by boto3 (AWS's Python SDK) on every request is clearly
excessive. In AWS's SDKs the user cannot make the request headers shorter,
but we plan to provide such a feature in ScyllaDB's extensions to AWS's SDKs.

Note that the request signing protocol used by Alternator and DynamoDB,
known as AWS Signature Version 4, needs the headers `x-amz-date` and
`Authorization` - which together use (as can be seen in the above example)
more than 230 bytes in each and every request. We could save these 230 bytes
by using SSL - instead of AWS Signature Version 4 - for authentication.
This idea is not yet implemented however - it is not supported by Alternator,
DynamoDB, or any of the client SDKs.

## Rack-aware request routing

In some deployments, the network between the client and the ScyllaDB server
is only expensive if the communication crosses **rack** boundaries.
A "rack" is a ScyllaDB term roughly equivalent to AWS's "availability zone".

Typically, a ScyllaDB cluster is divided to three "racks" and each piece of
data is replicated once on each rack. Each instance of the client application
also lives in one of these racks. In many deployments, communication inside
the rack is free but communicating to a different rack costs extra. It is
therefore important that the client be aware of which rack it is running
on, and send the request to a ScyllaDB node on the same rack - and not
choose one of the three replicas as random. This is known as "rack-aware
request routing", and should be enabled in the ScyllaDB extension of the
AWS SDK.

## Networking inside the ScyllaDB cluster

The previous sections were all about reducing the amount of network traffic
between the client application and the ScyllaDB cluster. If the network
between different ScyllaDB nodes is also metered - especially between nodes
in different racks - then this intra-cluster networking should also be reduced.
The best way to do that is to enable compression of the internode
communication. Refer to the "Advanced Internode (RPC) Compression" document
for instructions on how to enable it.
