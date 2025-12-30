# Reducing network costs in Alternator

In some deployments, the network between the application and the ScyllaDB
cluster is limited in bandwidth, or network traffic is expensive.
This document surveys some mechanisms that can be used to reduce this network
traffic.

## Compression

An easy way to reduce network traffic between the application and the
ScyllaDB cluster is to compress the requests and responses. The resulting
saving can be substantial if items are long and compress well - e.g., long
strings. But additionally, Alternator's protocol (the DynamoDB API) is
JSON-based - textual and verbose - and compresses well.

Note that enabling compression also has a downside - it requires more
computation by both client and server. So the choice of whether to enable
compression depends on the relative cost of network traffic and CPU.

### Compression of requests

When the application sends a large request - notably a `PutItem` or
`BatchWriteItem` - we can reduce network usage by compressing the content
of this request.

Alternator's request protocol - the DynamoDB API - is based on HTTP or HTTPS.
The standard HTTP 1.1 mechanism for compressing a request is to compress
the request's body using some compression algorithm (e.g., gzip) and
send a header `Content-Encoding: gzip` stating which compression algorithm
was used. Alternator currently supports two compression algorithms, `gzip`
and `deflate`, both standardized in ([RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html)).
Other standard compression types which are listed in
[IANA's HTTP Content Coding Registry](https://www.iana.org/assignments/http-parameters/http-parameters.xhtml#content-coding),
including `zstd` ([RFC 8878][https://www.rfc-editor.org/rfc/rfc8878.html]),
are not yet supported by Alternator.

Note that HTTP's compression only compresses the request's _body_ - not the
request's _headers_ - so it is beneficial to avoid sending unnecessary headers
in the request, as they will not be compressed. See more on this below.

To use compressed requests, the client library (SDK) used by the application
should be configured to actually compress requests. Amazon's AWS SDKs support
this feature in some languages, but not in others, and may automatically
compress only requests that are longer than a certain size. Check their website
<https://docs.aws.amazon.com/sdkref/latest/guide/feature-compression.html>
for more details for the specific SDK you are using. ScyllaDB also publishes
extensions for these SDKs which may have better support for compressed
requests (and other features mentioned in this document), so again please
consult the documentation of the specific SDK that you are using.

### Compression of responses

Some types of requests, notably `GetItem` and `BatchGetItem`, can have small
requests but large responses, so it can be beneficial to compress these
responses. The standard HTTP mechanism for doing this is that the client
provides a header like `Accept-Encoding: gzip` in the request, signalling
that it is ready to accept a gzip'ed response. Alternator may then compress
the response, or not, depending on its configuration and the response's
size. If Alternator does compress the response body, it sets a header
`Content-Encoding: gzip` in the response.

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
support compressing responses, it is not clear if any of Amazon's AWS SDKs can
use this feature. ScyllaDB publishes extensions for these SDKs which may
have better support for compressed responses (and other features mentioned
in this document), so please consult the documentation of the specific SDK that
you are using to check if it can make use of the response compression
feature that Alternator supports.

## Fewer and shorter headers

As we saw above, the HTTP 1.1 protocol allows compressing the _bodies_ of
requests and responses. But HTTP 1.1 does not have a mechanism to compress
_headers_. So both client (SDK) and server (Alternator) should avoid sending
headers that are unnecessary or unnecessarily long.

The Alternator server sends headers like the following in its responses:
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

Some middleware or gateways modify HTTP traffic going through them and add
even more headers for their own use. If you suspect this might be happening,
inspect the HTTP traffic of your workload with a packet analyzer to see if
any unnecessary HTTP headers appear in requests arriving to Alternator, or
in responses arriving back at the application.

## Rack-aware request routing

In some deployments, the network between the client and the ScyllaDB server
is only expensive if the communication crosses **rack** boundaries.
A "rack" is a ScyllaDB term roughly equivalent to AWS's "availability zone".

Typically, a ScyllaDB cluster is divided to three racks and each item of
data is replicated once on each rack. Each instance of the client application
also lives in one of these racks. In many deployments, communication inside
the rack is free but communicating to a different rack costs extra. It is
therefore important that the client be aware of which rack it is running
on, and send the request to a ScyllaDB node on the same rack. It should not
choose one of the three replicas as random. This is known as "rack-aware
request routing", and should be enabled in the ScyllaDB extension of the
AWS SDK.

## Networking inside the ScyllaDB cluster

The previous sections were all about reducing the amount of network traffic
between the client application and the ScyllaDB cluster. If the network
between different ScyllaDB nodes is also metered - especially between nodes
in different racks - then this intra-cluster networking should also be reduced.
The best way to do that is to enable compression of internode communication.
Refer to [Advanced Internode (RPC) Compression](../operating-scylla/procedures/config-change/advanced-internode-compression.html)
for instructions on how to enable it.
