# Defining Workload Attributes

## Introduction

A typical database has more than one [workload](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Workload) running simultaneously with a different acceptable level of latency and
throughput. By defining the attributes of each workload, you can specify how ScyllaDB will handle requests depending on
the workload to which they are assigned.

You can define a workload’s attribute using the *service level* concept. The service level CQL commands allow you to attach
attributes to users and roles. When a user logs into the system, all of the attributes attached to that user and to the roles
granted to that user are combined and become a set of workload attributes.

See [Service Level Management](https://enterprise.docs.scylladb.com/stable/using-scylla/workload-prioritization.html#workload-prioritization-workflow) for more information about service levels.

## Prerequisites

* An [authenticated](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/runtime-authentication.md) and [authorized](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/enable-authorization.md) user
* At least one [role created](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/authorization.md#create-role-statement).

## Procedure

1. Create a service level with the desired attribute.
   > ```cql
   > CREATE SERVICE LEVEL <service_level_name> WITH <attribute> [ AND <attribute>];
   > ```

   > For example:
   > ```cql
   > CREATE SERVICE LEVEL sl2 WITH timeout = 500ms AND workload_type=interactive;
   > ```

   > See [Available Attributes](#workload-attributes-available-attributes).
2. Assign a service level to a role or user:
   > ```cql
   > ATTACH SERVICE_LEVEL <service_level_name> TO <role_name|user_name>;
   > ```

   > For example:
   > ```cql
   > ATTACH SERVICE LEVEL sl2 TO scylla;
   > ```

You can modify the service level attributes with the `ALTER SERVICE LEVEL` command:

> > ```cql
> > ALTER SERVICE LEVEL <service_level_name> WITH <attribute> [ AND <attribute>];
> > ```

> For example:
```cql
ALTER SERVICE LEVEL sl2 WITH timeout = null;
```

<a id="workload-attributes-available-attributes"></a>

## Available Attributes

| Attribute       | Details                                                          |
|-----------------|------------------------------------------------------------------|
| `timeout`       | [Specifying Service Level Timeout](#workload-attributes-timeout) |
| `workload_type` | [Specifying Workload Type](#workload-attributes-workload-type)   |

<a id="workload-attributes-timeout"></a>

## Specifying Service Level Timeout

You can specify the timeout for a service level (in milliseconds or seconds) with the `timeout` attribute.

For example:

```cql
CREATE SERVICE LEVEL primary WITH timeout = 30ms;
```

Specifying the timeout value is useful when your workloads have different acceptable latency levels.

<a id="workload-attributes-workload-type"></a>

## Specifying Workload Type

You can specify the workload type for a service level with the `workload_type` attribute.

For example:

```cql
CREATE SERVICE LEVEL secondary WITH workload_type = 'batch';
```

Specifying the workload type allows ScyllaDB to handle sessions more efficiently (for example, depending on whether the workload is
sensitive to latency).

### Available Workload Types

| Workload type   | Description                                                                                                                                                                                                                |
|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `unspecified`   | A generic workload without any specific characteristics (default).                                                                                                                                                         |
| `interactive`   | A workload sensitive to latency, expected to have high/unbounded concurrency, with dynamic characteristics. For example, a workload assigned to users clicking on a website and generating events with their clicks.       |
| `batch`         | A workload for processing large amounts of data, not sensitive to latency, expected to have fixed concurrency. For example, a workload assigned to processing billions of historical sales records to generate statistics. |
