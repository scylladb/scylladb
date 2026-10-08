# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# The tests in this file test the "COMMENT ON" and "SECURITY LABEL ON"
# statements, which Cassandra added in Cassandra 6.0.

from ...porting import *
from ....util import new_cql
from cassandra import Unauthorized

TABLE_NAME = "tbl_comment"
SECURITY_TABLE_NAME = "tbl_security"
TYPE_NAME = "address"

# Test data constants
TEST_COMMENT = "Test comment"
UPDATED_COMMENT = "Updated comment"
TEST_LABEL = "TEST_LABEL"
UPDATED_LABEL = "UPDATED_LABEL"

# The original test creates its keyspaces with SimpleStrategy, but Scylla
# doesn't allow SimpleStrategy when tablets are enabled, so we use
# NetworkTopologyStrategy, with the same replication factor, instead.
REPLICATION = "replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}"

# Helper methods for setting comments and security labels
def setComment(cql, type, objectName, comment):
    statement = buildCommentStatement(type, objectName, comment)
    cql.execute(statement)

def setSecurityLabel(cql, type, objectName, label):
    statement = buildSecurityLabelStatement(type, objectName, label)
    cql.execute(statement)

def buildStatement(statementType, type, objectName, value):
    valueClause = "'" + value.replace("'", "''") + "'" if value is not None else "NULL"
    return f"{statementType} ON {type} {objectName} IS {valueClause}"

def buildCommentStatement(type, objectName, comment):
    return buildStatement("COMMENT", type, objectName, comment)

def buildSecurityLabelStatement(type, objectName, label):
    return buildStatement("SECURITY LABEL", type, objectName, label)

# Helper methods for assertions
def assertComment(cql, type, keyspace, objectName, expected):
    actual = getComment(cql, type, keyspace, objectName)
    assert expected == actual

def assertSecurityLabel(cql, type, keyspace, objectName, expected):
    actual = getSecurityLabel(cql, type, keyspace, objectName)
    assert expected == actual

def assertWarningsContain(result, expected):
    warnings = result.response_future.warnings
    assert warnings and any(expected in warning for warning in warnings)

def extractObjectName(type, objectName):
    if type == "TABLE" or type == "TYPE":
        return objectName.split(".")[1] if "." in objectName else objectName
    return objectName

def parseColumnReference(objectName):
    parts = objectName.split(".")
    if len(parts) == 2:
        return [parts[0], parts[1]] # table/type, column/field
    elif len(parts) == 3:
        return [parts[1], parts[2]] # table/type, column/field (ignore keyspace part)
    else:
        raise ValueError("Invalid reference format: " + objectName)

# The original Java test reads the comments and security labels from
# Cassandra's internal schema objects. Here, we read them through CQL from
# the virtual tables system_views.schema_comments and
# system_views.schema_security_labels, which Cassandra 6 added together with
# this feature. These tables have a row only for schema elements with a
# non-empty comment or security label, so a missing row means an empty one.
def getMetadataValue(cql, type, keyspace, objectName, isComment):
    if isComment:
        table, column = "system_views.schema_comments", "comment"
    else:
        table, column = "system_views.schema_security_labels", "security_label"
    # The virtual tables' clustering key is (table_name, column_name,
    # udt_name, field_name), and unused components are empty strings.
    if type == "KEYSPACE":
        objectType, key = "KEYSPACE", ["", "", "", ""]
    elif type == "TABLE":
        objectType, key = "TABLE", [extractObjectName(type, objectName), "", "", ""]
    elif type == "COLUMN":
        columnParts = parseColumnReference(objectName)
        objectType, key = "COLUMN", [columnParts[0], columnParts[1], "", ""]
    elif type == "TYPE":
        objectType, key = "UDT", ["", "", extractObjectName(type, objectName), ""]
    elif type == "FIELD":
        fieldParts = parseColumnReference(objectName)
        objectType, key = "FIELD", ["", "", fieldParts[0], fieldParts[1]]
    else:
        raise ValueError("Unsupported object type: " + type)
    rows = list(cql.execute(f"SELECT {column} FROM {table} WHERE object_type = %s AND keyspace_name = %s AND table_name = %s AND column_name = %s AND udt_name = %s AND field_name = %s",
                            [objectType, keyspace] + key))
    return rows[0][0] if rows else ""

def getComment(cql, type, keyspace, objectName):
    return getMetadataValue(cql, type, keyspace, objectName, True)

def getSecurityLabel(cql, type, keyspace, objectName):
    return getMetadataValue(cql, type, keyspace, objectName, False)

# Generic lifecycle test method
def metadataLifecycle(cql, type, keyspace, objectName, isComment):
    testValue = TEST_COMMENT if isComment else TEST_LABEL
    updatedValue = UPDATED_COMMENT if isComment else UPDATED_LABEL
    if isComment:
        emptyStringStatement = buildCommentStatement(type, objectName, "")
        assert_invalid_message(cql, keyspace, "Cannot set comment to empty string", emptyStringStatement)
        setComment(cql, type, objectName, testValue)
        assertComment(cql, type, keyspace, objectName, testValue)
        setComment(cql, type, objectName, updatedValue)
        assertComment(cql, type, keyspace, objectName, updatedValue)
        setComment(cql, type, objectName, None)
        assertComment(cql, type, keyspace, objectName, "")
        setComment(cql, type, objectName, None)
        assertComment(cql, type, keyspace, objectName, "")
        longComment = buildCommentStatement(type, objectName, "a" * 129)
        assert_invalid_message(cql, keyspace, "comment length (129) exceeds maximum allowed length (128)", longComment)
    else:
        emptyStringStatement = buildSecurityLabelStatement(type, objectName, "")
        assert_invalid_message(cql, keyspace, "Cannot set security label to empty string", emptyStringStatement)
        setSecurityLabel(cql, type, objectName, testValue)
        assertSecurityLabel(cql, type, keyspace, objectName, testValue)
        setSecurityLabel(cql, type, objectName, updatedValue)
        assertSecurityLabel(cql, type, keyspace, objectName, updatedValue)
        setSecurityLabel(cql, type, objectName, None)
        assertSecurityLabel(cql, type, keyspace, objectName, "")
        setSecurityLabel(cql, type, objectName, None)
        assertSecurityLabel(cql, type, keyspace, objectName, "")
        longSecurityLabel = buildSecurityLabelStatement(type, objectName, "a" * 49)
        assert_invalid_message(cql, keyspace, "security label length (49) exceeds maximum allowed length (48)", longSecurityLabel)

def commentLifecycle(cql, type, keyspace, objectName):
    metadataLifecycle(cql, type, keyspace, objectName, True)

def securityLabelLifecycle(cql, type, keyspace, objectName):
    metadataLifecycle(cql, type, keyspace, objectName, False)

def createTableWithName(cql, keyspace, taleName):
    cql.execute(f"CREATE TABLE {keyspace}.{taleName} (id int PRIMARY KEY, name text)")

# Reproduces SCYLLADB-5146 (COMMENT ON and SECURITY LABEL ON statements).
@pytest.mark.xfail(reason="SCYLLADB-5146")
def testCommentOnKeyspace(cql, new_to_cassandra_6):
    with create_keyspace(cql, REPLICATION) as ks:
        commentLifecycle(cql, "KEYSPACE", ks, ks)

# Reproduces SCYLLADB-5146 (COMMENT ON and SECURITY LABEL ON statements).
@pytest.mark.xfail(reason="SCYLLADB-5146")
def testSecurityLabelOnKeyspace(cql, new_to_cassandra_6):
    with create_keyspace(cql, REPLICATION) as ks:
        securityLabelLifecycle(cql, "KEYSPACE", ks, ks)

        # Test provider warning
        result = cql.execute(f"SECURITY LABEL FOR test_provider ON KEYSPACE {ks} IS 'SENSITIVE'")
        assertWarningsContain(result, "Provider functionality not implemented.")
        assertSecurityLabel(cql, "KEYSPACE", ks, ks, "SENSITIVE")

# Reproduces SCYLLADB-5146 (COMMENT ON and SECURITY LABEL ON statements).
@pytest.mark.xfail(reason="SCYLLADB-5146")
def testCommentOnTable(cql, new_to_cassandra_6):
    with create_keyspace(cql, REPLICATION) as ks:
        createTableWithName(cql, ks, TABLE_NAME)
        tableRef = f"{ks}.{TABLE_NAME}"
        commentLifecycle(cql, "TABLE", ks, tableRef)

# Reproduces SCYLLADB-5146 (COMMENT ON and SECURITY LABEL ON statements).
@pytest.mark.xfail(reason="SCYLLADB-5146")
def testSecurityLabelOnTable(cql, new_to_cassandra_6):
    with create_keyspace(cql, REPLICATION) as ks:
        createTableWithName(cql, ks, SECURITY_TABLE_NAME)
        tableRef = f"{ks}.{SECURITY_TABLE_NAME}"
        securityLabelLifecycle(cql, "TABLE", ks, tableRef)

        # Test provider warning
        result = cql.execute(f"SECURITY LABEL FOR my_provider ON TABLE {tableRef} IS 'CONFIDENTIAL'")
        assertWarningsContain(result, "Provider functionality not implemented.")
        assertSecurityLabel(cql, "TABLE", ks, tableRef, "CONFIDENTIAL")

# Reproduces SCYLLADB-5146 (COMMENT ON and SECURITY LABEL ON statements).
@pytest.mark.xfail(reason="SCYLLADB-5146")
def testCommentOnColumn(cql, new_to_cassandra_6):
    with create_keyspace(cql, REPLICATION) as ks:
        createTableWithName(cql, ks, TABLE_NAME)
        columnRef = f"{ks}.{TABLE_NAME}.name"
        commentLifecycle(cql, "COLUMN", ks, columnRef)

# Reproduces SCYLLADB-5146 (COMMENT ON and SECURITY LABEL ON statements).
@pytest.mark.xfail(reason="SCYLLADB-5146")
def testSecurityLabelOnColumn(cql, new_to_cassandra_6):
    with create_keyspace(cql, REPLICATION) as ks:
        cql.execute(f"CREATE TABLE {ks}.{SECURITY_TABLE_NAME} (id int PRIMARY KEY, ssn text, name text)")
        columnRef = f"{ks}.{SECURITY_TABLE_NAME}.ssn"
        securityLabelLifecycle(cql, "COLUMN", ks, columnRef)

        # Test provider warning
        result = cql.execute(f"SECURITY LABEL FOR data_classifier ON COLUMN {columnRef} IS 'PII'")
        assertWarningsContain(result, "Provider functionality not implemented.")
        assertSecurityLabel(cql, "COLUMN", ks, columnRef, "PII")

# Reproduces SCYLLADB-5146 (COMMENT ON and SECURITY LABEL ON statements).
@pytest.mark.xfail(reason="SCYLLADB-5146")
def testCommentOnType(cql, new_to_cassandra_6):
    with create_keyspace(cql, REPLICATION) as ks:
        cql.execute(f"CREATE TYPE {ks}.{TYPE_NAME} (street text, city text, zip int)")
        typeRef = f"{ks}.{TYPE_NAME}"
        commentLifecycle(cql, "TYPE", ks, typeRef)

# Reproduces SCYLLADB-5146 (COMMENT ON and SECURITY LABEL ON statements).
@pytest.mark.xfail(reason="SCYLLADB-5146")
def testSecurityLabelOnType(cql, new_to_cassandra_6):
    with create_keyspace(cql, REPLICATION) as ks:
        typeName = "personal_info"
        cql.execute(f"CREATE TYPE {ks}.{typeName} (ssn text, dob date)")
        typeRef = f"{ks}.{typeName}"
        securityLabelLifecycle(cql, "TYPE", ks, typeRef)

        # Test provider warning
        result = cql.execute(f"SECURITY LABEL FOR security_provider ON TYPE {typeRef} IS 'RESTRICTED'")
        assertWarningsContain(result, "Provider functionality not implemented.")
        assertSecurityLabel(cql, "TYPE", ks, typeRef, "RESTRICTED")

# Reproduces SCYLLADB-5146 (COMMENT ON and SECURITY LABEL ON statements).
@pytest.mark.xfail(reason="SCYLLADB-5146")
def testCommentOnField(cql, new_to_cassandra_6):
    with create_keyspace(cql, REPLICATION) as ks:
        cql.execute(f"CREATE TYPE {ks}.geo_position (latitude double, longitude double, altitude double)")
        fieldRef = f"{ks}.geo_position.latitude"
        commentLifecycle(cql, "FIELD", ks, fieldRef)

# Reproduces SCYLLADB-5146 (COMMENT ON and SECURITY LABEL ON statements).
@pytest.mark.xfail(reason="SCYLLADB-5146")
def testSecurityLabelOnField(cql, new_to_cassandra_6):
    with create_keyspace(cql, REPLICATION) as ks:
        cql.execute(f"CREATE TYPE {ks}.patient_record (ssn text, diagnosis text, treatment text)")
        fieldRef = f"{ks}.patient_record.ssn"
        securityLabelLifecycle(cql, "FIELD", ks, fieldRef)

        # Test provider warning
        result = cql.execute(f"SECURITY LABEL FOR healthcare_provider ON FIELD {fieldRef} IS 'PHI'")
        assertWarningsContain(result, "Provider functionality not implemented.")
        assertSecurityLabel(cql, "FIELD", ks, fieldRef, "PHI")

# Reproduces SCYLLADB-5146 (COMMENT ON and SECURITY LABEL ON statements).
@pytest.mark.xfail(reason="SCYLLADB-5146")
def testFieldWithUseKeyspace(cql, new_to_cassandra_6):
    with create_keyspace(cql, REPLICATION) as ks:
        cql.execute(f"CREATE TYPE {ks}.address (street text, city text, zip int)")
        # "USE" cannot be undone, so we do it on a separate connection
        with new_cql(cql) as ncql:
            ncql.execute(f"USE {ks}")

            # Test unqualified field reference with USE KEYSPACE context
            setComment(ncql, "FIELD", "address.street", "Street address")
            setSecurityLabel(ncql, "FIELD", "address.street", "PUBLIC")
            setComment(ncql, "FIELD", "address.city", "City name")
            setSecurityLabel(ncql, "FIELD", "address.city", "PUBLIC")

        # Verify
        assertComment(cql, "FIELD", ks, "address.street", "Street address")
        assertSecurityLabel(cql, "FIELD", ks, "address.street", "PUBLIC")
        assertComment(cql, "FIELD", ks, "address.city", "City name")
        assertSecurityLabel(cql, "FIELD", ks, "address.city", "PUBLIC")
