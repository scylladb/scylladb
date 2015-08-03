/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.service;

public interface IMigrationListener
{
    public void onCreateKeyspace(String ksName);
    public void onCreateColumnFamily(String ksName, String cfName);
    public void onCreateUserType(String ksName, String typeName);
    public void onCreateFunction(String ksName, String functionName);
    public void onCreateAggregate(String ksName, String aggregateName);

    public void onUpdateKeyspace(String ksName);
    public void onUpdateColumnFamily(String ksName, String cfName);
    public void onUpdateUserType(String ksName, String typeName);
    public void onUpdateFunction(String ksName, String functionName);
    public void onUpdateAggregate(String ksName, String aggregateName);

    public void onDropKeyspace(String ksName);
    public void onDropColumnFamily(String ksName, String cfName);
    public void onDropUserType(String ksName, String typeName);
    public void onDropFunction(String ksName, String functionName);
    public void onDropAggregate(String ksName, String aggregateName);

}
