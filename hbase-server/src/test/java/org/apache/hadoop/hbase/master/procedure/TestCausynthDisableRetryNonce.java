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
package org.apache.hadoop.hbase.master.procedure;

import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * GraphChecker workload of hbase-inj-01 (hbase-adhoc-04, AbstractStateMachineTableProcedure:163,
 * a table procedure's check that its table is enabled), copied from hbase-buggy-20642's: two
 * tables are created and each is disabled once through the client's Admin. With hbase-inj-01
 * injected, the master's DisableTable handler drops the request's nonce, so an attempt the client
 * retries after its reply was lost is not the master's duplicate of the first: the master
 * constructs a second DisableTableProcedure for the table the first one is disabling, and its
 * preflight finds the table no longer enabled. Nothing here retries: the run only reaches that
 * throw in a world where a reply is lost.
 */
@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestCausynthDisableRetryNonce {
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void disableTwoTables() throws Exception {
    Admin admin = UTIL.getAdmin();
    TableName first = TableName.valueOf("causynth_disable_first");
    TableName second = TableName.valueOf("causynth_disable_second");
    for (TableName table : new TableName[] { first, second }) {
      admin.createTable(TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build());
    }
    admin.disableTable(first);
    admin.disableTable(second);
  }
}
