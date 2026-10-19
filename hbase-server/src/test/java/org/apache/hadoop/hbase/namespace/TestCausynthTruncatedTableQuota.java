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
package org.apache.hadoop.hbase.namespace;

import java.util.concurrent.Future;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * GraphChecker workload of hbase-adhoc-22 (NamespaceStateManager's region quota at table creation)
 * that holds HBASE-30483: a table truncated without its splits keeps its old region count in the
 * master's namespace quota. The namespace allows two regions; its first table has two, is truncated
 * to one (the quota still counts two) and is deleted while a one-region table is created. Here the
 * deletion's quota release lands first and the creation fits. Had the creation's check run before
 * the release, it would count the truncated table's two regions and refuse the creation, where the
 * table's one real region leaves room for it.
 */
@Tag(MediumTests.TAG)
public class TestCausynthTruncatedTableQuota {
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final String NAMESPACE = "causynth_quota";

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    UTIL.startMiniCluster(1);
    UTIL.waitFor(30000,
      () -> UTIL.getMiniHBaseCluster().getMaster().getMasterQuotaManager().isQuotaInitialized());
  }

  @AfterAll
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void createWhileATruncatedTableIsDeleted() throws Exception {
    Admin admin = UTIL.getAdmin();
    admin.createNamespace(NamespaceDescriptor.create(NAMESPACE)
      .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "2").build());
    TableName truncated = TableName.valueOf(NAMESPACE, "truncated");
    admin.createTable(table(truncated), new byte[][] { Bytes.toBytes("m") });
    admin.disableTable(truncated);
    admin.truncateTable(truncated, false);
    admin.disableTable(truncated);

    Future<Void> deletion = admin.deleteTableAsync(truncated);
    Thread.sleep(2000);
    admin.createTable(table(TableName.valueOf(NAMESPACE, "created")));
    deletion.get();
  }

  private static TableDescriptor table(TableName name) {
    return TableDescriptorBuilder.newBuilder(name)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build();
  }
}
