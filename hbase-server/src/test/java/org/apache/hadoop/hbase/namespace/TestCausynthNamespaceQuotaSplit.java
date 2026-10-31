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

import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
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
 * GraphChecker workload of hbase-inj-03 (hbase-adhoc-08, ClusterSchemaServiceImpl:111, a
 * namespace the master's namespace cache does not hold), from hbase-adhoc-22's
 * TestCausynthTruncatedTableQuota: with the namespace quota on, a namespace allowing ten regions
 * is created, a one-region table in it, the namespace is modified to allow twenty, and the table's
 * region is split. The split's namespace-quota check (SplitTableRegionProcedure's PRE_OPERATION,
 * NamespaceStateManager#checkAndUpdateNamespaceRegionCount) looks the namespace up on a procedure
 * worker. With hbase-inj-03 injected, a modification whose hbase:meta put fails drops the cached
 * descriptor and finishes, so that lookup finds no namespace; here the put succeeds.
 */
@Tag(MediumTests.TAG)
public class TestCausynthNamespaceQuotaSplit {
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final String NAMESPACE = "causynth_split";

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
  public void splitAfterTheNamespaceIsModified() throws Exception {
    Admin admin = UTIL.getAdmin();
    admin.createNamespace(NamespaceDescriptor.create(NAMESPACE)
      .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "10").build());
    TableName table = TableName.valueOf(NAMESPACE, "split");
    admin.createTable(TableDescriptorBuilder.newBuilder(table)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build());
    admin.modifyNamespace(NamespaceDescriptor.create(NAMESPACE)
      .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "20").build());
    RegionInfo region = admin.getRegions(table).get(0);
    admin.splitRegionAsync(region.getRegionName(), Bytes.toBytes("m")).get();
  }
}
