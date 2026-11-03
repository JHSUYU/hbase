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
 * GraphChecker workload of hbase-inj-05 (hbase-adhoc-22's point, NamespaceStateManager's region
 * quota at table creation), from hbase-inj-02's TestCausynthMergedTableQuota with a split in place
 * of the merge. The namespace allows two regions; a one-region table is created in it, its region
 * is split in two, and a one-region table is created. With hbase-inj-05 injected the split's new
 * region is never counted: the creation is checked against one region, fits, and the namespace
 * holds three regions under a quota of two, with no exception. On the unmodified code the split is
 * counted and the creation is refused at the check.
 */
@Tag(MediumTests.TAG)
public class TestCausynthSplitTableQuota {
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final String NAMESPACE = "causynth_split_quota";

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
  public void createAfterASplit() throws Exception {
    Admin admin = UTIL.getAdmin();
    admin.createNamespace(NamespaceDescriptor.create(NAMESPACE)
      .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "2").build());
    TableName split = TableName.valueOf(NAMESPACE, "split");
    admin.createTable(table(split));
    RegionInfo region = admin.getRegions(split).get(0);
    admin.splitRegionAsync(region.getRegionName(), Bytes.toBytes("m")).get();
    admin.createTable(table(TableName.valueOf(NAMESPACE, "created")));
  }

  static TableDescriptor table(TableName name) {
    return TableDescriptorBuilder.newBuilder(name)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build();
  }
}
