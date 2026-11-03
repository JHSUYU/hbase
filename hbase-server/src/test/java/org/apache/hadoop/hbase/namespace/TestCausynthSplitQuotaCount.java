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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Native demonstration of hbase-inj-05 (no GraphChecker run): TestCausynthSplitTableQuota's steps
 * with the invariant asserted. After the split the master's namespace quota must count the split
 * table's two regions, and a further one-region table must be refused (three regions under a
 * quota of two). The unmodified code passes; with hbase-inj-05 the count is one and the table is
 * created, and nothing in the system reports it.
 */
@Tag(MediumTests.TAG)
public class TestCausynthSplitQuotaCount {
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final String NAMESPACE = "causynth_split_count";

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
  public void theQuotaCountsEveryRegion() throws Exception {
    Admin admin = UTIL.getAdmin();
    admin.createNamespace(NamespaceDescriptor.create(NAMESPACE)
      .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "2").build());
    TableName split = TableName.valueOf(NAMESPACE, "split");
    admin.createTable(TestCausynthSplitTableQuota.table(split));
    RegionInfo region = admin.getRegions(split).get(0);
    admin.splitRegionAsync(region.getRegionName(), Bytes.toBytes("m")).get();

    int regions = admin.getRegions(split).size();
    int counted = UTIL.getMiniHBaseCluster().getMaster().getMasterQuotaManager()
      .getNamespaceQuotaManager().getState(NAMESPACE).getRegionCount();
    boolean refused;
    try {
      admin.createTable(TestCausynthSplitTableQuota.table(TableName.valueOf(NAMESPACE, "created")));
      refused = false;
    } catch (IOException e) {
      refused = true;
    }
    final boolean createRefused = refused;
    assertAll(
      () -> assertEquals(regions, counted,
        "the namespace quota counts the split table's " + regions + " regions"),
      () -> assertTrue(createRefused, "a third region in a namespace that allows two is refused"));
  }
}
