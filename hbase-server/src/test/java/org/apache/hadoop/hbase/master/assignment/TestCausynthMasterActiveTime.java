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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * GraphChecker workload of hbase-bugs-master-active-time (bug_study HBase #14): a master failover
 * while a region opens. The active master dispatches the region's open with its active time; the
 * region server holds the open until that master stopped and the standby master is active and
 * initialized, then reports OPENED, with the old master's active time, to the master the master
 * znode names now. The new master compares that time with its own (MasterRpcServices
 * #throwOnOldMaster): its activation came later, so the report is taken and the region opens.
 */
@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestCausynthMasterActiveTime {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final TableName NAME = TableName.valueOf("ActiveTime");

  private static final byte[] CF = Bytes.toBytes("cf");

  private static volatile CountDownLatch ARRIVE;

  private static volatile CountDownLatch RESUME;

  /** Holds a region's open, once armed, until the test resumes it. */
  public static final class HoldOpen implements RegionCoprocessor, RegionObserver {
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void postOpen(ObserverContext<? extends RegionCoprocessorEnvironment> c) {
      CountDownLatch arrive = ARRIVE;
      CountDownLatch resume = RESUME;
      if (arrive == null || resume == null) {
        return;
      }
      arrive.countDown();
      try {
        resume.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(
      StartTestingClusterOption.builder().numMasters(2).numRegionServers(1).build());
    UTIL.getAdmin()
      .createTable(TableDescriptorBuilder.newBuilder(NAME)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
        .setCoprocessor(HoldOpen.class.getName()).build());
    UTIL.waitTableAvailable(NAME);
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void reportOpenedToTheNextMaster() throws Exception {
    SingleProcessHBaseCluster cluster = UTIL.getMiniHBaseCluster();
    RegionInfo region = cluster.getRegions(NAME).get(0).getRegionInfo();
    HMaster old = cluster.getMaster();
    ServerName server =
      old.getAssignmentManager().getRegionStates().getRegionStateNode(region).getRegionLocation();

    ARRIVE = new CountDownLatch(1);
    RESUME = new CountDownLatch(1);
    old.getAssignmentManager().moveAsync(new RegionPlan(region, server, server));
    ARRIVE.await();
    ARRIVE = null;

    cluster.stopMaster(old.getServerName());
    UTIL.waitFor(60000, () -> {
      for (MasterThread thread : cluster.getMasterThreads()) {
        HMaster master = thread.getMaster();
        if (master != old && master.isActiveMaster() && master.isInitialized()) {
          return true;
        }
      }
      return false;
    });
    RESUME.countDown();

    HRegionServer regionServer = cluster.getRegionServer(server);
    UTIL.waitFor(60000, () -> regionServer.getRegion(region.getEncodedName()) != null);
    UTIL.waitUntilNoRegionsInTransition(60000);
    assertNotNull(regionServer.getRegion(region.getEncodedName()));
  }
}
