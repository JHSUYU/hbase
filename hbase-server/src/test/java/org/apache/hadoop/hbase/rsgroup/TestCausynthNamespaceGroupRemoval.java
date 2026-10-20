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
package org.apache.hadoop.hbase.rsgroup;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.procedure.CreateNamespaceProcedure;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RSGroupTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GraphChecker workload of hbase-bugs-ns-rsgroup-removal: a namespace is created in a region server
 * group, and the group is emptied and removed while the creation's procedure may still run. The
 * creation checks the group once, at its prepare step; the removal checks the namespaces the master
 * has already inserted. Nothing orders the two but time: the test waits a second, not for the
 * procedure, so here the insertion lands before the removal's check, the removal is refused, and
 * the table created in the namespace afterwards is refused for the group's emptiness. Had the
 * removal's check run before the insertion, the group would be gone while the namespace still
 * names it, and every table created in the namespace would be refused for a group that does not
 * exist. A failed insertion is retried only after the removal returned
 * ({@link CreateNamespaceProcedure#insertRetryHold}); with no failure the hold is never reached.
 */
@Tag(RSGroupTests.TAG)
@Tag(MediumTests.TAG)
public class TestCausynthNamespaceGroupRemoval {
  private static final Logger LOG = LoggerFactory.getLogger(TestCausynthNamespaceGroupRemoval.class);
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final String GROUP = "causynth_group";
  private static final String NAMESPACE = "causynth_ns";

  @BeforeAll
  public static void setUp() throws Exception {
    RSGroupUtil.enableRSGroup(UTIL.getConfiguration());
    UTIL.startMiniCluster(2);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    CreateNamespaceProcedure.insertRetryHold = null;
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void removeTheGroupOfANamespaceBeingCreated() throws Exception {
    Admin admin = UTIL.getAdmin();
    ServerName meta = UTIL.getMiniHBaseCluster().getServerHoldingMeta();
    Address server = UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(thread -> thread.getRegionServer().getServerName()).filter(name -> !name.equals(meta))
      .findFirst().get().getAddress();
    admin.addRSGroup(GROUP);
    admin.moveServersToRSGroup(Collections.singleton(server), GROUP);

    CountDownLatch hold = new CountDownLatch(1);
    CreateNamespaceProcedure.insertRetryHold = hold;
    Future<Void> creation = admin.createNamespaceAsync(NamespaceDescriptor.create(NAMESPACE)
      .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, GROUP).build());
    Thread.sleep(1000);
    admin.moveServersToRSGroup(Collections.singleton(server), RSGroupInfo.DEFAULT_GROUP);
    try {
      admin.removeRSGroup(GROUP);
      LOG.info("Removed {} while {} was being created", GROUP, NAMESPACE);
    } catch (IOException refused) {
      LOG.info("Removal of {} refused", GROUP, refused);
    } finally {
      hold.countDown();
    }
    creation.get();

    try {
      admin.createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(NAMESPACE, "t"))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build());
      LOG.info("Created a table in {}", NAMESPACE);
    } catch (IOException refused) {
      LOG.info("Table creation in {} refused", NAMESPACE, refused);
    }
  }
}
