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
package org.apache.hadoop.hbase.backup;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * GraphChecker workload for HBase bug H2-16 (docs/checks/HBASE.md): one client takes the backup
 * system table's exclusive lock and gives it back. Its acquisition is a checkAndMutate sent with no
 * nonce; a reply lost after the region server committed it makes the client's retry meet its own
 * lock, so its first startBackupExclusiveOperation throws ExclusiveOperationException and the lock
 * is never released. One acquirer, so no other client's hold explains that throw (TestBackupManager's
 * two workers throw it by contention in every passing run).
 */
@Tag(MediumTests.TAG)
public class TestCausynthBackupLockReplyLost {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static SingleProcessHBaseCluster cluster;
  private static Connection conn;

  @BeforeAll
  public static void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean(BackupRestoreConstants.BACKUP_ENABLE_KEY, true);
    conf.setInt(BackupManager.BACKUP_EXCLUSIVE_OPERATION_TIMEOUT_SECONDS_KEY, 5);
    BackupManager.decorateMasterConfiguration(conf);
    BackupManager.decorateRegionServerConfiguration(conf);
    cluster = UTIL.startMiniCluster();
    conn = UTIL.getConnection();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void takeAndReleaseTheBackupLock() throws IOException {
    try (BackupManager backupManager = new BackupManager(conn, conn.getConfiguration())) {
      backupManager.startBackupSession();
      backupManager.finishBackupSession();
    }
  }
}
