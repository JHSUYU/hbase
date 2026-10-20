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
package org.apache.hadoop.hbase.security.token;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * GraphChecker workload of hbase-adhoc-34 (owner 2026-10-19: no MiniKdc, no Kerberos): every
 * server runs its token secret manager on a simple-authentication cluster (RpcServer
 * #CAUSYNTH_TOKEN_SECRETS). The manager that won the key-master election generates a token; the
 * other server's manager, which learnt the keys through ZooKeeper (ZKSecretWatcher), validates the
 * token's identifier as its bytes decode, as SASL hands it over (retrievePassword: the key looked up
 * by id, the keys refreshed from ZooKeeper when it is missing). The other server validates the token
 * before it has processed the watch event of the token's key (the key is removed from its manager
 * first), so the validation refreshes the keys from ZooKeeper.
 */
@Tag(SecurityTests.TAG)
@Tag(MediumTests.TAG)
public class TestCausynthTokenKeys {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setBoolean(RpcServer.CAUSYNTH_TOKEN_SECRETS, true);
    UTIL.startMiniCluster(1);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void anotherServerValidatesTheKeyMastersToken() throws Exception {
    SingleProcessHBaseCluster cluster = UTIL.getMiniHBaseCluster();
    AuthenticationTokenSecretManager onMaster = (AuthenticationTokenSecretManager) ((RpcServer) cluster
      .getMaster().getRpcServer()).getSecretManager();
    AuthenticationTokenSecretManager onServer = (AuthenticationTokenSecretManager) ((RpcServer) cluster
      .getRegionServer(0).getRpcServer()).getSecretManager();
    UTIL.waitFor(60000, () -> onMaster.isMaster() || onServer.isMaster());
    AuthenticationTokenSecretManager leader = onMaster.isMaster() ? onMaster : onServer;
    AuthenticationTokenSecretManager other = leader == onMaster ? onServer : onMaster;
    UTIL.waitFor(60000, () -> leader.getCurrentKey() != null);

    Token<AuthenticationTokenIdentifier> token = leader.generateToken("causynth");
    AuthenticationTokenIdentifier identifier = token.decodeIdentifier();
    other.removeKey(identifier.getKeyId());
    assertArrayEquals(token.getPassword(), other.retrievePassword(identifier));
  }
}
