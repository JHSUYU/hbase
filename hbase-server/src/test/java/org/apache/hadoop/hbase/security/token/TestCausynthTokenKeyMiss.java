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
 * hbase-inj-05's native demonstration (a silent failure): the elected key master issues a token
 * under its first key, then rolls a new key, which the other server applies through its
 * ZKSecretWatcher within two seconds; the other server's entry for the token's key is then
 * dropped, as TestCausynthTokenKeys does to stand for a watch event not yet processed, and the
 * other server validates the token. The correct code refreshes the keys from ZooKeeper on the miss
 * and returns the token's password. With hbase-inj-05 injected the miss is answered with the
 * current (new) key: retrievePassword returns another password, without an exception, and the
 * assertion fails on every run.
 */
@Tag(SecurityTests.TAG)
@Tag(MediumTests.TAG)
public class TestCausynthTokenKeyMiss {

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
  public void theOtherServerValidatesATokenOfAnOlderKey() throws Exception {
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
    leader.rollCurrentKey();
    Thread.sleep(2000);
    other.removeKey(identifier.getKeyId());
    assertArrayEquals(token.getPassword(), other.retrievePassword(identifier));
  }
}
