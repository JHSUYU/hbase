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
package org.apache.hadoop.hbase;

import edu.uva.liftlab.graphchecker.runtime.MessageDoors;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;

/**
 * What makes an HBase mini cluster a GraphChecker workload, with {@link #AUTOMATIC} set: any
 * cluster test is one as it is written.
 * <p>
 * Every node is registered whole as the node it is ({@link MessageDoors#registerNode}): each
 * HMaster and HRegionServer with the objects it runs its work through, the mini cluster's NameNode
 * and DataNodes, and its ZooKeeper servers, named {@code auto0/<role><i>} in the order they
 * registered; a master or region server the test starts later is registered as it is added, before
 * it starts. The window opens once the cluster is up, with one request of the test on the thread
 * that started it and on the test's client connection, and closes when the test shuts the cluster
 * down: the teardown is not recorded.
 * <p>
 * Nothing the cluster sets up expires on the wall clock ({@link #configure}): the recording runs on
 * a JIT and every replay on an interpreter many times slower, so an RPC, a session, an idle
 * connection or a test's own wait that the recording finished in time would time out in a replay.
 */
public final class CausynthHBaseCluster {
  public static final String AUTOMATIC = "causynth.cluster.automatic";
  private static final String NODE = "CLUSTER_NODE";
  private static final String PREFIX = "auto0/";
  /** Longer than any case runs: an expiry that cannot fire. */
  private static final int NEVER_MS = (int) TimeUnit.HOURS.toMillis(6);
  /** How many times longer a test's own waits ({@code Waiter}) may take. */
  private static final float WAIT_RATIO = 100f;

  /** The nodes registered so far, by identity, and how many of each role. */
  private static final Map<Object, String> REGISTERED = new IdentityHashMap<>();
  private static final Map<String, Integer> COUNTS = new HashMap<>();
  private static Object windowClient;
  private static long windowRequest;

  private CausynthHBaseCluster() {
  }

  public static boolean automatic() {
    return Boolean.getBoolean(AUTOMATIC);
  }

  /**
   * Before the cluster starts, on the configuration it is built from: no RPC, socket, session or
   * idle connection of HBase, of its ZooKeeper or of its HDFS times out, and a test's own waits are
   * as many times longer as a replay is slower.
   */
  public static void configure(Configuration conf) {
    for (String key : new String[] { HConstants.HBASE_RPC_TIMEOUT_KEY,
      HConstants.HBASE_RPC_READ_TIMEOUT_KEY, HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY,
      HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY, HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
      HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT,
      HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, HConstants.ZK_SESSION_TIMEOUT,
      "hbase.zookeeper.property.maxSessionTimeout", "hbase.ipc.client.socket.timeout.connect",
      "hbase.ipc.client.socket.timeout.read", "hbase.ipc.client.socket.timeout.write",
      "hbase.ipc.client.connection.maxidletime",
      "hbase.ipc.client.connection.minIdleTimeBeforeClose",
      // the mini cluster's HDFS, which the WALs and store files are written to
      "ipc.client.connection.maxidletime", "ipc.client.rpc-timeout.ms",
      "dfs.client.socket-timeout", "dfs.datanode.socket.write.timeout",
      "dfs.client.socketcache.expiryMsec" }) {
      conf.setInt(key, NEVER_MS);
    }
    conf.setFloat(Waiter.HBASE_TEST_WAIT_FOR_RATIO, WAIT_RATIO);
  }

  /**
   * The cluster is up: every node registered, and the window opened on the test's thread and
   * client connection.
   */
  public static synchronized void registerAutomatically(HBaseTestingUtil util) throws IOException {
    MiniDFSCluster dfs = util.getDFSCluster();
    if (dfs != null) {
      registerHdfs(dfs);
    }
    MiniZooKeeperCluster zk = util.getZkCluster();
    if (zk != null) {
      registerZooKeeper(zk);
    }
    SingleProcessHBaseCluster cluster = util.getMiniHBaseCluster();
    for (JVMClusterUtil.MasterThread master : cluster.getMasterThreads()) {
      registerMaster(master.getMaster());
    }
    for (JVMClusterUtil.RegionServerThread server : cluster.getRegionServerThreads()) {
      registerRegionServer(server.getRegionServer());
    }
    if (windowClient == null) {
      AsyncClusterConnection client = util.getAsyncConnection();
      MessageDoors.registerNode(client, "EXTERNAL_APP", "HBASE_CLIENT", PREFIX + "client0",
        client.getRpcClient());
      windowClient = client;
      MessageDoors.startRecording();
      windowRequest = MessageDoors.beginRequest(client, "test");
    }
  }

  /** The test shuts its cluster down: its request ends, and so does the window. */
  public static synchronized void shutdownAutomatically() {
    if (windowClient != null) {
      MessageDoors.endRequest(windowRequest, "test");
      MessageDoors.stopRecording();
      windowClient = null;
    }
  }

  /**
   * A master as its own node (its own clock): its RPC server and services, its procedure executor
   * and the remote dispatcher its procedures reach region servers through, the managers it
   * assigns and splits through, its ZooKeeper watcher and its connection to the cluster. A master
   * not yet active has none of the active ones' components: its threads are still found through
   * the master itself.
   */
  public static synchronized void registerMaster(HMaster master) {
    if (REGISTERED.containsKey(master)) {
      return;
    }
    List<Object> aliases = serverAliases(master.getRpcServer(), master.getZooKeeper(),
      master.getAsyncClusterConnection(), master.getExecutorService());
    aliases.add(master.getMasterRpcServices());
    ProcedureExecutor<MasterProcedureEnv> procedures = master.getMasterProcedureExecutor();
    if (procedures != null) {
      aliases.add(procedures);
      aliases.add(procedures.getEnvironment());
      aliases.add(procedures.getEnvironment().getRemoteDispatcher());
    }
    aliases.add(master.getAssignmentManager());
    aliases.add(master.getServerManager());
    aliases.add(master.getSplitWALManager());
    aliases.add(master.getMasterWalManager());
    register(master, "HMASTER", "master", aliases);
  }

  /**
   * A region server as its own node: its RPC server and services, its executor (the handlers of
   * the procedures a master dispatches to it), its ZooKeeper watcher and its connection to the
   * cluster -- the one its procedure reports go to the master on.
   */
  public static synchronized void registerRegionServer(HRegionServer server) {
    if (REGISTERED.containsKey(server)) {
      return;
    }
    List<Object> aliases = serverAliases(server.getRpcServer(), server.getZooKeeper(),
      server.getAsyncClusterConnection(), server.getExecutorService());
    aliases.add(server.getRSRpcServices());
    aliases.add(server.getWalFactory());
    register(server, "REGIONSERVER", "rs", aliases);
  }

  /** Whether the window is open: a node the test adds now is registered as it is added. */
  public static synchronized boolean windowOpen() {
    return windowClient != null;
  }

  private static List<Object> serverAliases(Object rpcServer, Object zooKeeper,
    AsyncClusterConnection connection, Object executor) {
    List<Object> aliases = new ArrayList<>();
    aliases.add(rpcServer);
    aliases.add(zooKeeper);
    aliases.add(connection);
    if (connection != null) {
      aliases.add(connection.getRpcClient());
    }
    aliases.add(executor);
    return aliases;
  }

  /** The NameNode and each DataNode of the mini cluster's HDFS. */
  private static void registerHdfs(MiniDFSCluster dfs) {
    for (int index = 0; index < dfs.getNumNameNodes(); index++) {
      if (dfs.getNameNode(index) == null || REGISTERED.containsKey(dfs.getNameNode(index))) {
        continue;
      }
      NameNodeRpcServer rpc = (NameNodeRpcServer) dfs.getNameNodeRpc(index);
      List<Object> aliases = new ArrayList<>();
      aliases.add(rpc);
      aliases.add(rpc.getClientRpcServer());
      aliases.add(dfs.getNamesystem(index));
      aliases.add(dfs.getNamesystem(index).getBlockManager());
      register(dfs.getNameNode(index), "NAMENODE", "nn", aliases);
    }
    for (DataNode datanode : dfs.getDataNodes()) {
      if (REGISTERED.containsKey(datanode)) {
        continue;
      }
      List<Object> aliases = new ArrayList<>();
      aliases.add(datanode.ipcServer);
      aliases.add(datanode.getXferServer());
      aliases.add(datanode.getSaslServer());
      register(datanode, "DATANODE", "dn", aliases);
    }
  }

  /** Each ZooKeeper server of the mini cluster, which the class keeps private. */
  private static void registerZooKeeper(MiniZooKeeperCluster zk) {
    if (REGISTERED.containsKey(zk)) {
      return;
    }
    List<Object> aliases = new ArrayList<>();
    try {
      Field servers = MiniZooKeeperCluster.class.getDeclaredField("zooKeeperServers");
      servers.setAccessible(true);
      aliases.addAll((List<?>) servers.get(zk));
    } catch (ReflectiveOperationException | RuntimeException unreadable) {
      // the cluster object alone, then
    }
    register(zk, "ZOOKEEPER", "zk", aliases);
  }

  private static void register(Object anchor, String role, String name, List<Object> aliases) {
    int index = COUNTS.merge(name, 1, Integer::sum) - 1;
    String sourceId = PREFIX + name + index;
    REGISTERED.put(anchor, sourceId);
    MessageDoors.registerNode(anchor, NODE, role, sourceId, aliases.toArray());
  }
}
