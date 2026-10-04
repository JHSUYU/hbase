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
 * it starts. The window opens once the cluster is up, with one request of the test (client
 * {@code auto0/client0}, the testing utility and its client connection) on the thread that started
 * it, and closes when the test shuts the cluster down: the teardown is not recorded.
 * <p>
 * A case that declares its window at startup ({@link MessageDoors#startupWindow}: a bug whose fatal
 * value is set while the cluster starts, a master's activation time) has it open before the HBase
 * cluster starts, its HDFS and ZooKeeper up: those are registered then, its client connection once
 * there is one, and each master and region server is constructed in a node startup under the name
 * it is then registered by ({@link #localCluster}; {@code JVMClusterUtil#start} brackets its
 * thread's start too), so what they start or create belongs to the node from the first, not to the
 * test's request they run under.
 * <p>
 * Nothing the cluster sets up expires on the wall clock ({@link #configure}): the recording runs on
 * a JIT and every replay on an interpreter many times slower, so an RPC, a session, an idle
 * connection or a test's own wait that the recording finished in time would time out in a replay.
 */
public final class CausynthHBaseCluster {
  public static final String AUTOMATIC = "causynth.cluster.automatic";
  private static final String NODE = "CLUSTER_NODE";
  private static final String MASTER = "HMASTER";
  private static final String REGION_SERVER = "REGIONSERVER";
  private static final String PREFIX = "auto0/";
  /** Longer than any case runs: an expiry that cannot fire. */
  private static final int NEVER_MS = (int) TimeUnit.HOURS.toMillis(6);
  /** How many times longer a test's own waits ({@code Waiter}) may take. */
  private static final float WAIT_RATIO = 100f;

  /** The nodes registered so far, by identity, and how many of each role. */
  private static final Map<Object, String> REGISTERED = new IdentityHashMap<>();
  private static final Map<String, Integer> COUNTS = new HashMap<>();
  /** The test's client while the window is open, else null. */
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
      "dfs.client.socketcache.expiryMsec",
      // the local cluster's waits for an active, then an initialized master
      "hbase.master.start.timeout.localHBaseCluster",
      "hbase.master.init.timeout.localHBaseCluster" }) {
      conf.setInt(key, NEVER_MS);
    }
    conf.setFloat(Waiter.HBASE_TEST_WAIT_FOR_RATIO, WAIT_RATIO);
  }

  /**
   * The HBase cluster is about to start, its HDFS and ZooKeeper up: for a case that declares its
   * window at startup, they are registered and the window opened on the test's thread.
   */
  public static synchronized void openStartupWindow(HBaseTestingUtil util) {
    if (!MessageDoors.startupWindow() || windowClient != null) {
      return;
    }
    registerStores(util);
    openWindow(util);
  }

  /**
   * The cluster is up: every node registered, with the components it has made since it was
   * constructed, and the window opened on the test's thread unless the case opened it at startup;
   * the test's client connection is the test's client.
   */
  public static synchronized void registerAutomatically(HBaseTestingUtil util) throws IOException {
    registerStores(util);
    SingleProcessHBaseCluster cluster = util.getMiniHBaseCluster();
    for (JVMClusterUtil.MasterThread master : cluster.getMasterThreads()) {
      registerMaster(master.getMaster());
    }
    for (JVMClusterUtil.RegionServerThread server : cluster.getRegionServerThreads()) {
      registerRegionServer(server.getRegionServer());
    }
    AsyncClusterConnection connection = util.getAsyncConnection();
    if (windowClient == null) {
      openWindow(util);
    }
    MessageDoors.registerNode(util, "EXTERNAL_APP", "HBASE_CLIENT", PREFIX + "client0",
      connection, connection.getRpcClient());
  }

  /** The mini cluster's HDFS and ZooKeeper, whichever it has. */
  private static void registerStores(HBaseTestingUtil util) {
    MiniDFSCluster dfs = util.getDFSCluster();
    if (dfs != null) {
      registerHdfs(dfs);
    }
    MiniZooKeeperCluster zk = util.getZkCluster();
    if (zk != null) {
      registerZooKeeper(zk);
    }
  }

  /** The window opens with the test's request, on the thread that started the cluster. */
  private static void openWindow(HBaseTestingUtil util) {
    MessageDoors.registerNode(util, "EXTERNAL_APP", "HBASE_CLIENT", PREFIX + "client0");
    windowClient = util;
    MessageDoors.startRecording();
    windowRequest = MessageDoors.beginRequest(util, "test");
  }

  /**
   * The local cluster a mini cluster builds its masters and region servers in: while the window is
   * open, each is constructed in a node startup under the name it is then registered by, so what
   * its construction starts or creates is the node's from the first (named only once registered,
   * its ZooKeeper callbacks raced the registration and a replay ordered them otherwise).
   */
  public static LocalHBaseCluster localCluster(Configuration conf, int masters, int standByMasters,
    Class<? extends HMaster> masterClass, Class<? extends HRegionServer> regionServerClass)
    throws IOException {
    return new LocalHBaseCluster(conf, masters, standByMasters, 0, masterClass,
      regionServerClass) {
      @Override
      public JVMClusterUtil.MasterThread addMaster(Configuration c, int index) throws IOException {
        if (!windowOpen()) {
          return super.addMaster(c, index);
        }
        String sourceId = reserve(MASTER, "master");
        long startup = MessageDoors.beginNodeStartup(NODE, MASTER, sourceId);
        try {
          JVMClusterUtil.MasterThread thread = super.addMaster(c, index);
          named(thread.getMaster(), sourceId);
          registerMaster(thread.getMaster());
          return thread;
        } finally {
          MessageDoors.endNodeStartup(startup);
        }
      }

      @Override
      public JVMClusterUtil.RegionServerThread addRegionServer(Configuration c, int index)
        throws IOException {
        if (!windowOpen()) {
          return super.addRegionServer(c, index);
        }
        String sourceId = reserve(REGION_SERVER, "rs");
        long startup = MessageDoors.beginNodeStartup(NODE, REGION_SERVER, sourceId);
        try {
          JVMClusterUtil.RegionServerThread thread = super.addRegionServer(c, index);
          named(thread.getRegionServer(), sourceId);
          registerRegionServer(thread.getRegionServer());
          return thread;
        } finally {
          MessageDoors.endNodeStartup(startup);
        }
      }
    };
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
   * the master itself. Registered again, it keeps its name and adds what it has made since.
   */
  public static synchronized void registerMaster(HMaster master) {
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
    register(master, MASTER, "master", aliases);
  }

  /**
   * A region server as its own node: its RPC server and services, its executor (the handlers of
   * the procedures a master dispatches to it), its ZooKeeper watcher and its connection to the
   * cluster -- the one its procedure reports go to the master on. Registered again, it keeps its
   * name and adds what it has made since.
   */
  public static synchronized void registerRegionServer(HRegionServer server) {
    List<Object> aliases = serverAliases(server.getRpcServer(), server.getZooKeeper(),
      server.getAsyncClusterConnection(), server.getExecutorService());
    aliases.add(server.getRSRpcServices());
    aliases.add(server.getWalFactory());
    register(server, REGION_SERVER, "rs", aliases);
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

  /** A node under the name it was first given, else the next of its role. */
  private static synchronized void register(Object anchor, String role, String name,
    List<Object> aliases) {
    String sourceId = REGISTERED.get(anchor);
    if (sourceId == null) {
      sourceId = reserve(role, name);
      named(anchor, sourceId);
    }
    MessageDoors.registerNode(anchor, NODE, role, sourceId, aliases.toArray());
  }

  /** The next name of a role's nodes: {@code auto0/<name><i>}, in the order they are named. */
  private static synchronized String reserve(String role, String name) {
    return PREFIX + name + (COUNTS.merge(name, 1, Integer::sum) - 1);
  }

  private static synchronized void named(Object anchor, String sourceId) {
    REGISTERED.put(anchor, sourceId);
  }
}
