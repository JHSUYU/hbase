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
package org.apache.hadoop.hbase.zookeeper;

import edu.uva.liftlab.graphchecker.runtime.MessageDoors;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A zookeeper that can handle 'recoverable' errors.
 * <p>
 * To handle recoverable errors, developers need to realize that there are two classes of requests:
 * idempotent and non-idempotent requests. Read requests and unconditional sets and deletes are
 * examples of idempotent requests, they can be reissued with the same results.
 * <p>
 * (Although, the delete may throw a NoNodeException on reissue its effect on the ZooKeeper state is
 * the same.) Non-idempotent requests need special handling, application and library writers need to
 * keep in mind that they may need to encode information in the data or name of znodes to detect
 * retries. A simple example is a create that uses a sequence flag. If a process issues a
 * create("/x-", ..., SEQUENCE) and gets a connection loss exception, that process will reissue
 * another create("/x-", ..., SEQUENCE) and get back x-111. When the process does a
 * getChildren("/"), it sees x-1,x-30,x-109,x-110,x-111, now it could be that x-109 was the result
 * of the previous create, so the process actually owns both x-109 and x-111. An easy way around
 * this is to use "x-process id-" when doing the create. If the process is using an id of 352,
 * before reissuing the create it will do a getChildren("/") and see "x-222-1", "x-542-30",
 * "x-352-109", x-333-110". The process will know that the original create succeeded an the znode it
 * created is "x-352-109".
 * @see "https://cwiki.apache.org/confluence/display/HADOOP2/ZooKeeper+ErrorHandling"
 */
@InterfaceAudience.Private
public class RecoverableZooKeeper {
  private static final Logger LOG = LoggerFactory.getLogger(RecoverableZooKeeper.class);
  /** GraphChecker's store transport: a znode version, written by one node, read by any. */
  public static final String CAUSYNTH_ZK = "HBASE_ZK";
  // the actual ZooKeeper client instance
  private ZooKeeper zk;
  private final RetryCounterFactory retryCounterFactory;
  // An identifier of this process in the cluster
  private final String identifier;
  private final byte[] id;
  private final Watcher watcher;
  private final int sessionTimeout;
  private final String quorumServers;
  private final int maxMultiSize;
  private final ZKClientConfig zkClientConfig;

  /**
   * See {@link #connect(Configuration, String, Watcher, String, ZKClientConfig)}.
   */
  public static RecoverableZooKeeper connect(Configuration conf, Watcher watcher)
    throws IOException {
    String ensemble = ZKConfig.getZKQuorumServersString(conf);
    return connect(conf, ensemble, watcher, null, null);
  }

  /**
   * Creates a new connection to ZooKeeper, pulling settings and ensemble config from the specified
   * configuration object using methods from {@link ZKConfig}. Sets the connection status monitoring
   * watcher to the specified watcher.
   * @param conf           configuration to pull ensemble and other settings from
   * @param watcher        watcher to monitor connection changes
   * @param ensemble       ZooKeeper servers quorum string
   * @param identifier     value used to identify this client instance.
   * @param zkClientConfig client specific configurations for this instance
   * @return connection to zookeeper
   * @throws IOException if unable to connect to zk or config problem
   */
  public static RecoverableZooKeeper connect(Configuration conf, String ensemble, Watcher watcher,
    final String identifier, ZKClientConfig zkClientConfig) throws IOException {
    if (ensemble == null) {
      throw new IOException("Unable to determine ZooKeeper ensemble");
    }
    int timeout = conf.getInt(HConstants.ZK_SESSION_TIMEOUT, HConstants.DEFAULT_ZK_SESSION_TIMEOUT);
    if (LOG.isTraceEnabled()) {
      LOG.trace("{} opening connection to ZooKeeper ensemble={}", identifier, ensemble);
    }
    int retry = conf.getInt("zookeeper.recovery.retry", 3);
    int retryIntervalMillis = conf.getInt("zookeeper.recovery.retry.intervalmill", 1000);
    int maxSleepTime = conf.getInt("zookeeper.recovery.retry.maxsleeptime", 60000);
    int multiMaxSize = conf.getInt("zookeeper.multi.max.size", 1024 * 1024);
    return new RecoverableZooKeeper(ensemble, timeout, watcher, retry, retryIntervalMillis,
      maxSleepTime, identifier, multiMaxSize, zkClientConfig);
  }

  RecoverableZooKeeper(String quorumServers, int sessionTimeout, Watcher watcher, int maxRetries,
    int retryIntervalMillis, int maxSleepTime, String identifier, int maxMultiSize,
    ZKClientConfig zkClientConfig) throws IOException {
    // TODO: Add support for zk 'chroot'; we don't add it to the quorumServers String as we should.
    this.retryCounterFactory =
      new RetryCounterFactory(maxRetries + 1, retryIntervalMillis, maxSleepTime);

    if (identifier == null || identifier.length() == 0) {
      // the identifier = processID@hostName
      identifier = ManagementFactory.getRuntimeMXBean().getName();
    }
    LOG.info("Process identifier={} connecting to ZooKeeper ensemble={}", identifier,
      quorumServers);
    this.identifier = identifier;
    this.id = Bytes.toBytes(identifier);

    this.watcher = watcher;
    this.sessionTimeout = sessionTimeout;
    this.quorumServers = quorumServers;
    this.maxMultiSize = maxMultiSize;
    this.zkClientConfig = zkClientConfig;
  }

  /**
   * Returns the maximum size (in bytes) that should be included in any single multi() call. NB:
   * This is an approximation, so there may be variance in the msg actually sent over the wire.
   * Please be sure to set this approximately, with respect to your ZK server configuration for
   * jute.maxbuffer.
   */
  public int getMaxMultiSizeLimit() {
    return maxMultiSize;
  }

  /**
   * Try to create a ZooKeeper connection. Turns any exception encountered into a
   * KeeperException.OperationTimeoutException so it can retried.
   * @return The created ZooKeeper connection object
   * @throws KeeperException if a ZooKeeper operation fails
   */
  private synchronized ZooKeeper checkZk() throws KeeperException {
    if (this.zk == null) {
      try {
        this.zk =
          new ZooKeeper(quorumServers, sessionTimeout, causynthWatch(watcher), zkClientConfig);
      } catch (IOException ex) {
        LOG.warn("Unable to create ZooKeeper Connection", ex);
        throw new KeeperException.OperationTimeoutException();
      }
    }
    return zk;
  }

  public synchronized void reconnectAfterExpiration()
    throws IOException, KeeperException, InterruptedException {
    if (zk != null) {
      LOG.info("Closing dead ZooKeeper connection, session" + " was: 0x"
        + Long.toHexString(zk.getSessionId()));
      zk.close();
      // reset the ZooKeeper connection
      zk = null;
    }
    checkZk();
    LOG.info("Recreated a ZooKeeper, session" + " is: 0x" + Long.toHexString(zk.getSessionId()));
  }

  /**
   * delete is an idempotent operation. Retry before throwing exception. This function will not
   * throw NoNodeException if the path does not exist.
   */
  public void delete(String path, int version) throws InterruptedException, KeeperException {
    long store = MessageDoors.storingChild(CAUSYNTH_ZK, path);
    try {
      deleteRetrying(path, version);
      causynthTombstone(path);
      causynthDeleted();
    } finally {
      causynthStored(store);
    }
  }

  private void deleteRetrying(String path, int version)
    throws InterruptedException, KeeperException {
    final Span span = TraceUtil.createSpan("RecoverableZookeeper.delete");
    try (Scope ignored = span.makeCurrent()) {
      RetryCounter retryCounter = retryCounterFactory.create();
      boolean isRetry = false; // False for first attempt, true for all retries.
      while (true) {
        try {
          checkZk().delete(path, version);
          span.setStatus(StatusCode.OK);
          return;
        } catch (KeeperException e) {
          switch (e.code()) {
            case NONODE:
              if (isRetry) {
                LOG.debug(
                  "Node " + path + " already deleted. Assuming a " + "previous attempt succeeded.");
                return;
              }
              LOG.debug("Node {} already deleted, retry={}", path, isRetry);
              TraceUtil.setError(span, e);
              throw e;

            case CONNECTIONLOSS:
            case OPERATIONTIMEOUT:
            case REQUESTTIMEOUT:
              TraceUtil.setError(span, e);
              retryOrThrow(retryCounter, e, "delete");
              break;

            default:
              TraceUtil.setError(span, e);
              throw e;
          }
        }
        retryCounter.sleepUntilNextRetry();
        isRetry = true;
      }
    } finally {
      span.end();
    }
  }

  /**
   * exists is an idempotent operation. Retry before throwing exception
   * @return A Stat instance
   */
  public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
    return exists(path, watcher, null);
  }

  private Stat exists(String path, Watcher watcher, Boolean watch)
    throws InterruptedException, KeeperException {
    causynthReading(path);
    Stat found = existsRetrying(path, watcher, watch);
    if (found != null || !causynthAbsent(path, "exists")) {
      causynthStoreRead(path, found == null ? -1 : found.getVersion(), "exists");
    }
    causynthWatched(path, causynthWatcher(watcher, watch));
    return found;
  }

  private Stat existsRetrying(String path, Watcher watcher, Boolean watch)
    throws InterruptedException, KeeperException {
    final Span span = TraceUtil.createSpan("RecoverableZookeeper.exists");
    try (Scope ignored = span.makeCurrent()) {
      RetryCounter retryCounter = retryCounterFactory.create();
      while (true) {
        try {
          Stat nodeStat;
          if (watch == null) {
            nodeStat = checkZk().exists(path, causynthWatch(watcher));
          } else {
            nodeStat = checkZk().exists(path, watch);
          }
          span.setStatus(StatusCode.OK);
          return nodeStat;
        } catch (KeeperException e) {
          switch (e.code()) {
            case CONNECTIONLOSS:
            case OPERATIONTIMEOUT:
            case REQUESTTIMEOUT:
              TraceUtil.setError(span, e);
              retryOrThrow(retryCounter, e, "exists");
              break;

            default:
              TraceUtil.setError(span, e);
              throw e;
          }
        }
        retryCounter.sleepUntilNextRetry();
      }
    } finally {
      span.end();
    }
  }

  /**
   * exists is an idempotent operation. Retry before throwing exception
   * @return A Stat instance
   */
  public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
    return exists(path, null, watch);
  }

  private void retryOrThrow(RetryCounter retryCounter, KeeperException e, String opName)
    throws KeeperException {
    if (!retryCounter.shouldRetry()) {
      LOG.error("ZooKeeper {} failed after {} attempts", opName, retryCounter.getMaxAttempts());
      throw e;
    }
    LOG.debug("Retry, connectivity issue (JVM Pause?); quorum={},exception{}=", quorumServers, e);
  }

  /**
   * getChildren is an idempotent operation. Retry before throwing exception
   * @return List of children znodes
   */
  public List<String> getChildren(String path, Watcher watcher)
    throws KeeperException, InterruptedException {
    return getChildren(path, watcher, null);
  }

  private List<String> getChildren(String path, Watcher watcher, Boolean watch)
    throws InterruptedException, KeeperException {
    causynthReading(path);
    List<String> children = getChildrenRetrying(path, watcher, watch);
    causynthStoreRead(path, -1, "getChildren");
    return children;
  }

  private List<String> getChildrenRetrying(String path, Watcher watcher, Boolean watch)
    throws InterruptedException, KeeperException {
    final Span span = TraceUtil.createSpan("RecoverableZookeeper.getChildren");
    try (Scope ignored = span.makeCurrent()) {
      RetryCounter retryCounter = retryCounterFactory.create();
      while (true) {
        try {
          List<String> children;
          if (watch == null) {
            children = checkZk().getChildren(path, causynthWatch(watcher));
          } else {
            children = checkZk().getChildren(path, watch);
          }
          span.setStatus(StatusCode.OK);
          return children;
        } catch (KeeperException e) {
          switch (e.code()) {
            case CONNECTIONLOSS:
            case OPERATIONTIMEOUT:
            case REQUESTTIMEOUT:
              TraceUtil.setError(span, e);
              retryOrThrow(retryCounter, e, "getChildren");
              break;

            default:
              TraceUtil.setError(span, e);
              throw e;
          }
        }
        retryCounter.sleepUntilNextRetry();
      }
    } finally {
      span.end();
    }
  }

  /**
   * getChildren is an idempotent operation. Retry before throwing exception
   * @return List of children znodes
   */
  public List<String> getChildren(String path, boolean watch)
    throws KeeperException, InterruptedException {
    return getChildren(path, null, watch);
  }

  /**
   * getData is an idempotent operation. Retry before throwing exception
   */
  public byte[] getData(String path, Watcher watcher, Stat stat)
    throws KeeperException, InterruptedException {
    return getData(path, watcher, null, stat);
  }

  private byte[] getData(String path, Watcher watcher, Boolean watch, Stat stat)
    throws InterruptedException, KeeperException {
    final Stat read = stat != null ? stat : new Stat();
    causynthReading(path);
    byte[] data;
    try {
      data = getDataRetrying(path, watcher, watch, read);
    } catch (KeeperException.NoNodeException absent) {
      causynthAbsent(path, "getData");
      throw absent;
    }
    causynthRead(path, read, data);
    try {
      causynthReplied();
    } catch (KeeperException lost) {
      causynthLose(path, causynthWatcher(watcher, watch));
      throw lost;
    }
    causynthWatched(path, causynthWatcher(watcher, watch));
    return data;
  }

  private byte[] getDataRetrying(String path, Watcher watcher, Boolean watch, Stat stat)
    throws InterruptedException, KeeperException {
    final Span span = TraceUtil.createSpan("RecoverableZookeeper.getData");
    try (Scope ignored = span.makeCurrent()) {
      RetryCounter retryCounter = retryCounterFactory.create();
      while (true) {
        try {
          byte[] revData;
          if (watch == null) {
            revData = checkZk().getData(path, causynthWatch(watcher), stat);
          } else {
            revData = checkZk().getData(path, watch, stat);
          }
          span.setStatus(StatusCode.OK);
          return ZKMetadata.removeMetaData(revData);
        } catch (KeeperException e) {
          switch (e.code()) {
            case CONNECTIONLOSS:
            case OPERATIONTIMEOUT:
            case REQUESTTIMEOUT:
              TraceUtil.setError(span, e);
              retryOrThrow(retryCounter, e, "getData");
              break;

            default:
              TraceUtil.setError(span, e);
              throw e;
          }
        }
        retryCounter.sleepUntilNextRetry();
      }
    } finally {
      span.end();
    }
  }

  /**
   * getData is an idempotent operation. Retry before throwing exception
   */
  public byte[] getData(String path, boolean watch, Stat stat)
    throws KeeperException, InterruptedException {
    return getData(path, null, watch, stat);
  }

  /**
   * setData is NOT an idempotent operation. Retry may cause BadVersion Exception Adding an
   * identifier field into the data to check whether badversion is caused by the result of previous
   * correctly setData
   * @return Stat instance
   */
  public Stat setData(String path, byte[] data, int version)
    throws KeeperException, InterruptedException {
    Stat written;
    long store = MessageDoors.storing(CAUSYNTH_ZK, path);
    try {
      written = setDataRetrying(path, data, version);
      causynthWrote(path, written.getVersion(), data);
    } finally {
      causynthStored(store);
    }
    return written;
  }

  private Stat setDataRetrying(String path, byte[] data, int version)
    throws KeeperException, InterruptedException {
    final Span span = TraceUtil.createSpan("RecoverableZookeeper.setData");
    try (Scope ignored = span.makeCurrent()) {
      RetryCounter retryCounter = retryCounterFactory.create();
      byte[] newData = ZKMetadata.appendMetaData(id, data);
      boolean isRetry = false;
      while (true) {
        try {
          span.setStatus(StatusCode.OK);
          return checkZk().setData(path, newData, version);
        } catch (KeeperException e) {
          switch (e.code()) {
            case CONNECTIONLOSS:
            case OPERATIONTIMEOUT:
            case REQUESTTIMEOUT:
              TraceUtil.setError(span, e);
              retryOrThrow(retryCounter, e, "setData");
              break;
            case BADVERSION:
              if (isRetry) {
                // try to verify whether the previous setData success or not
                try {
                  Stat stat = new Stat();
                  byte[] revData = checkZk().getData(path, false, stat);
                  if (Bytes.compareTo(revData, newData) == 0) {
                    // the bad version is caused by previous successful setData
                    return stat;
                  }
                } catch (KeeperException keeperException) {
                  // the ZK is not reliable at this moment. just throwing exception
                  TraceUtil.setError(span, e);
                  throw keeperException;
                }
              }
              // throw other exceptions and verified bad version exceptions
            default:
              TraceUtil.setError(span, e);
              throw e;
          }
        }
        retryCounter.sleepUntilNextRetry();
        isRetry = true;
      }
    } finally {
      span.end();
    }
  }

  /**
   * getAcl is an idempotent operation. Retry before throwing exception
   * @return list of ACLs
   */
  public List<ACL> getAcl(String path, Stat stat) throws KeeperException, InterruptedException {
    final Span span = TraceUtil.createSpan("RecoverableZookeeper.getAcl");
    try (Scope ignored = span.makeCurrent()) {
      RetryCounter retryCounter = retryCounterFactory.create();
      while (true) {
        try {
          span.setStatus(StatusCode.OK);
          return checkZk().getACL(path, stat);
        } catch (KeeperException e) {
          switch (e.code()) {
            case CONNECTIONLOSS:
            case OPERATIONTIMEOUT:
            case REQUESTTIMEOUT:
              TraceUtil.setError(span, e);
              retryOrThrow(retryCounter, e, "getAcl");
              break;

            default:
              TraceUtil.setError(span, e);
              throw e;
          }
        }
        retryCounter.sleepUntilNextRetry();
      }
    } finally {
      span.end();
    }
  }

  /**
   * setAcl is an idempotent operation. Retry before throwing exception
   * @return list of ACLs
   */
  public Stat setAcl(String path, List<ACL> acls, int version)
    throws KeeperException, InterruptedException {
    final Span span = TraceUtil.createSpan("RecoverableZookeeper.setAcl");
    try (Scope ignored = span.makeCurrent()) {
      RetryCounter retryCounter = retryCounterFactory.create();
      while (true) {
        try {
          span.setStatus(StatusCode.OK);
          return checkZk().setACL(path, acls, version);
        } catch (KeeperException e) {
          switch (e.code()) {
            case CONNECTIONLOSS:
            case OPERATIONTIMEOUT:
              TraceUtil.setError(span, e);
              retryOrThrow(retryCounter, e, "setAcl");
              break;

            default:
              TraceUtil.setError(span, e);
              throw e;
          }
        }
        retryCounter.sleepUntilNextRetry();
      }
    } finally {
      span.end();
    }
  }

  /**
   * <p>
   * NONSEQUENTIAL create is idempotent operation. Retry before throwing exceptions. But this
   * function will not throw the NodeExist exception back to the application.
   * </p>
   * <p>
   * But SEQUENTIAL is NOT idempotent operation. It is necessary to add identifier to the path to
   * verify, whether the previous one is successful or not.
   * </p>
   */
  public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
    throws KeeperException, InterruptedException {
    String created;
    long store = causynthStoring(path, createMode);
    try {
      created = createRetrying(path, data, acl, createMode);
      causynthWrote(created, 0, data);
    } finally {
      causynthStored(store);
    }
    return created;
  }

  private String createRetrying(String path, byte[] data, List<ACL> acl, CreateMode createMode)
    throws KeeperException, InterruptedException {
    final Span span = TraceUtil.createSpan("RecoverableZookeeper.create");
    try (Scope ignored = span.makeCurrent()) {
      byte[] newData = ZKMetadata.appendMetaData(id, data);
      switch (createMode) {
        case EPHEMERAL:
        case PERSISTENT:
          span.setStatus(StatusCode.OK);
          return createNonSequential(path, newData, acl, createMode);

        case EPHEMERAL_SEQUENTIAL:
        case PERSISTENT_SEQUENTIAL:
          span.setStatus(StatusCode.OK);
          return createSequential(path, newData, acl, createMode);

        default:
          final IllegalArgumentException e =
            new IllegalArgumentException("Unrecognized CreateMode: " + createMode);
          TraceUtil.setError(span, e);
          throw e;
      }
    } finally {
      span.end();
    }
  }

  private String createNonSequential(String path, byte[] data, List<ACL> acl, CreateMode createMode)
    throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    boolean isRetry = false; // False for first attempt, true for all retries.
    while (true) {
      try {
        return checkZk().create(path, data, acl, createMode);
      } catch (KeeperException e) {
        switch (e.code()) {
          case NODEEXISTS:
            if (isRetry) {
              // If the connection was lost, there is still a possibility that
              // we have successfully created the node at our previous attempt,
              // so we read the node and compare.
              byte[] currentData = checkZk().getData(path, false, null);
              if (currentData != null && Bytes.compareTo(currentData, data) == 0) {
                // We successfully created a non-sequential node
                return path;
              }
              LOG.error("Node " + path + " already exists with " + Bytes.toStringBinary(currentData)
                + ", could not write " + Bytes.toStringBinary(data));
              throw e;
            }
            LOG.trace("Node {} already exists", path);
            throw e;

          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
          case REQUESTTIMEOUT:
            retryOrThrow(retryCounter, e, "create");
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      isRetry = true;
    }
  }

  private String createSequential(String path, byte[] data, List<ACL> acl, CreateMode createMode)
    throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    boolean first = true;
    String newPath = path + this.identifier;
    while (true) {
      try {
        if (!first) {
          // Check if we succeeded on a previous attempt
          String previousResult = findPreviousSequentialNode(newPath);
          if (previousResult != null) {
            return previousResult;
          }
        }
        first = false;
        return checkZk().create(newPath, data, acl, createMode);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
          case REQUESTTIMEOUT:
            retryOrThrow(retryCounter, e, "create");
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
    }
  }

  /**
   * Convert Iterable of {@link org.apache.zookeeper.Op} we got into the ZooKeeper.Op instances to
   * actually pass to multi (need to do this in order to appendMetaData).
   */
  private Iterable<Op> prepareZKMulti(Iterable<Op> ops) throws UnsupportedOperationException {
    if (ops == null) {
      return null;
    }

    List<Op> preparedOps = new LinkedList<>();
    for (Op op : ops) {
      if (op.getType() == ZooDefs.OpCode.create) {
        CreateRequest create = (CreateRequest) op.toRequestRecord();
        preparedOps.add(Op.create(create.getPath(), ZKMetadata.appendMetaData(id, create.getData()),
          create.getAcl(), create.getFlags()));
      } else if (op.getType() == ZooDefs.OpCode.delete) {
        // no need to appendMetaData for delete
        preparedOps.add(op);
      } else if (op.getType() == ZooDefs.OpCode.setData) {
        SetDataRequest setData = (SetDataRequest) op.toRequestRecord();
        preparedOps.add(Op.setData(setData.getPath(),
          ZKMetadata.appendMetaData(id, setData.getData()), setData.getVersion()));
      } else {
        throw new UnsupportedOperationException("Unexpected ZKOp type: " + op.getClass().getName());
      }
    }
    return preparedOps;
  }

  /**
   * Run multiple operations in a transactional manner. Retry before throwing exception
   */
  public List<OpResult> multi(Iterable<Op> ops) throws KeeperException, InterruptedException {
    List<OpResult> results;
    List<Long> stores = new ArrayList<>();
    for (Op each : ops) {
      stores.add(causynthStoring(each));
    }
    try {
      results = multiRetrying(ops);
      causynthWrote(ops, results);
    } finally {
      for (long store : stores) {
        causynthStored(store);
      }
    }
    return results;
  }

  private List<OpResult> multiRetrying(Iterable<Op> ops)
    throws KeeperException, InterruptedException {
    final Span span = TraceUtil.createSpan("RecoverableZookeeper.multi");
    try (Scope ignored = span.makeCurrent()) {
      RetryCounter retryCounter = retryCounterFactory.create();
      Iterable<Op> multiOps = prepareZKMulti(ops);
      while (true) {
        try {
          span.setStatus(StatusCode.OK);
          return checkZk().multi(multiOps);
        } catch (KeeperException e) {
          switch (e.code()) {
            case CONNECTIONLOSS:
            case OPERATIONTIMEOUT:
            case REQUESTTIMEOUT:
              TraceUtil.setError(span, e);
              retryOrThrow(retryCounter, e, "multi");
              break;

            default:
              TraceUtil.setError(span, e);
              throw e;
          }
        }
        retryCounter.sleepUntilNextRetry();
      }
    } finally {
      span.end();
    }
  }

  private String findPreviousSequentialNode(String path)
    throws KeeperException, InterruptedException {
    int lastSlashIdx = path.lastIndexOf('/');
    assert (lastSlashIdx != -1);
    String parent = path.substring(0, lastSlashIdx);
    String nodePrefix = path.substring(lastSlashIdx + 1);
    List<String> nodes = checkZk().getChildren(parent, false);
    List<String> matching = filterByPrefix(nodes, nodePrefix);
    for (String node : matching) {
      String nodePath = parent + "/" + node;
      Stat stat = checkZk().exists(nodePath, false);
      if (stat != null) {
        return nodePath;
      }
    }
    return null;
  }

  public synchronized long getSessionId() {
    return zk == null ? -1 : zk.getSessionId();
  }

  public synchronized void close() throws InterruptedException {
    if (zk != null) {
      zk.close();
    }
  }

  public synchronized States getState() {
    return zk == null ? null : zk.getState();
  }

  /**
   * Returns the wrapped ZooKeeper client. If the wrapped client hasn't been created yet then tries
   * create it first.
   * @return the wrapped ZK client of null if the creation has failed.
   */
  public synchronized ZooKeeper getZooKeeper() {
    // Callers expect an initialized ZooKeeper instance
    // Pre HBASE-28529 the constructor used to call checkZk()
    try {
      return checkZk();
    } catch (Exception x) {
      LOG.warn("Failed to initialize Zookeeper object, returning null", x);
      return null;
    }
  }

  public void sync(String path, AsyncCallback.VoidCallback cb, Object ctx) throws KeeperException {
    checkZk().sync(path, cb, ctx);
  }

  /**
   * Filters the given node list by the given prefixes. This method is all-inclusive--if any element
   * in the node list starts with any of the given prefixes, then it is included in the result.
   * @param nodes    the nodes to filter
   * @param prefixes the prefixes to include in the result
   * @return list of every element that starts with one of the prefixes
   */
  private static List<String> filterByPrefix(List<String> nodes, String... prefixes) {
    List<String> lockChildren = new ArrayList<>();
    for (String child : nodes) {
      for (String prefix : prefixes) {
        if (child.startsWith(prefix)) {
          lockChildren.add(child);
          break;
        }
      }
    }
    return lockChildren;
  }

  public String getIdentifier() {
    return identifier;
  }

  /**
   * GraphChecker: per watcher, the paths whose data read's reply a fault lost after ZooKeeper
   * applied it ({@link #causynthReplied}): the server fires that watch, and a client that never got
   * the reply has none registered for it and drops the event ({@link #causynthDropped}); a later
   * data read of the path whose reply arrives registers it again ({@link #causynthWatched}).
   */
  private final Map<Watcher, Set<String>> causynthLost =
    Collections.synchronizedMap(new IdentityHashMap<>());

  /** A write of {@code data} made this version: what the store carries. */
  private void causynthWrote(String path, int version, byte[] data) {
    MessageDoors.stored(CAUSYNTH_ZK, path + "@" + version, this, data);
  }

  /** Each create and set of a multi made its version, filed once the multi returned. */
  private void causynthWrote(Iterable<Op> ops, List<OpResult> results) {
    Iterator<Op> op = ops.iterator();
    for (OpResult result : results) {
      Op each = op.hasNext() ? op.next() : null;
      if (result instanceof OpResult.CreateResult) {
        causynthWrote(((OpResult.CreateResult) result).getPath(), 0, causynthData(each));
      } else if (result instanceof OpResult.SetDataResult && each != null) {
        causynthWrote(each.getPath(), ((OpResult.SetDataResult) result).getStat().getVersion(),
          causynthData(each));
      } else if (result instanceof OpResult.DeleteResult && each != null) {
        causynthTombstone(each.getPath());
      }
    }
  }

  /** Where a delete returns to its caller, after the retries: its write door's place. */
  private static void causynthDeleted() {
  }

  /** A delete ended the path's life: the version every later absent read of it reads. */
  private void causynthTombstone(String path) {
    MessageDoors.deleted(CAUSYNTH_ZK, path, this);
  }

  /** A read found no node: a read of the path's tombstone, if a delete filed one. */
  private boolean causynthAbsent(String path, String op) {
    return MessageDoors.absent(CAUSYNTH_ZK, path, op, this);
  }

  /**
   * GraphChecker: a watch callback's listing of {@code parent}'s children names the newest child
   * create it was not yet delivered as its cause, as a data read names the version it read.
   */
  public void causynthChildren(String parent, List<String> children) {
    MessageDoors.childrenFetched(CAUSYNTH_ZK, parent, children, this);
  }

  /**
   * Where a data read's reply reaches its caller, after the retries: the read's door is woven
   * before it, so a fault there is the reply lost -- ZooKeeper applied the read, and a client
   * registers a watch only with a delivered reply.
   */
  private static void causynthReplied() throws KeeperException {
  }

  /**
   * A write's turn at the path it writes, closed by {@link #causynthStored} in a finally around
   * the write and its {@link #causynthWrote}; a sequential create takes the next number of its
   * parent, and a delete moves the parent's child counter: theirs is the parent's.
   */
  private static long causynthStoring(String path, CreateMode mode) {
    return mode.isSequential()
      ? MessageDoors.storingChild(CAUSYNTH_ZK, path)
      : MessageDoors.storing(CAUSYNTH_ZK, path);
  }

  private static long causynthStoring(Op op) {
    if (op.getType() == ZooDefs.OpCode.delete) {
      return MessageDoors.storingChild(CAUSYNTH_ZK, op.getPath());
    }
    org.apache.jute.Record request = op.toRequestRecord();
    if (request instanceof CreateRequest) {
      return causynthStoring(op.getPath(),
        CreateMode.fromFlag(((CreateRequest) request).getFlags(), CreateMode.PERSISTENT));
    }
    return MessageDoors.storing(CAUSYNTH_ZK, op.getPath());
  }

  /** What a multi's create or set writes, or null. */
  private static byte[] causynthData(Op op) {
    if (op == null) {
      return null;
    }
    org.apache.jute.Record request = op.toRequestRecord();
    return request instanceof CreateRequest ? ((CreateRequest) request).getData()
      : request instanceof SetDataRequest ? ((SetDataRequest) request).getData()
      : null;
  }

  private static void causynthStored(long store) {
    MessageDoors.storeEnded(CAUSYNTH_ZK, store);
  }

  /** A read's turn at the path it reads (a getChildren's: the parent), before the retries. */
  private static void causynthReading(String path) {
    MessageDoors.reading(CAUSYNTH_ZK, path);
  }

  /** A data read: the version it read is the message its write sent. */
  private void causynthRead(String path, Stat stat, byte[] data) {
    String key = path + "@" + stat.getVersion();
    if (data != null) {
      CAUSYNTH_FETCHED.put(data, key);
    }
    MessageDoors.fetched(CAUSYNTH_ZK, key, this, data);
  }

  /** The version each array a data read returned is, by the array's identity, while it lives. */
  private static final Map<byte[], String> CAUSYNTH_FETCHED =
    Collections.synchronizedMap(new WeakHashMap<>());

  /**
   * GraphChecker: a reader parsed a znode's bytes, as a data read returned them, into the object
   * it keeps: that object reads the version the bytes are ({@code MessageDoors#decoded}).
   */
  public static void causynthDecoded(byte[] data, Object product) {
    String key = data == null || product == null ? null : CAUSYNTH_FETCHED.get(data);
    if (key != null) {
      MessageDoors.decoded(CAUSYNTH_ZK, key, product);
    }
  }

  /** An exists or getChildren read: the version it returned, -1 for no node or none stated. */
  private void causynthStoreRead(String path, int version, String op) {
    MessageDoors.storeRead(CAUSYNTH_ZK, path + "@" + version, op, this);
  }

  /** The watcher a read registers: its own, the client's default one, or none. */
  private Watcher causynthWatcher(Watcher watcher, Boolean watch) {
    return watch == null ? watcher : watch ? this.watcher : null;
  }

  private void causynthLose(String path, Watcher watcher) {
    if (watcher != null) {
      causynthLost.computeIfAbsent(watcher, w -> ConcurrentHashMap.newKeySet()).add(path);
    }
  }

  /** A data read (getData, exists) whose reply arrived registers the data watch again. */
  private void causynthWatched(String path, Watcher watcher) {
    Set<String> lost = watcher == null ? null : causynthLost.get(watcher);
    if (lost != null) {
      lost.remove(path);
    }
  }

  /** Whether the event is the data event a lost reply's unregistered watch drops. */
  private boolean causynthDropped(Watcher watcher, WatchedEvent event) {
    if (
      event.getPath() == null || event.getType() == Watcher.Event.EventType.NodeChildrenChanged
    ) {
      return false;
    }
    Set<String> lost = causynthLost.get(watcher);
    return lost != null && lost.remove(event.getPath());
  }

  /** Every watcher this client hands ZooKeeper, so a lost reply's event can be dropped. */
  private Watcher causynthWatch(Watcher watcher) {
    return watcher == null || watcher instanceof CausynthWatch
      ? watcher
      : new CausynthWatch(watcher);
  }

  /** A watcher as ZooKeeper holds it: equal to another of the same watcher. */
  private final class CausynthWatch implements Watcher {
    private final Watcher watcher;

    private CausynthWatch(Watcher watcher) {
      this.watcher = watcher;
    }

    @Override
    public void process(WatchedEvent event) {
      if (!causynthDropped(watcher, event)) {
        watcher.process(event);
      }
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof CausynthWatch && watcher.equals(((CausynthWatch) other).watcher);
    }

    @Override
    public int hashCode() {
      return watcher.hashCode();
    }
  }
}
