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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The names an automatic cluster's masters and region servers get (GraphChecker's
 * {@code causynth.cluster.automatic} set on the JVM): the same in the recording and in every
 * replay, so the order the cluster keeps its servers in -- a {@code ServerName}'s host, port and
 * start code, which the master's online servers are sorted by and its assignment draws from -- is
 * the order the nodes were made in. Each node, as it is constructed ({@link JVMClusterUtil}),
 * takes a port above every one handed out before (the first at random: replays of one case share
 * a host) where its configuration asks for any free one, and a start code that is a function of
 * its place in that order, not of the wall clock (a restarted node is a later one); its host is
 * {@code localhost} (CausynthHBaseCluster#configure). A region server reports for duty only once
 * the one made before it has sent its first load report, the one that makes it a destination for
 * regions (ServerManager#createDestinationServersList, HBASE-25032), so the servers a placement
 * draws from are always the first ones made, in their order: hbase:meta is placed on the first
 * region server in every run. Unset elsewhere: a stock run reads its clock, takes a free port and
 * reports for duty at once.
 */
@InterfaceAudience.Private
public final class CausynthNodes {
  public static final String AUTOMATIC = "causynth.cluster.automatic";
  /** A node's start code, set on its own configuration when it is constructed. */
  public static final String STARTCODE = "hbase.causynth.startcode";
  /** A region server's place in the order region servers report in, set when it is made. */
  public static final String REGISTRATION = "hbase.causynth.registration";
  /** The start code of the first node made; each later one's is 1000 more than the one before. */
  public static final long FIRST_STARTCODE = 1_700_000_000_000L;
  private static final long REGISTRATION_WAIT_MINUTES = 5;
  private static final Object TURN = new Object();
  private static int lastPort;
  private static int made;
  private static int regionServers;
  private static int registered;

  private CausynthNodes() {
  }

  public static boolean automatic() {
    return Boolean.getBoolean(AUTOMATIC);
  }

  /** A node is about to be constructed from {@code conf}, its own copy, listening on {@code portKey}. */
  public static synchronized void naming(Configuration conf, String portKey) {
    if (!automatic()) {
      return;
    }
    if (conf.getInt(portKey, -1) == 0) {
      conf.setInt(portKey, nextPort());
    }
    conf.setLong(STARTCODE, FIRST_STARTCODE + 1000L * made++);
    if (HConstants.REGIONSERVER_PORT.equals(portKey)) {
      conf.setInt(REGISTRATION, regionServers++);
    }
  }

  /** A region server is about to report for duty: it waits until those made before it reported. */
  public static void awaitRegistration(Configuration conf) throws InterruptedIOException {
    int place = conf.getInt(REGISTRATION, -1);
    if (place < 0) {
      return;
    }
    long end = System.nanoTime() + TimeUnit.MINUTES.toNanos(REGISTRATION_WAIT_MINUTES);
    synchronized (TURN) {
      while (registered < place) {
        long left = TimeUnit.NANOSECONDS.toMillis(end - System.nanoTime());
        if (left <= 0) {
          return;
        }
        try {
          TURN.wait(left);
        } catch (InterruptedException e) {
          throw (InterruptedIOException) new InterruptedIOException().initCause(e);
        }
      }
    }
  }

  /** The master took a region server's load report: the next one may report for duty. */
  public static void reported(Configuration conf) {
    int place = conf.getInt(REGISTRATION, -1);
    if (place < 0) {
      return;
    }
    synchronized (TURN) {
      registered = Math.max(registered, place + 1);
      TURN.notifyAll();
    }
  }

  /** The start code of a node built from {@code conf}: its given one, else the clock's now. */
  public static long startcode(Configuration conf) {
    return conf.get(STARTCODE) == null
      ? EnvironmentEdgeManager.currentTime()
      : conf.getLong(STARTCODE, 0L);
  }

  private static int nextPort() {
    try {
      if (lastPort == 0) {
        try (ServerSocket socket = new ServerSocket(0)) {
          lastPort = socket.getLocalPort();
          return lastPort;
        }
      }
      for (int port = lastPort + 1; port < 65536; port++) {
        try (ServerSocket socket = new ServerSocket(port)) {
          socket.setReuseAddress(true);
          lastPort = port;
          return port;
        } catch (IOException taken) {
          // the next one
        }
      }
      throw new IOException("no free port above " + lastPort);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
