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
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import org.apache.hadoop.conf.Configuration;
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
 * {@code localhost} (CausynthHBaseCluster#configure). Unset elsewhere: a stock run reads its clock
 * and takes a free port.
 */
@InterfaceAudience.Private
public final class CausynthNodes {
  public static final String AUTOMATIC = "causynth.cluster.automatic";
  /** A node's start code, set on its own configuration when it is constructed. */
  public static final String STARTCODE = "hbase.causynth.startcode";
  private static final long FIRST_STARTCODE = 1_700_000_000_000L;
  private static int lastPort;
  private static int made;

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
