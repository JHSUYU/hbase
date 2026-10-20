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

import java.lang.management.ManagementFactory;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Default implementation of an environment edge.
 * <p>
 * GraphChecker's automatic mode ({@code causynth.cluster.automatic}): every run's clock starts at
 * the same instant, {@link #AUTOMATIC_BASE} at the JVM's start, so the times the recording and
 * every replay read lie on one timeline and differ by how fast each run went, not by when it ran
 * (hbase-bugs-master-active-time: sessions up to an hour later delivered active times up to an
 * hour later). The JVM's start is the runtime's own record of it, no clock reading of the run.
 * The base lies before any file the run writes, so no file's age is ever past a period that would
 * start work the run does not otherwise do.
 * </p>
 */
@InterfaceAudience.Private
public class DefaultEnvironmentEdge implements EnvironmentEdge {
  private static final boolean AUTOMATIC = Boolean.getBoolean("causynth.cluster.automatic");
  private static final long AUTOMATIC_BASE = 1_700_000_000_000L;
  private static final long JVM_START = ManagementFactory.getRuntimeMXBean().getStartTime();

  /** Returns whether GraphChecker's automatic mode ({@code causynth.cluster.automatic}) is on. */
  public static boolean automatic() {
    return AUTOMATIC;
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation returns {@link System#currentTimeMillis()}, in automatic mode counted from
   * the automatic base on.
   * </p>
   */
  @Override
  public long currentTime() {
    long now = System.currentTimeMillis();
    return AUTOMATIC ? AUTOMATIC_BASE + (now - JVM_START) : now;
  }
}
