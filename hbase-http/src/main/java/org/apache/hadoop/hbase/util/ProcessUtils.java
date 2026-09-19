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
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process related utilities.
 */
@InterfaceAudience.Private
public final class ProcessUtils {
  private static Logger LOG = LoggerFactory.getLogger(ProcessUtils.class);

  private ProcessUtils() {
  }

  public static Integer getPid() {
    // JVM_PID is exported by bin/hbase run script
    String pidStr = System.getenv("JVM_PID");

    // in case if it is not set correctly used fallback from mxbean which is implementation specific
    if (pidStr == null || pidStr.trim().isEmpty()) {
      String name = ManagementFactory.getRuntimeMXBean().getName();
      if (name != null) {
        int idx = name.indexOf("@");
        if (idx != -1) {
          pidStr = name.substring(0, name.indexOf("@"));
        }
      }
    }
    try {
      if (pidStr != null) {
        return Integer.valueOf(pidStr);
      }
    } catch (NumberFormatException nfe) {
      // ignore
    }
    return null;
  }

  /**
   * Returns the current JVM process ID without trusting the potentially stale JVM_PID environment
   * variable. Java 8 does not provide ProcessHandle, so use the runtime MXBean name first, then
   * Linux's process-specific /proc/self link if the MXBean name has a different format.
   */
  public static int getCurrentPid() {
    String name = ManagementFactory.getRuntimeMXBean().getName();
    if (name != null) {
      int at = name.indexOf('@');
      if (at > 0) {
        try {
          return Integer.valueOf(name.substring(0, at));
        } catch (NumberFormatException ignored) {
          // Report an unrecognized JVM process ID below.
        }
      }
    }
    try {
      return Integer.parseInt(Files.readSymbolicLink(Paths.get("/proc/self")).toString());
    } catch (IOException | NumberFormatException | UnsupportedOperationException
      | SecurityException ignored) {
      // Report an unrecognized JVM process ID below.
    }
    throw new IllegalStateException("Unable to determine the current JVM process ID from " + name);
  }

  public static Process runCmdAsync(List<String> cmd) {
    try {
      LOG.info("Running command async: " + cmd);
      return new ProcessBuilder(cmd).inheritIO().start();
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }
}
