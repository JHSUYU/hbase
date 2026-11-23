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
package org.apache.hadoop.hbase.procedure2;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A procedure's step as Causym work (with {@code causym.cluster.automatic} set), its start
 * filed at each {@code HANDS.execute}. The first step is handed off where the procedure is first
 * made runnable ({@link AbstractProcedureScheduler#push}):
 * the request that submitted it, or the parent step that added it as a child. Every later step is
 * handed off at the end of the step before it, inside that step's claim, on its worker and under
 * the procedure's execution lock, whatever later makes it runnable again -- a yield, the report or RPC reply that
 * wakes it (ProcedureFutureUtil's callback on a Netty thread), a timeout, its last child. A step
 * then stands on its procedure's previous step, in every run alike: neither on a worker's n-th poll
 * wake, nor on which reply's exchange or which timeout thread's turn happened to wake it. A step is
 * handed to a worker of its own (a {@link SynchronousQueue} pool's hand-off, which runs nothing):
 * held where it runs, it holds that worker alone, the scheduler's other procedures going to the
 * others, so a confirmation may order it in place. A step that finishes its procedure ends with one
 * more hand-off, never claimed, made after the step released the procedure's locks: the finish and
 * the release make no event of their own, so without it a replay could let the next procedure's
 * step run into a lock the recording had seen released (hbase-adhoc-13: the second merge met the
 * catalog janitor's collection still holding the region's lock, and its check ran a step later).
 */
@InterfaceAudience.Private
final class CausymStep implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(CausymStep.class);
  private static final boolean AUTOMATIC = Boolean.getBoolean("causym.cluster.automatic");
  private static final CausymStep RUNNING = new CausymStep();
  private static final CausymStep FINISHED = new CausymStep();
  private static final ThreadPoolExecutor HANDS =
    new ThreadPoolExecutor(0, 1, 1, TimeUnit.SECONDS, new SynchronousQueue<>()) {
      @Override
      public void execute(Runnable step) {
      }
    };
  private Procedure<?> procedure;
  private Runnable body;

  private CausymStep() {
  }

  /** The procedure is made runnable on this thread: its first step is handed off from here. */
  static void handOff(Procedure<?> procedure) {
    if (!AUTOMATIC) {
      return;
    }
    CausymStep step;
    synchronized (CausymStep.class) {
      if (procedure.causymStep != null) {
        return;
      }
      step = new CausymStep();
      procedure.causymStep = step;
    }
    HANDS.execute(step);
  }

  /**
   * A worker runs the procedure's step, standing on the hand-off that made it runnable; a step that
   * leaves the procedure unfinished hands off the next one.
   */
  static void run(Procedure<?> procedure, Runnable body) {
    CausymStep step;
    synchronized (CausymStep.class) {
      step = procedure.causymStep;
      if (step != null && step != RUNNING) {
        procedure.causymStep = RUNNING;
      }
    }
    if (step == null || step == RUNNING) {
      if (AUTOMATIC) {
        unclaimed(procedure, step);
      }
      body.run();
      return;
    }
    step.procedure = procedure;
    step.body = body;
    step.run();
  }

  private static void unclaimed(Procedure<?> procedure, CausymStep step) {
    LOG.warn("CAUSYM LINEAGE_LOST: a step of {} ran with {} hand-off, on its worker's own context",
      procedure, step == null ? "no" : "a running");
  }

  @Override
  public void run() {
    try {
      body.run();
    } finally {
      boolean finished = procedure.isFinished();
      CausymStep next = null;
      synchronized (CausymStep.class) {
        if (finished) {
          procedure.causymStep = null;
        } else {
          next = new CausymStep();
          procedure.causymStep = next;
        }
      }
      if (next != null) {
        HANDS.execute(next);
      }
      if (finished) {
        HANDS.execute(FINISHED);
      }
    }
  }
}
