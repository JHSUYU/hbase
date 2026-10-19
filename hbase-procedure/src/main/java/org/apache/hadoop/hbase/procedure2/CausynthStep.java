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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A procedure's step as GraphChecker work (with {@code causynth.cluster.automatic} set): handed,
 * when the procedure is made runnable ({@link AbstractProcedureScheduler#push}), from the thread
 * that made it so -- the request that submitted it, the step that yielded it or added it as a
 * child, the report that woke it -- to whichever worker runs it, as an executor's task is. A step
 * then stands on what made it runnable, in every run alike, not on the worker's turns: which
 * worker polls which procedure, and how often a worker's poll waited first, are the replay's
 * timing (a step's occurrence named by its worker's n-th poll wake was another step in a replay).
 */
@InterfaceAudience.Private
final class CausynthStep implements Runnable {
  private static final boolean AUTOMATIC = Boolean.getBoolean("causynth.cluster.automatic");
  private Runnable body;

  private CausynthStep() {
  }

  /** The procedure is made runnable on this thread: its next step is handed off from here. */
  static void handOff(Procedure<?> procedure) {
    if (!AUTOMATIC) {
      return;
    }
    CausynthStep step = new CausynthStep();
    procedure.causynthStep = step;
    execute(step);
  }

  /** The hand-off: GraphChecker files the step's start at this call. */
  private static void execute(Runnable step) {
  }

  /** A worker runs the procedure's step, standing on the hand-off that made it runnable. */
  static void run(Procedure<?> procedure, Runnable body) {
    CausynthStep step = procedure.causynthStep;
    if (step == null) {
      body.run();
      return;
    }
    procedure.causynthStep = null;
    step.body = body;
    step.run();
  }

  @Override
  public void run() {
    body.run();
  }
}
