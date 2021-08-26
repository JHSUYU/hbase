/**
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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Factory method for creating store file tracker.
 */
@InterfaceAudience.Private
public final class StoreFileTrackerFactory {

  public static final String TRACK_IMPL = "hbase.store.file-tracker.impl";

  public static StoreFileTracker create(Configuration conf, boolean isPrimaryReplica,
    StoreContext ctx) {
    Class<? extends StoreFileTracker> tracker =
      conf.getClass(TRACK_IMPL, DefaultStoreFileTracker.class, StoreFileTracker.class);
    return ReflectionUtils.newInstance(tracker, conf, isPrimaryReplica, ctx);
  }
}