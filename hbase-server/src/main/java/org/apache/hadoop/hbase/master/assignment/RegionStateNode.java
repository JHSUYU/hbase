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
package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.DoNotRetryRegionException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionOfflineException;
import org.apache.hadoop.hbase.dryrun.DryRunManager;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Current Region State. Most fields are synchronized with meta region, i.e, we will update meta
 * immediately after we modify this RegionStateNode, and usually under the lock. The only exception
 * is {@link #lastHost}, which should not be used for critical condition.
 * <p/>
 * Typically, the only way to modify this class is through {@link TransitRegionStateProcedure}, and
 * we will record the TRSP along with this RegionStateNode to make sure that there could at most one
 * TRSP. For other operations, such as SCP, we will first get the lock, and then try to schedule a
 * TRSP. If there is already one, then the solution will be different:
 * <ul>
 * <li>For SCP, we will update the region state in meta to tell the TRSP to retry.</li>
 * <li>For DisableTableProcedure, as we have the xlock, we can make sure that the TRSP has not been
 * executed yet, so just unset it and attach a new one. The original one will quit immediately when
 * executing.</li>
 * <li>For split/merge, we will fail immediately as there is no actual operations yet so no
 * harm.</li>
 * <li>For EnableTableProcedure/TruncateTableProcedure, we can make sure that there will be no TRSP
 * attached with the RSNs.</li>
 * <li>For other procedures, you'd better use ReopenTableRegionsProcedure. The RTRP will take care
 * of lots of corner cases when reopening regions.</li>
 * </ul>
 * <p/>
 * Several fields are declared with {@code volatile}, which means you are free to get it without
 * lock, but usually you should not use these fields without locking for critical condition, as it
 * will be easily to introduce inconsistency. For example, you are free to dump the status and show
 * it on web without locking, but if you want to change the state of the RegionStateNode by checking
 * the current state, you'd better have the lock...
 */
@InterfaceAudience.Private
public class RegionStateNode implements Comparable<RegionStateNode> {
  public boolean isDryRun = false;
  private static final Logger LOG = LoggerFactory.getLogger(RegionStateNode.class);

  private static final class AssignmentProcedureEvent extends ProcedureEvent<RegionInfo> {
    public AssignmentProcedureEvent(final RegionInfo regionInfo) {
      super(regionInfo);
    }
  }

  final Lock lock = new ReentrantLock();
  private final RegionInfo regionInfo;
  private RegionInfo regionInfo$dryrun = null;
  private final ProcedureEvent<?> event;
  private final ConcurrentMap<RegionInfo, RegionStateNode> ritMap;
  private ConcurrentMap<RegionInfo, RegionStateNode> ritMap$dryrun;

  // volatile only for getLastUpdate and test usage, the upper layer should sync on the
  // RegionStateNode before accessing usually.
  private volatile TransitRegionStateProcedure procedure = null;
  private volatile TransitRegionStateProcedure procedure$dryrun = null;
  private volatile ServerName regionLocation = null;
  private volatile ServerName regionLocation$dryrun = null;
  // notice that, the lastHost will only be updated when a region is successfully CLOSED through
  // UnassignProcedure, so do not use it for critical condition as the data maybe stale and unsync
  // with the data in meta.
  private volatile ServerName lastHost = null;
  /**
   * A Region-in-Transition (RIT) moves through states. See {@link State} for complete list. A
   * Region that is opened moves from OFFLINE => OPENING => OPENED.
   */
  private volatile State state = State.OFFLINE;

  /**
   * Updated whenever a call to {@link #setRegionLocation(ServerName)} or
   * {@link #setState(RegionState.State, RegionState.State...)}.
   */
  private volatile long lastUpdate = 0;

  private volatile long lastUpdate$dryrun = 0;

  private volatile long openSeqNum = HConstants.NO_SEQNUM;

  RegionStateNode(RegionInfo regionInfo, ConcurrentMap<RegionInfo, RegionStateNode> ritMap) {
    this.regionInfo = regionInfo;
    this.event = new AssignmentProcedureEvent(regionInfo);
    this.ritMap = ritMap;
  }

  /**
   * @param update   new region state this node should be assigned.
   * @param expected current state should be in this given list of expected states
   * @return true, if current state is in expected list; otherwise false.
   */
  public boolean setState(final State update, final State... expected) {
    Exception e = new IOException();
    e.printStackTrace();;
    if (!isInState(expected)) {
      return false;
    }
    this.state = update;
    this.lastUpdate = EnvironmentEdgeManager.currentTime();
    return true;
  }

  /**
   * Put region into OFFLINE mode (set state and clear location).
   * @return Last recorded server deploy
   */
  public ServerName offline() {
    setState(State.OFFLINE);
    return setRegionLocation(null);
  }

  /**
   * Set new {@link State} but only if currently in <code>expected</code> State (if not, throw
   * {@link UnexpectedStateException}.
   */
  public void transitionState(final State update, final State... expected)
    throws UnexpectedStateException {
    if (!setState(update, expected)) {
      throw new UnexpectedStateException("Expected " + Arrays.toString(expected)
        + " so could move to " + update + " but current state=" + getState());
    }
  }

  /**
   * Notice that, we will return true if {@code expected} is empty.
   * <p/>
   * This is a bit strange but we need this logic, for example, we can change the state to OPENING
   * from any state, as in SCP we will not change the state to CLOSED before opening the region.
   */
  public boolean isInState(State... expected) {
    if (expected.length == 0) {
      return true;
    }
    return getState().matches(expected);
  }

  public boolean isStuck() {
    return isInState(State.FAILED_OPEN) && getProcedure() != null;
  }

  public boolean isInTransition() {
    return getProcedure() != null;
  }

  /**
   * Return whether the region has been split and not online.
   * <p/>
   * In this method we will test both region info and state, and will return true if either of the
   * test returns true. Please see the comments in
   * {@link AssignmentManager#markRegionAsSplit(RegionInfo, ServerName, RegionInfo, RegionInfo)} for
   * more details on why we need to test two conditions.
   */
  public boolean isSplit() {
    if(TraceUtil.isDryRun()){
      LOG.info("Failure Recovery, isSplit redirect to isSplit$instrumentation");
      return isSplit$instrumentation();
    }
    return regionInfo.isSplit() || isInState(State.SPLIT);
  }

  public boolean isSplit$instrumentation() {
    if(regionInfo$dryrun == null){
      regionInfo$dryrun = DryRunManager.clone(regionInfo);
    }
    return regionInfo$dryrun.isSplit() || isInState(State.SPLIT);
  }

  public long getLastUpdate() {
    if(TraceUtil.isDryRun()){
      LOG.info("Failure Recovery, getLastUpdate redirect to getLastUpdate$instrumentation");
      return getLastUpdate$instrumentation();
    }
    TransitRegionStateProcedure proc = this.procedure;
    if (proc != null) {
      long lastUpdate = proc.getLastUpdate();
      return lastUpdate != 0 ? lastUpdate : proc.getSubmittedTime();
    }
    return lastUpdate;
  }

  public long getLastUpdate$instrumentation() {
    if(procedure$dryrun == null){
      procedure$dryrun = DryRunManager.clone(procedure);
    }
    //state = new Object
    //state.field
    TransitRegionStateProcedure proc = this.procedure$dryrun;
    if (proc != null) {
      long lastUpdate = proc.getLastUpdate();
      return lastUpdate != 0 ? lastUpdate : proc.getSubmittedTime();
    }
    return lastUpdate;
  }

  public void setLastHost(final ServerName serverName) {
    this.lastHost = serverName;
  }

  public void setOpenSeqNum(final long seqId) {
    this.openSeqNum = seqId;
  }

  public ServerName setRegionLocation(final ServerName serverName) {
    if(TraceUtil.isDryRun()){
      LOG.info("Failure Recovery, setRegionLocation redirect to setRegionLocation$instrumentation");
      return setRegionLocation$instrumentation(serverName);
    }
    ServerName lastRegionLocation = this.regionLocation;
    if (LOG.isTraceEnabled() && serverName == null) {
      LOG.trace("Tracking when we are set to null " + this, new Throwable("TRACE"));
    }
    this.regionLocation = serverName;
    this.lastUpdate = EnvironmentEdgeManager.currentTime();
    return lastRegionLocation;
  }

  public ServerName setRegionLocation$instrumentation(final ServerName serverName) {
    if(regionLocation$dryrun == null){
      regionLocation$dryrun = DryRunManager.clone(regionLocation);
    }
    ServerName lastRegionLocation = this.regionLocation$dryrun;
    if (LOG.isTraceEnabled() && serverName == null) {
      LOG.trace("Tracking when we are set to null " + this, new Throwable("TRACE"));
    }
    this.regionLocation$dryrun = serverName;
    this.lastUpdate$dryrun = EnvironmentEdgeManager.currentTime();
    return lastRegionLocation;
  }

  public TransitRegionStateProcedure setProcedure(TransitRegionStateProcedure proc) {
    if(TraceUtil.isDryRun()) {
      LOG.info("Failure Recovery, setProcedure redirect to setProcedure$instrumentation");
      return setProcedure$instrumentation(proc);
    }
    assert this.procedure == null;
    this.procedure = proc;
    LOG.debug("Failure Recovery, ritMap.className is " + ritMap.getClass().getName());
    ritMap.put(regionInfo, this);
    return proc;
  }

  public TransitRegionStateProcedure setProcedure$instrumentation(TransitRegionStateProcedure proc) {

    assert this.procedure == null;
    if(procedure$dryrun == null){
      this.procedure$dryrun = DryRunManager.clone(this.procedure);
    }
    this.procedure$dryrun = proc;
    if(this.ritMap$dryrun == null){
      LOG.debug("Failure Recovery, this.ritMap.className is {}, this.ritMap.size is {}", this.ritMap.getClass().getName(), this.ritMap.size());
      this.ritMap$dryrun = DryRunManager.clone(this.ritMap);
      LOG.debug("Failure Recovery, this.ritMap.className is {}, this.ritMap.size is {}", this.ritMap$dryrun.getClass().getName(), this.ritMap$dryrun.size());
    }

    if(this.regionInfo$dryrun == null){
      this.regionInfo$dryrun = DryRunManager.clone(this.regionInfo);
    }
    ritMap$dryrun.put(regionInfo$dryrun, this);
    return proc;
  }

  public void unsetProcedure(TransitRegionStateProcedure proc) {
    if(TraceUtil.isDryRun()){
      LOG.info("Failure Recovery, unsetProcedure redirect to unsetProcedure$instrumentation");
      unsetProcedure$instrumentation(proc);
      return;
    }
    assert this.procedure == proc;
    this.procedure = null;
    ritMap.remove(regionInfo, this);
  }

  public void unsetProcedure$instrumentation(TransitRegionStateProcedure proc) {
    if(procedure$dryrun == null){
      procedure$dryrun = DryRunManager.clone(procedure);
    }
    assert this.procedure$dryrun == proc;
    this.procedure$dryrun = null;
    if(regionInfo$dryrun == null){
      regionInfo$dryrun = DryRunManager.clone(regionInfo);
    }
    if(ritMap$dryrun == null){
      ritMap$dryrun = DryRunManager.clone(ritMap);
    }
    ritMap$dryrun.remove(regionInfo$dryrun, this);
  }

  public TransitRegionStateProcedure getProcedure() {
    if(TraceUtil.isDryRun()){
      return getProcedure$instrumentation();
    }
    return procedure;
  }

  public TransitRegionStateProcedure getProcedure$instrumentation() {
    if(procedure$dryrun == null){
      procedure$dryrun = DryRunManager.clone(procedure);
    }
    return procedure$dryrun;
  }

  public ProcedureEvent<?> getProcedureEvent() {
    return event;
  }

  public RegionInfo getRegionInfo() {
    if(TraceUtil.isDryRun()){
      LOG.debug("Failure Recovery, getRegionInfo redirect to getRegionInfo$instrumentation");
      return getRegionInfo$instrumentation();
    }
    return regionInfo;
  }

  public RegionInfo getRegionInfo$instrumentation() {
    if(regionInfo$dryrun == null){
      regionInfo$dryrun = DryRunManager.clone(regionInfo);
    }
    return regionInfo$dryrun;
  }

  public TableName getTable() {
    return getRegionInfo().getTable();
  }

  public boolean isSystemTable() {
    return getTable().isSystemTable();
  }

  public ServerName getLastHost() {
    return lastHost;
  }

  public ServerName getRegionLocation() {
    if(TraceUtil.isDryRun() || isDryRun){
      return getRegionLocation$instrumentation();
    }
    return regionLocation;
  }

  public ServerName getRegionLocation$instrumentation() {
    if(regionLocation$dryrun == null){
      regionLocation$dryrun = DryRunManager.clone(regionLocation);
    }
    return regionLocation$dryrun;
  }

  public State getState() {
    return state;
  }

  public long getOpenSeqNum() {
    return openSeqNum;
  }

  public int getFormatVersion() {
    // we don't have any format for now
    // it should probably be in regionInfo.getFormatVersion()
    return 0;
  }

  public RegionState toRegionState() {
    return new RegionState(getRegionInfo(), getState(), getLastUpdate(), getRegionLocation());
  }

  @Override
  public int compareTo(final RegionStateNode other) {
    // NOTE: RegionInfo sort by table first, so we are relying on that.
    // we have a TestRegionState#testOrderedByTable() that check for that.
    return RegionInfo.COMPARATOR.compare(getRegionInfo(), other.getRegionInfo());
  }

  @Override
  public int hashCode() {
    return getRegionInfo().hashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof RegionStateNode)) {
      return false;
    }
    return compareTo((RegionStateNode) other) == 0;
  }

  @Override
  public String toString() {
    return toDescriptiveString();
  }

  public String toShortString() {
    // rit= is the current Region-In-Transition State -- see State enum.
    return String.format("state=%s, location=%s", getState(), getRegionLocation());
  }

  public String toDescriptiveString() {
    return String.format("%s, table=%s, region=%s", toShortString(), getTable(),
      getRegionInfo().getEncodedName());
  }

  public void checkOnline() throws DoNotRetryRegionException {
    RegionInfo ri = getRegionInfo();
    State s = state;
    if (s != State.OPEN) {
      throw new DoNotRetryRegionException(ri.getEncodedName() + " is not OPEN; state=" + s);
    }
    if (ri.isSplitParent()) {
      throw new DoNotRetryRegionException(
        ri.getEncodedName() + " is not online (splitParent=true)");
    }
    if (ri.isSplit()) {
      throw new DoNotRetryRegionException(ri.getEncodedName() + " has split=true");
    }
    if (ri.isOffline()) {
      // RegionOfflineException is not instance of DNRIOE so wrap it.
      throw new DoNotRetryRegionException(new RegionOfflineException(ri.getEncodedName()));
    }
  }

  public void lock() {
    lock.lock();
  }

  public boolean tryLock() {
    return lock.tryLock();
  }

  public void unlock() {
    lock.unlock();
  }
}
