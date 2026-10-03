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
package org.apache.hadoop.hbase.ipc;

import edu.uva.liftlab.graphchecker.runtime.MessageDoors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ResponseHeader;

/**
 * HBase RPC on GraphChecker's message doors ({@link MessageDoors}).  What is HBase's own here: the
 * transport's name, its correlation id, and where the context travels -- three fields of the RPC
 * request header, two of the response header.
 * <p>
 * A request leaves on the thread that makes the call ({@link AbstractRpcClient}'s
 * {@code callMethod}), where the values the call's message was built with were staged, not on the
 * event loop that later writes it; the event loop only copies what the call carries into the
 * header ({@link IPCUtil#buildRequestHeader}). A request is served on the handler that runs it
 * ({@code CallRunner}), whose response header carries the handler's context ({@code ServerCall}).
 * A blocking call's response is installed on its caller when the call returns.
 */
@InterfaceAudience.Private
public final class CausynthRpc {
  public static final String TRANSPORT = "HBASE_RPC";
  /** An RPC is never retried under its own call id: a retry is a new call. */
  private static final String ATTEMPT = "0";
  private static final AtomicLong CLIENTS = new AtomicLong();

  private CausynthRpc() {
  }

  /** A client's own number: its calls' correlation ids are unique in the JVM. */
  static long nextClient() {
    return CLIENTS.incrementAndGet();
  }

  /**
   * A call leaves, on the caller's thread: the exchange the caller served is over, and the call
   * takes the caller's context and the values it staged.
   */
  static void leave(Call call, long client, Object owner) {
    MessageDoors.endInbound();
    call.causynthCorrelation = "hbase-rpc-" + client + "-" + call.id;
    MessageDoors.Outbound outbound = MessageDoors.outbound(TRANSPORT, call.causynthCorrelation,
      ATTEMPT, MessageDoors.REQUEST, owner, null, call.param);
    call.causynthTrace = outbound.trace;
    call.causynthSymbolic = outbound.symbolic;
  }

  /** What the call carries, into its request header. */
  static void header(RequestHeader.Builder header, Call call) {
    if (!call.causynthTrace.isEmpty() || !call.causynthSymbolic.isEmpty()) {
      header.setCausynthCorrelation(call.causynthCorrelation);
      header.setCausynthTrace(call.causynthTrace);
      header.setCausynthSymbolic(call.causynthSymbolic);
    }
  }

  /** A response arrived for the call: what its header carried, kept until the caller returns. */
  static void responded(Call call, ResponseHeader header) {
    call.causynthResponseTrace = header.getCausynthTrace();
    call.causynthResponseSymbolic = header.getCausynthSymbolic();
  }

  /** A blocking call returned: its response is installed on the caller. */
  static void returned(Call call, Object owner) {
    if (call.causynthResponseTrace.isEmpty() && call.causynthResponseSymbolic.isEmpty()) {
      return;
    }
    MessageDoors.inbound(call.causynthResponseTrace, call.causynthResponseSymbolic, TRANSPORT,
      call.causynthCorrelation, ATTEMPT, MessageDoors.RESPONSE, owner, false);
  }

  /** A handler serves a request: its sender's context, until {@link #served}. */
  public static MessageDoors.Inbound serve(RequestHeader header, Object server) {
    if (header == null || !header.hasCausynthCorrelation()) {
      return null;
    }
    return MessageDoors.inbound(header.getCausynthTrace(), header.getCausynthSymbolic(), TRANSPORT,
      header.getCausynthCorrelation(), ATTEMPT, MessageDoors.REQUEST, server, false);
  }

  /** The handler is done with the request {@link #serve} installed. */
  public static void served(MessageDoors.Inbound inbound) {
    if (inbound != null) {
      MessageDoors.endInbound(inbound);
    }
  }

  /** A request's response leaves: the serving handler's context, into its header. */
  public static void respond(ResponseHeader.Builder response, RequestHeader request,
    Object server) {
    if (request == null || !request.hasCausynthCorrelation()) {
      return;
    }
    MessageDoors.Outbound outbound = MessageDoors.outbound(TRANSPORT,
      request.getCausynthCorrelation(), ATTEMPT, MessageDoors.RESPONSE, server, null);
    if (outbound.active()) {
      response.setCausynthTrace(outbound.trace);
      response.setCausynthSymbolic(outbound.symbolic);
    }
  }
}
