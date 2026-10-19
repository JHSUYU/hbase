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

import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ResponseHeader;

@InterfaceAudience.Private
public final class CausynthRpc {
  public static final String TRANSPORT = "HBASE_RPC";
  private static final String ATTEMPT = "1";
  private static final AtomicLong CLIENTS = new AtomicLong();

  private CausynthRpc() {
  }

  static long nextClient() {
    return CLIENTS.incrementAndGet();
  }

  static String verb(Descriptors.MethodDescriptor md) {
    return md.getService().getName() + "." + md.getName();
  }

  static long starting(Descriptors.MethodDescriptor md) {
    return MessageDoors.requestStarting(TRANSPORT, verb(md), 1);
  }

  static void ended(long request) {
    MessageDoors.requestEnded(request);
  }

  static void sent(Call call, long client, Object owner) {
    call.causynthCorrelation = "hbase-rpc-" + client + "-" + call.id;
    call.causynthOwner = owner;
    MessageDoors.Outbound outbound = MessageDoors.outboundRequest(TRANSPORT,
      call.causynthCorrelation, ATTEMPT, owner, null, verb(call.md), call.param);
    call.causynthTrace = outbound.trace;
    call.causynthSymbolic = outbound.symbolic;
  }

  static void header(RequestHeader.Builder header, Call call) {
    if (!call.causynthTrace.isEmpty() || !call.causynthSymbolic.isEmpty()) {
      header.setCausynthCorrelation(call.causynthCorrelation);
      header.setCausynthTrace(call.causynthTrace);
      header.setCausynthSymbolic(call.causynthSymbolic);
    }
  }

  static MessageDoors.Inbound responded(Call call, ResponseHeader header) {
    if (!header.hasCausynthTrace() && !header.hasCausynthSymbolic()) {
      return null;
    }
    return MessageDoors.inbound(header.getCausynthTrace(), header.getCausynthSymbolic(),
      TRANSPORT, call.causynthCorrelation, ATTEMPT, MessageDoors.RESPONSE, call.causynthOwner,
      false);
  }

  static void parsed(Call call, Message response) {
    MessageDoors.Inbound served = MessageDoors.current();
    if (
      served != null && MessageDoors.RESPONSE.equals(served.half)
        && served.correlationId.equals(call.causynthCorrelation)
    ) {
      MessageDoors.parsed(response);
    }
  }

  static void handled(MessageDoors.Inbound response) {
    MessageDoors.endInbound(response);
  }

  public static MessageDoors.Inbound serve(RequestHeader header, Message param, Object server) {
    if (header == null || !header.hasCausynthCorrelation()) {
      return MessageDoors.inbound(null, null, TRANSPORT, "", ATTEMPT, MessageDoors.REQUEST, server,
        false);
    }
    MessageDoors.Inbound inbound = MessageDoors.inbound(header.getCausynthTrace(),
      header.getCausynthSymbolic(), TRANSPORT, header.getCausynthCorrelation(), ATTEMPT,
      MessageDoors.REQUEST, server, false);
    MessageDoors.parsed(param);
    return inbound;
  }

  public static void served(MessageDoors.Inbound inbound) {
    MessageDoors.endInbound(inbound);
  }

  public static void respond(ResponseHeader.Builder response, RequestHeader request,
    Object server, Message message) {
    if (request == null || !request.hasCausynthCorrelation()) {
      return;
    }
    MessageDoors.Outbound outbound = MessageDoors.outbound(TRANSPORT,
      request.getCausynthCorrelation(), ATTEMPT, MessageDoors.RESPONSE, server, null, message);
    if (outbound.active()) {
      response.setCausynthTrace(outbound.trace);
      response.setCausynthSymbolic(outbound.symbolic);
    }
  }
}
