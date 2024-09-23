package org.apache.hadoop.hbase.ipc;

import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelDuplexHandler;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandler;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPromise;
import org.apache.hbase.thirdparty.io.netty.util.Attribute;
import org.apache.hbase.thirdparty.io.netty.util.AttributeKey;
import org.apache.http.HttpRequest;
import org.apache.yetus.audience.InterfaceAudience;
@InterfaceAudience.Private
public class NettyRpcDryRunTracerHandler extends ChannelDuplexHandler {

  public static final AttributeKey<String> DRY_RUN_BAGGAGE = AttributeKey.valueOf("dryRunBaggage");

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    System.out.println("Failure Recovery, NettyRpcDryRunTracerHandler write dryrun is: " + TraceUtil.isDryRun());
    Baggage dryRunBaggage = Baggage.current();
    String isDryRun = String.valueOf(TraceUtil.isDryRun());
    if (dryRunBaggage != null) {
      System.out.println("Failure Recovery, NettyRpcDryRunTracerHandler write dryrun is: " + isDryRun);
      ctx.channel().attr(DRY_RUN_BAGGAGE).set(isDryRun);
      // HttpRequest?
    }
    System.out.println("FL Message type: " + msg.getClass().getName());
    if(msg instanceof Call){
      Call call = (Call) msg;
      call.isDryRun = Boolean.parseBoolean(isDryRun);
    }
    try (Scope ignored = Context.current().makeCurrent()) {
      super.write(ctx, msg, promise);
    } catch (Throwable throwable) {
      ctx.channel().attr(DRY_RUN_BAGGAGE).remove();
      throw throwable;
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    System.out.println("Failure Recovery, NettyRpcDryRunTracerHandler channelRead");
    if (msg instanceof Call) {
      Call call = (Call) msg;
      boolean isDryRun = call.isDryRun;  // 从 Call 对象中获取 isDryRun
      ctx.channel().attr(DRY_RUN_BAGGAGE).set(String.valueOf(isDryRun));
    }
    String isDryRun = ctx.channel().attr(DRY_RUN_BAGGAGE).get();
    if (isDryRun != null) {
      System.out.println("Failure Recovery, NettyRpcDryRunTracerHandler channelRead dryrun is: " + isDryRun);
      if (Boolean.parseBoolean(isDryRun)) {
        Baggage dryRunBaggage = TraceUtil.createDryRunBaggage();
        dryRunBaggage.makeCurrent();
        Context.current().with(dryRunBaggage);
      }
      super.channelRead(ctx, msg);
    } else {
      super.channelRead(ctx, msg);
    }
  }
}
