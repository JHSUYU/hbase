package org.apache.hadoop.hbase.dryrun;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.Scope;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class DryRunTracer {
  public static final String DRY_RUN_KEY = "dry_run";
  public static final Tracer tracer = GlobalOpenTelemetry.getTracer("SplitWALTracer");

  public static Span startSpan(String operationName, boolean isDryRun) {
    ContextKey<Boolean> dryRunKey = ContextKey.named(DRY_RUN_KEY);
    return tracer.spanBuilder(operationName).setParent(Context.current()).setAttribute(DRY_RUN_KEY, isDryRun).setSpanKind(SpanKind.INTERNAL).startSpan();
  }

  public static boolean isDryRun(Span span) {
    ContextKey<Boolean> dryRunKey = ContextKey.named(DRY_RUN_KEY);
    return Context.current().get(dryRunKey) != null && Boolean.TRUE.equals(
      Context.current().get(dryRunKey));
  }
}
