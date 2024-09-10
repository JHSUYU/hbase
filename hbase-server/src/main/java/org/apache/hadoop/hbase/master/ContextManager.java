package org.apache.hadoop.hbase.master;

import org.apache.yetus.audience.InterfaceAudience;
import java.util.concurrent.ConcurrentHashMap;

@InterfaceAudience.Private
public class ContextManager {
  private static final ConcurrentHashMap<String, Object> contextMap = new ConcurrentHashMap<>();

  public static <T> T getVariable(String variableName, T original) {
    String copyKey = variableName + "$copy";

    // 如果复制不存在，创建一个深拷贝
    return (T) contextMap.computeIfAbsent(copyKey, k -> deepCopy(original));
  }

  public static void setVariable(String variableName, Object value) {
    String copyKey = variableName + "$copy";
    contextMap.remove(copyKey);  // 移除旧的复制
  }

  private static <T> T deepCopy(T original) {
    if (original == null) {
      return null;
    }

    try {
      return original;
    } catch (Exception e) {
      throw new RuntimeException("Deep copy failed", e);
    }
  }
}

