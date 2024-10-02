package org.apache.hadoop.hbase.dryrun;
import com.rits.cloning.Cloner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@InterfaceAudience.Private
public class DryRunManager {
  private static final Logger LOG = LoggerFactory.getLogger(DryRunManager.class);
  private static Set<Object> trackedObjects = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private static Map<Object, Object> objectProxies = new ConcurrentHashMap<>();
  private static Map<Object, Map<String, Pair<String, Object>>> trackedPrimitiveFields = new ConcurrentHashMap<>();
  private static Map<Map, Map<Object, Object>> trackedConcurrentHashMap = new ConcurrentHashMap<>();

  public static Cloner cloner = new Cloner();

  public static <T1,T2> void track(T1 object, T2 field) {
    if (object == null) {
      return;
    }

    if(!trackedObjects.contains(object)){
      return;
    }

    if(trackedObjects.contains(field)){
      return;
    }

    if(isPrimitiveType(field)){
      LOG.info("Tracking a primitive field: " + field + " for object: " + object);
      trackedPrimitiveFields.computeIfAbsent(object, k -> new ConcurrentHashMap<>())
        .put(field.getClass().getSimpleName(),
          Pair.newPair(field.getClass().getSimpleName(), field));
      return;
    }

    trackedObjects.add(field);
    //print object and field information
    LOG.info("Tracking object: " + object + " and field: " + field);
    T2 proxy = null;
    final Class<? extends Object> clazz = field.getClass();
    if(clazz == ArrayList.class ){
      LOG.info("Tracking a collection object: " + object);
      proxy = (T2)createArrayListProxy((ArrayList)field);
    }else if (clazz == HashMap.class){
      LOG.info("Tracking a hashMap object: " + object);
      proxy = (T2)createHashMapProxy((HashMap)field);
    }else if(clazz == Set.class){
      LOG.info("Tracking a set object: " + object);
      proxy = (T2)createArrayListProxy(new ArrayList((Set)field));
    }
    else if(clazz == ConcurrentHashMap.class){
      LOG.info("Tracking a concurrentHashMap object: " + object);
      proxy = (T2)createConcurrentMapProxy((ConcurrentHashMap)field);
    }else {
      proxy = createObjectProxy(field);
    }
    LOG.info("Creat proxy successfully");
    objectProxies.put(field, proxy);
    return;
  }

  private static <K, V> ConcurrentHashMap<K, V> createConcurrentMapProxy(ConcurrentHashMap<K, V> originalMap) {
    ConcurrentHashMap<K, V> proxy = new ConcurrentHashMap<>();
    for (Map.Entry<K, V> entry : originalMap.entrySet()) {
      trackedObjects.add(entry.getKey());
      trackedObjects.add(entry.getValue());
      proxy.put(entry.getKey(), entry.getValue());
    }
    return proxy;
  }


  private static <K, V> HashMap<K, V> createHashMapProxy(HashMap<K, V> originalMap) {
    HashMap<K, V> proxy = new HashMap<>();
    for (Map.Entry<K, V> entry : originalMap.entrySet()) {
      trackedObjects.add(entry.getKey());
      trackedObjects.add(entry.getValue());
      proxy.put(entry.getKey(), entry.getValue());
    }
    return proxy;
  }

  private static <E> ArrayList<E>  createArrayListProxy(ArrayList<E> originalList) {
    ArrayList<E> proxy = new ArrayList<>();
    for (E element : originalList) {
      trackedObjects.add(element);
      proxy.add(element);
    }
    return proxy;
  }

  public static boolean isPrimitiveType(Object object) {
    return object instanceof Boolean || object instanceof Character || object instanceof Byte || object instanceof Short || object instanceof Integer || object instanceof Long || object instanceof Float || object instanceof Double || object instanceof String;
  }

  public static <T> T add(T object) {
    if (object == null) {
      return null;
    }

    trackedObjects.add(object);
    T proxy = null;
    final Class<? extends Object> clazz = object.getClass();
    if(clazz == ArrayList.class){
      LOG.info("Tracking an ArrayList object: " + object);
      proxy =  (T)createArrayListProxy((ArrayList)object);
    }else {
      proxy = createObjectProxy(object);
    }
    objectProxies.put(object, proxy);
    return proxy;
  }

  public static <T1, T2> T2 get(T1 parent, T2 object) {
    if(object == null) {
      return null;
    }

    if(isPrimitiveType(object)){
      track(parent, object);
      return object;
    }

    track(parent, object);

    if(!trackedObjects.contains(object)){
      return object;
    }

    LOG.info("Failure Recovery, returning the proxy object for: " + object);
    return (T2)objectProxies.get(object);
  }

  @SuppressWarnings("unchecked")
  private static <T> T createObjectProxy(T obj) {
    T proxy = cloner.shallowCloneWithNewInstance(obj);
    objectProxies.put(obj, proxy);
    return obj;
  }

  public static <T> T shallowCopy(T obj, T obj$dryrun){
    if(obj$dryrun == null){
      return cloner.shallowClone(obj);
    }
    return obj$dryrun;
  }

  private static Object createBasicTypeProxy(Object value) {
    if (value instanceof Boolean) {
      return new BooleanWrapper((Boolean) value);
    } else if (value instanceof Character) {
      return new CharacterWrapper((Character) value);
    } else if (value instanceof Byte) {
      return new ByteWrapper((Byte) value);
    } else if (value instanceof Short) {
      return new ShortWrapper((Short) value);
    } else if (value instanceof Integer) {
      return new IntegerWrapper((Integer) value);
    } else if (value instanceof Long) {
      return new LongWrapper((Long) value);
    } else if (value instanceof Float) {
      return new FloatWrapper((Float) value);
    } else if (value instanceof Double) {
      return new DoubleWrapper((Double) value);
    } else if (value instanceof String) {
      return new StringWrapper((String) value);
    }
    throw new IllegalArgumentException("Unsupported type: " + value.getClass());
  }

  // Wrapper classes for basic types
  private static class BooleanWrapper {
    private Boolean value;
    public BooleanWrapper(Boolean value) { this.value = value; }
    public Boolean getValue() { return value; }
    public void setValue(Boolean value) { this.value = value; }
  }

  private static class CharacterWrapper {
    private Character value;
    public CharacterWrapper(Character value) { this.value = value; }
    public Character getValue() { return value; }
    public void setValue(Character value) { this.value = value; }
  }

  private static class ByteWrapper {
    private Byte value;
    public ByteWrapper(Byte value) { this.value = value; }
    public Byte getValue() { return value; }
    public void setValue(Byte value) { this.value = value; }
  }

  private static class ShortWrapper {
    private Short value;
    public ShortWrapper(Short value) { this.value = value; }
    public Short getValue() { return value; }
    public void setValue(Short value) { this.value = value; }
  }

  private static class IntegerWrapper {
    private Integer value;
    public IntegerWrapper(Integer value) { this.value = value; }
    public Integer getValue() { return value; }
    public void setValue(Integer value) { this.value = value; }
  }

  private static class LongWrapper {
    private Long value;
    public LongWrapper(Long value) { this.value = value; }
    public Long getValue() { return value; }
    public void setValue(Long value) { this.value = value; }
  }

  private static class FloatWrapper {
    private Float value;
    public FloatWrapper(Float value) { this.value = value; }
    public Float getValue() { return value; }
    public void setValue(Float value) { this.value = value; }
  }

  private static class DoubleWrapper {
    private Double value;
    public DoubleWrapper(Double value) { this.value = value; }
    public Double getValue() { return value; }
    public void setValue(Double value) { this.value = value; }
  }

  private static class StringWrapper {
    private String value;
    public StringWrapper(String value) { this.value = value; }
    public String getValue() { return value; }
    public void setValue(String value) { this.value = value; }
  }


}
