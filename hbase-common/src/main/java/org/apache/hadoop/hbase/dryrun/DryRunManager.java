package org.apache.hadoop.hbase.dryrun;

import org.apache.yetus.audience.InterfaceAudience;
import java.util.*;
import java.util.concurrent.*;
@InterfaceAudience.Private
public class DryRunManager {
  public static <T> T clone(T obj) {
    if (obj == null) {
      return null;
    }

    if (obj instanceof Set) {
      return (T) cloneSet((Set<?>) obj);
    } else if (obj instanceof Map) {
      return (T) cloneMap((Map<?, ?>) obj);
    } else if (obj instanceof Queue) {
      return (T) cloneQueue((Queue<?>) obj);
    } else if (obj instanceof List) {
      return (T) cloneList((List<?>) obj);
    }

    // For other types, return the original object
    return obj;
  }

  private static <E> Set<E> cloneSet(Set<E> original) {
    if (original == null) {
      return null;
    }

    try {
      // Try to create a new instance of the same class
      Set<E> newSet = original.getClass().getDeclaredConstructor().newInstance();
      newSet.addAll(original);
      return newSet;
    } catch (Exception e) {
      // If we can't create a new instance, we'll use type-specific handling
      if (original instanceof TreeSet) {
        return new TreeSet<>(original);
      } else if (original instanceof LinkedHashSet) {
        return new LinkedHashSet<>(original);
      } else if (original instanceof ConcurrentSkipListSet) {
        return new ConcurrentSkipListSet<>(original);
      } else if (original instanceof CopyOnWriteArraySet) {
        return new CopyOnWriteArraySet<>(original);
      } else if (original instanceof HashSet) {
        return new HashSet<>(original);
      } else {
        // For unknown types, including Collections.unmodifiableSet,
        // Collections.synchronizedSet, etc., we return the original set
        // as these are usually wrappers and don't need cloning
        return original;
      }
    }
  }

  private static <K, V> Map<K, V> cloneMap(Map<K, V> original) {
    if (original == null) {
      return null;
    }

    try {
      // Try to create a new instance of the same class
      Map<K, V> newMap = original.getClass().getDeclaredConstructor().newInstance();
      newMap.putAll(original);
      return newMap;
    } catch (Exception e) {
      // If we can't create a new instance, we'll use type-specific handling
      if (original instanceof TreeMap) {
        return new TreeMap<>(original);
      } else if (original instanceof LinkedHashMap) {
        return new LinkedHashMap<>(original);
      } else if (original instanceof ConcurrentHashMap) {
        return new ConcurrentHashMap<>(original);
      } else if (original instanceof ConcurrentSkipListMap) {
        ConcurrentSkipListMap<K, V> originalCSLM = (ConcurrentSkipListMap<K, V>) original;
        ConcurrentSkipListMap<K, V> newCSLM = new ConcurrentSkipListMap<>(originalCSLM.comparator());
        newCSLM.putAll(original);
        return newCSLM;
      } else if (original instanceof IdentityHashMap) {
        return new IdentityHashMap<>(original);
      } else if (original instanceof WeakHashMap) {
        return new WeakHashMap<>(original);
      } else {
        // For unknown types, including Collections.unmodifiableMap,
        // Collections.synchronizedMap, etc., we use HashMap
        return new HashMap<>(original);
      }
    }
  }

  private static <E> Queue<E> cloneQueue(Queue<E> original) {
    if (original == null) {
      return null;
    }

    if (original instanceof PriorityQueue) {
      return new PriorityQueue<>((PriorityQueue<E>) original);
    } else if (original instanceof ConcurrentLinkedQueue) {
      return new ConcurrentLinkedQueue<>(original);
    } else if (original instanceof BlockingQueue) {
      if (original instanceof ArrayBlockingQueue) {
        ArrayBlockingQueue<E> abq = (ArrayBlockingQueue<E>) original;
        return new ArrayBlockingQueue<>(abq.size(), abq.remainingCapacity() == 0, original);
      } else if (original instanceof LinkedBlockingQueue) {
        return new LinkedBlockingQueue<>(original);
      } else if (original instanceof PriorityBlockingQueue) {
        return new PriorityBlockingQueue<>(original);
      }
    } else if (original instanceof Deque) {
      if (original instanceof ConcurrentLinkedDeque) {
        return new ConcurrentLinkedDeque<>(original);
      } else if (original instanceof LinkedBlockingDeque) {
        return new LinkedBlockingDeque<>(original);
      } else {
        return new LinkedList<>(original);
      }
    }
    return new LinkedList<>(original);
  }

  private static <E> List<E> cloneList(List<E> original) {
    if (original == null) {
      return null;
    }

    if (original instanceof LinkedList) {
      return new LinkedList<>(original);
    } else if (original instanceof CopyOnWriteArrayList) {
      return new CopyOnWriteArrayList<>(original);
    } else {
      return new ArrayList<>(original);
    }
  }
}
