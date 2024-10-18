package org.apache.hadoop.hbase.dryrun;

import org.apache.yetus.audience.InterfaceAudience;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

@InterfaceAudience.Private
public class DryRunConditionVariable {
  private final Lock lock;
  private final Condition realCondition;
  private final Queue<WaitingThread> waitingThreads = new ConcurrentLinkedQueue<>();
  private final AtomicInteger waitCount = new AtomicInteger(0);
  private volatile boolean isDryRun = false;

  public DryRunConditionVariable(Lock lock) {
    this.lock = lock;
    this.realCondition = lock.newCondition();
  }

  public void setDryRun(boolean dryRun) {
    this.isDryRun = dryRun;
  }

  public void await() throws InterruptedException {
    if (!isDryRun) {
      realCondition.await();
      return;
    }

    int waitId = waitCount.getAndIncrement();
    WaitingThread waitingThread = new WaitingThread(Thread.currentThread(), waitId);
    waitingThreads.offer(waitingThread);

    lock.unlock();
    try {
      System.out.println("Thread " + Thread.currentThread().getId() + " started waiting (ID: " + waitId + ")");
      while (!waitingThread.isSignaled()) {
        if (Thread.interrupted()) {
          waitingThreads.remove(waitingThread);
          throw new InterruptedException();
        }
        Thread.yield(); // 允许其他线程运行
      }
      System.out.println("Thread " + Thread.currentThread().getId() + " finished waiting (ID: " + waitId + ")");
    } finally {
      lock.lock();
    }
  }


  public void signal() {
    if (!isDryRun) {
      realCondition.signal();
      return;
    }

    WaitingThread waitingThread = waitingThreads.poll();
    if (waitingThread != null) {
      waitingThread.signal();
      System.out.println("Signaled thread " + waitingThread.getThread().getId() +
        " (Wait ID: " + waitingThread.getWaitId() + ")");
    }
  }

  public void signalAll() {
    if (!isDryRun) {
      realCondition.signalAll();
      return;
    }

    WaitingThread waitingThread;
    while ((waitingThread = waitingThreads.poll()) != null) {
      waitingThread.signal();
      System.out.println("Signaled thread " + waitingThread.getThread().getId() +
        " (Wait ID: " + waitingThread.getWaitId() + ")");
    }
  }

  // 实现其他 Condition 接口方法...

  private static class WaitingThread {
    private final Thread thread;
    private final int waitId;
    private volatile boolean signaled = false;

    WaitingThread(Thread thread, int waitId) {
      this.thread = thread;
      this.waitId = waitId;
    }

    void signal() {
      signaled = true;
    }

    boolean isSignaled() {
      return signaled;
    }

    Thread getThread() {
      return thread;
    }

    int getWaitId() {
      return waitId;
    }
  }


  public int getWaitingThreadCount() {
    return waitingThreads.size();
  }

  public boolean isThreadWaiting(Thread thread) {
    return waitingThreads.stream().anyMatch(wt -> wt.getThread() == thread);
  }
}

