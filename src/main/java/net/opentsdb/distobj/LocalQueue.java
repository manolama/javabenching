package net.opentsdb.distobj;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

public class LocalQueue extends DistPriorityQueue {
  
  public static class QueueContext extends BaseContext {
    
    public QueueContext() {
      super();
      queue = new LocalPriorityQueue();
    }
  }
  
  @Benchmark
  public static void runPriorityBlockingQueue(QueueContext context, Blackhole blackHole) {
    run(context, blackHole);
  }
  
  static class LocalPriorityQueue implements TestQueue {
    PriorityBlockingQueue<DummyQuery> queue = new PriorityBlockingQueue<DummyQuery>();
    
    @Override
    public void put(DummyQuery entry) {
      queue.put(entry);
    }

    @Override
    public DummyQuery pollTimed() {
      try {
        return queue.poll(1, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException("WTF???", e);
      }
    }
    
    public void close() { }
  }
}
