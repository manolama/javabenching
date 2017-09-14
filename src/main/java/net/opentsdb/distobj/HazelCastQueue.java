package net.opentsdb.distobj;

import java.util.concurrent.BlockingQueue;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class HazelCastQueue extends DistPriorityQueue {
  
  public static class QueueContext extends BaseContext {
    
    public QueueContext() {
      super();
      queue = new HazelCastTestQueue();
    }
  }
  
  @Benchmark
  public static void runHazelcastTestQueue(QueueContext context, Blackhole blackHole) {
    run(context, blackHole);
  }
  
  static class HazelCastTestQueue implements TestQueue {

    HazelcastInstance hz;
    BlockingQueue<DummyQuery> queue;
    
    public HazelCastTestQueue() {
      com.hazelcast.config.Config config = new com.hazelcast.config.Config();
      hz = Hazelcast.newHazelcastInstance(config);
      queue = hz.getQueue("javaBench");
    }
    
    @Override
    public void put(DummyQuery entry) {
      if (!queue.add(entry)) {
        throw new RuntimeException("WTF? Couldn't write!");
      }
    }

    @Override
    public DummyQuery pollTimed() {
      return queue.poll();
    }
    
    public void close() {
      hz.shutdown();
    }
  }
}
