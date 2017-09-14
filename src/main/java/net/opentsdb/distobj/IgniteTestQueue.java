package net.opentsdb.distobj;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

public class IgniteTestQueue extends DistPriorityQueue {

  public static class QueueContext extends BaseContext {
    
    public QueueContext() {
      super();
      queue = new IgniteTest();
    }
  }
  
  @Benchmark
  public static void runIgniteTestQueue(QueueContext context, Blackhole blackHole) {
    run(context, blackHole);
  }
  
  static class IgniteTest implements TestQueue {
    Ignite ignite;
    CollectionConfiguration colCfg;
    IgniteQueue<DummyQuery> queue;
    
    public IgniteTest() {
      ignite = Ignition.start();
      colCfg = new CollectionConfiguration();
      colCfg.setCollocated(false); 
      colCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
      queue = ignite.queue("yamasBench", 0, colCfg);
    }
    
    @Override
    public void put(DummyQuery entry) {
      if (!queue.add(entry)) {
        throw new RuntimeException("WTF????");
      }
    }

    @Override
    public DummyQuery pollTimed() {
      return queue.poll();
    }
    
    public void close() {
      queue.close();
      ignite.close();
    }
  }
}
