package net.opentsdb.distobj;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import org.redisson.Redisson;
import org.redisson.api.RPriorityQueue;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class RedissonPriorityQueue extends DistPriorityQueue {

  public static class QueueContext extends BaseContext {
    
    public QueueContext() {
      super();
      queue = new RedissonQueue();
    }
  }

  @Benchmark
  public static void runRedissonPriorityQueue(QueueContext context, Blackhole blackHole) {
    run(context, blackHole);
  }
  
  static class RedissonQueue implements TestQueue {

    Config conf;
    RedissonClient redisson;
    RPriorityQueue<DummyQuery> queue;
    
    public RedissonQueue() {
      conf = new Config();
      conf.useSingleServer()
        .setAddress("redis://127.0.0.1:6379");
      redisson = Redisson.create(conf);
      
      queue = redisson.getPriorityQueue("javaBenchQueue");
//      for (int i = 0; i < 10000; i++) {
//        if (queue.add(new DummyQuery())) {
//          System.out.println("wrote to the queue, yay");
//        } else {
//          throw new RuntimeException("WTF? couldn't delete it!?!?!");
//        }
//      }
//      
      System.out.println("QUEUE SIZE: " + queue.size());
      System.out.println("COMPARTOR: " + queue.comparator());
    }
    
    @Override
    public void put(DummyQuery entry) {
      queue.add(entry);
//      if (queue.add(entry)) {
//        throw new RuntimeException("WTF? Couldn't write");
//      }
//      System.out.println("WROTE value: " + entry.priority);
    }

    @Override
    public DummyQuery pollTimed() {
      return queue.poll();
    }
    
    public void close() {
      redisson.shutdown();
    }
  }
}
