package net.opentsdb.distobj;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
public class DistPriorityQueue {
  // WARNING!! Make sure this is divsible by READ_THREADS!
  static final int ENTRIES = 1024 * 128;
  static final int WRITE_THREADS = 16;
  // WARNING!! Make sure this divides into ENTRIES
  static final int READ_THREADS = 8;
  
  public static void main(String[] args) {
    try {
      BaseContext ctx = new BaseContext();
      //ctx.setup();
      
      //runPriorityBlockingQueue(ctx, null);
      //runRedissonPriorityQueue(ctx, null);
      //runIgniteTestQueue(ctx, null);
      //runAtomixTestQueue(ctx, null);
      //runHazelcastTestQueue(ctx, null);
      //runXAPTestQueue(ctx, null);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  static void run(BaseContext context, Blackhole blackHole) {
    Thread[] writers = new Thread[WRITE_THREADS];
    Thread[] consumers = new Thread[READ_THREADS];
    
    for (int i = 0; i < WRITE_THREADS; i++) {
      writers[i] = new Writer(context, i, blackHole);
      writers[i].start();
    }
    for (int i = 0; i < READ_THREADS; i++) {
      consumers[i] = new Consumer(context, i, blackHole);
      consumers[i].start();
    }
    
    for (int i = 0; i < WRITE_THREADS; i++) {
      try {
        writers[i].join();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    for (int i = 0; i < READ_THREADS; i++) {
      try {
        consumers[i].join();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  
  static class Writer extends Thread {
    final BaseContext ctx;
    final int threadId;
    final Blackhole blackHole;
    public Writer(final BaseContext ctx, final int threadId, final Blackhole blackHole) {
      this.ctx = ctx;
      this.threadId = threadId;
      this.blackHole = blackHole;
    }
    
    @Override
    public void run() {
      Thread.currentThread().setName("WRITER_" + threadId);
      int perThread = ENTRIES / WRITE_THREADS;
      //System.out.println("[i] writing from " + (threadId * perThread) + " to " + (perThread + (threadId * perThread)) + " Out of " + ENTRIES);
      for (int i = threadId * perThread; i < (perThread + (threadId * perThread)); i++) {
        ctx.queue.put(ctx.queries[i]);
      }
      //System.out.println("Done with writer thread " + threadId);
    }
  }
  
  static class Consumer extends Thread {
    final BaseContext ctx;
    final int threadId;
    final Blackhole blackHole;
    public Consumer(final BaseContext ctx, final int threadId, final Blackhole blackHole) {
      this.ctx = ctx;
      this.threadId = threadId;
      this.blackHole = blackHole;
    }
    @Override
    public void run() {
      Thread.currentThread().setName("CONSUMER_" + threadId);
      while (ctx.latch.getCount() > 0) {
        DummyQuery query = ctx.queue.pollTimed();
        if (query != null) {
          if (blackHole == null) {
            System.out.println("Query: " + query.priority);
          } else {
            blackHole.consume(query.priority);
          }
          ctx.latch.countDown();
        }
      }
      //System.out.println("Done with consumer thread " + threadId);
    }
    
  }

}
