package net.opentsdb.distobj;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import net.opentsdb.utils.DateTime;

@State(Scope.Benchmark)
public class BaseContext {
  public DummyQuery[] queries = new DummyQuery[DistPriorityQueue.ENTRIES];
  public CountDownLatch latch = new CountDownLatch(DistPriorityQueue.ENTRIES);
  
  public TestQueue queue;
  
  public BaseContext() {
    Random rnd = new Random(DateTime.currentTimeMillis());
    for (int i = 0; i < DistPriorityQueue.ENTRIES; i++) {
      queries[i] = new DummyQuery();
      queries[i].priority = rnd.nextLong();
    }
  }
  
  @TearDown
  public void tearDown() {
    queue.close();
  }
}
