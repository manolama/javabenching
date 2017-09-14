package net.opentsdb.distobj;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

//import org.openspaces.collections.CollocationMode;
//import org.openspaces.collections.GigaQueueConfigurer;
//import org.openspaces.collections.queue.GigaBlockingQueue;
//import org.openspaces.core.GigaSpace;
//import org.openspaces.core.GigaSpaceConfigurer;
//import org.openspaces.core.space.EmbeddedSpaceConfigurer;

public class XAPTestQueue extends DistPriorityQueue {

  public static class QueueContext extends BaseContext {
    
    public QueueContext() {
      super();
      queue = new XAPTest();
    }
  }
  
  @Benchmark
  public static void runXAPTestQueue(QueueContext context, Blackhole blackHole) {
    run(context, blackHole);
  }
  
  static class XAPTest implements TestQueue {
//    EmbeddedSpaceConfigurer esc;
//    GigaSpaceConfigurer conf;
//    GigaSpace space;
//    GigaBlockingQueue<DummyQuery> queue;
    
    public XAPTest() {
//      esc = new EmbeddedSpaceConfigurer("JavaBench");
//      conf = new GigaSpaceConfigurer(esc);
//      space = conf.gigaSpace();
//      queue = new GigaQueueConfigurer<DummyQuery>(space, "javaBench", CollocationMode.DISTRIBUTED)
//          .elementType(DummyQuery.class)
//          .gigaQueue();
    }
    
    @Override
    public void put(DummyQuery entry) {
//      if (!queue.add(entry)) {
//        throw new RuntimeException("WTF!! Can't write");
//      }
    }
  
    @Override
    public DummyQuery pollTimed() {
      return null;//queue.poll();
    }
    
    @Override
    public void close() {
      // ALMOST just need to close it. The daemon threads are left running.
//    try {
//    q.queue.close();
//  } catch (Exception e) {
//    // TODO Auto-generated catch block
//    e.printStackTrace();
//  }
//  
//  try {
//    q.space.getSpace().getDirectProxy()
//    .getContainer().shutdown();
//  } catch (RemoteException e) {
//    // TODO Auto-generated catch block
//    e.printStackTrace();
//  }
//  q.esc.close();
    }
  }
}
