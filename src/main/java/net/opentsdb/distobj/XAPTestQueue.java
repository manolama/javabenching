// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
