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
