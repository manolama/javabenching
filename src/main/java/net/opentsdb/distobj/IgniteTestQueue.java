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
