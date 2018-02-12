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
