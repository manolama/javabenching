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

import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.transport.local.LocalServerRegistry;
import io.atomix.catalyst.transport.local.LocalTransport;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;

public class AtomixTestQueue extends DistPriorityQueue {

  public static class QueueContext extends BaseContext {
    
    public QueueContext() {
      super();
      queue = new AtomixTest();
    }
  }
  
  @Benchmark
  public static void runAtomixTestQueue(QueueContext context, Blackhole blackHole) {
    run(context, blackHole);
  }
  
  static class AtomixTest implements TestQueue {
    AtomixReplica.Builder builder;
    AtomixReplica replica;
    io.atomix.collections.DistributedQueue<DummyQuery> queue;
    
    public AtomixTest() {
      try {
        // IF USED, shut it down after.
        //NettyTransport t = new NettyTransport();
        LocalServerRegistry lsr = new LocalServerRegistry();
        Transport t = new LocalTransport(lsr);
        builder = AtomixReplica.builder(new Address("localhost", 8700))
            .withStorage(Storage.builder()
                .withStorageLevel(StorageLevel.MEMORY)
                .build())
            .withTransport(t);
        replica = builder.build().bootstrap().join();
        queue = replica.<DummyQuery>getQueue("javaBench").get();
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
    
    @Override
    public void put(DummyQuery entry) {
      if (!queue.add(entry).join()) {
        throw new RuntimeException("WTF? Couldn't write!");
      }
    }

    @Override
    public DummyQuery pollTimed() {
      return queue.poll().join();
    }
    
    public void close() {
      queue.close().join();
      replica.shutdown().join();
    }
  }
}
