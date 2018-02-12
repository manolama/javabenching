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
