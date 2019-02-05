// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.vibur.objectpool.ConcurrentPool;
import org.vibur.objectpool.PoolObjectFactory;
import org.vibur.objectpool.PoolService;
import org.vibur.objectpool.util.ConcurrentLinkedQueueCollection;

import stormpot.BlazePool;
import stormpot.PoolException;
import stormpot.Slot;

//@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Threads(16)
public class ObjectPools {
  public static final int SIZE = 1024 * 1024;
  public static final int MAX = 4096;
  
  @State(Scope.Benchmark)
  public static class Context
  {
    public final int[] lengths = new int[SIZE];
    public final long[] values = new long[SIZE];
    
    PooledLongArrayFactory pool;
    
    stormpot.BlazePool<StormPotArray> stormpot;

    PoolService<long[]> viburPool;
    
    @Setup
    public void setup() {
      System.out.println("******* INIT");
      Random random = new Random();
      for (int i = 0; i < SIZE; i++) {
        lengths[i] = random.nextInt(MAX - 1);
        if (lengths[i] == 0) {
          lengths[i] = 1;
        }
        values[i] = random.nextLong();
      }
      
      pool = new PooledLongArrayFactory(4096, 4096);
    
      stormpot.Config<StormPotArray> config = 
          new stormpot.Config<StormPotArray>()
          .setAllocator(new StormPotAlloc())
          .setSize(4096);
      stormpot = new BlazePool<StormPotArray>(config);
      
      viburPool = new ConcurrentPool<>(
          new ConcurrentLinkedQueueCollection<long[]>(), new PoolObjectFactory() {
            
            @Override
            public Object create() {
              return new long[MAX];
            }

            @Override
            public boolean readyToTake(Object obj) {
              // TODO Auto-generated method stub
              return false;
            }

            @Override
            public boolean readyToRestore(Object obj) {
              // TODO Auto-generated method stub
              return false;
            }

            @Override
            public void destroy(Object obj) {
              // TODO Auto-generated method stub
              
            }
            
          }, 1024, 4096, false);
      
    }
  
    @TearDown
    public void teardown() {
      try {
        stormpot.shutdown().await(new stormpot.Timeout(5, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  
  // https://github.com/georges-gomes/pooled-byte-array
//  @Benchmark
//  public static void allocateEachTime(Context context, Blackhole blackHole)
//  {
//    for (int i = 0; i < SIZE; i++) {
//      long[] array = new long[context.lengths[i]];
//      for (int x = 0; x < array.length; x++) {
//        array[x] = context.values[x];
//      }
//      if (blackHole != null) {
//        blackHole.consume(array);
//      }
//    }
//  }
  
  @Benchmark
  public static void roundRobinPool(Context context, Blackhole blackHole)
  {
    
    for (int i = 0; i < SIZE; i++) {
      PooledLongArray array = context.pool.getByteArray();
      for (int x = 0; x < context.lengths[i]; x++) {
        array.setAt(x, context.values[x]);
      }
      if (blackHole != null) {
        blackHole.consume(array);
      }
    }
  }
  
  // https://github.com/chrisvest
  @Benchmark
  public static void stormPotPool(Context context, Blackhole blackHole)
  {
    stormpot.Timeout timeout = new stormpot.Timeout(1, TimeUnit.SECONDS);
    for (int i = 0; i < SIZE; i++) {
      StormPotArray arr;
      try {
        arr = context.stormpot.claim(timeout);
        if (arr == null) {
          System.out.println("WTF? NO ALLOC? " + i);
          continue;
        }
        for (int x = 0; x < context.lengths[i]; x++) {
          arr.array[x] = context.values[x];
        }
        if (blackHole != null) {
          blackHole.consume(arr.array);
        }
        arr.release();
      } catch (PoolException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  
  // http://www.vibur.org/vibur-object-pool/
  @Benchmark
  public static void viburPool(Context context, Blackhole blackHole)
  {
    for (int i = 0; i < SIZE; i++) {
      long[] arr = null;
      try {
          arr = context.viburPool.tryTake(1000, TimeUnit.MILLISECONDS);
          if (arr == null) // i.e., no any objects were available in the pool for a duration of 500 milliseconds
              continue;
  
          for (int x = 0; x < context.lengths[i]; x++) {
            arr[x] = context.values[x];
          }
          if (blackHole != null) {
            blackHole.consume(arr);
          }
      } finally {
          if (arr != null)
            context.viburPool.restore(arr);
      }    
    }
  }
  
  public static class PooledLongArrayFactory
  {
      private Queue<PooledLongArray> pool;
      private final int arraySize;

      public PooledLongArrayFactory(int arraySize, int preAllocSize)
      {
          this.arraySize = arraySize;

          pool = new ArrayBlockingQueue<>(preAllocSize);
          for(int i=0; i<preAllocSize; i++)
          {
              pool.offer(new PooledLongArray(new long[arraySize]));
          }
      }

      /**
       * @return Ready to use PooledByteArray. Be careful not reset!
       */
      public PooledLongArray getByteArray()
      {
          PooledLongArray arr = pool.poll();
          pool.offer(arr);
          return arr;
      }

  }
  
  public static class PooledLongArray
  {
      private long[] array = null;

      PooledLongArray(long[] array)
      {
          this.array = array;
      }

      public int length()
      {
          return array.length;
      }

      public long getAt(int index)
      {
          return array[index];
      }

      public void setAt(int index, long b)
      {
        array[index] = b;
      }
  }
  
  public static class StormPotAlloc implements stormpot.Allocator<StormPotArray> {

    @Override
    public StormPotArray allocate(Slot slot) throws Exception {
      return new StormPotArray(slot);
    }

    @Override
    public void deallocate(StormPotArray poolable) throws Exception {
      poolable.release();
    }
    
  }
  
  public static class StormPotArray implements stormpot.Poolable {

    public long[] array = new long[MAX];
    
    final Slot slot;
    public StormPotArray(final Slot slot) {
      this.slot = slot;
    }
    
    @Override
    public void release() {
      slot.release(this);
    }
    
  }
  
}
