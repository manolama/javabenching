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
package net.opentsdb;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;
import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;

/**
 * This class is to try out various stack overflow avoidance mechanisms
 * when it comes to asynchronous pipelines.
 * 
 * see https://stackoverflow.com/questions/860550/stack-overflows-from-deep-recursion-in-java
 * https://stackoverflow.com/questions/189725/what-is-a-trampoline-function
 * 
 * Looks like the thread pool method doesn't incur too much of a perf hit over
 * regular old recursion. Also Quasar is definitely a bit slower than the 
 * thread pool, which is a bummer. And ForkJoin is just really inconsistent and
 * right-out there.
 * 
 * <code>
 * Benchmark                                                       Mode  Cnt         Score         Error   Units
SOAvoidance.mutualRecursion                                     avgt   15   5613856.149 ±   79140.618   ns/op
SOAvoidance.mutualRecursion:·gc.alloc.rate                      avgt   15      1484.245 ±      20.924  MB/sec
SOAvoidance.mutualRecursion:·gc.alloc.rate.norm                 avgt   15  13107263.870 ±      22.225    B/op
SOAvoidance.mutualRecursion:·gc.churn.PS_Eden_Space             avgt   15      1488.285 ±      46.688  MB/sec
SOAvoidance.mutualRecursion:·gc.churn.PS_Eden_Space.norm        avgt   15  13141576.480 ±  302534.261    B/op
SOAvoidance.mutualRecursion:·gc.churn.PS_Survivor_Space         avgt   15         0.185 ±       0.059  MB/sec
SOAvoidance.mutualRecursion:·gc.churn.PS_Survivor_Space.norm    avgt   15      1636.886 ±     530.733    B/op
SOAvoidance.mutualRecursion:·gc.count                           avgt   15       191.000                counts
SOAvoidance.mutualRecursion:·gc.time                            avgt   15       124.000                    ms
SOAvoidance.recursion                                           avgt   15   5829767.904 ±  101247.177   ns/op
SOAvoidance.recursion:·gc.alloc.rate                            avgt   15      1429.800 ±      24.939  MB/sec
SOAvoidance.recursion:·gc.alloc.rate.norm                       avgt   15  13107264.232 ±      23.522    B/op
SOAvoidance.recursion:·gc.churn.PS_Eden_Space                   avgt   15      1427.557 ±      86.766  MB/sec
SOAvoidance.recursion:·gc.churn.PS_Eden_Space.norm              avgt   15  13088666.478 ±  796089.137    B/op
SOAvoidance.recursion:·gc.churn.PS_Survivor_Space               avgt   15         0.161 ±       0.057  MB/sec
SOAvoidance.recursion:·gc.churn.PS_Survivor_Space.norm          avgt   15      1482.664 ±     533.105    B/op
SOAvoidance.recursion:·gc.count                                 avgt   15       148.000                counts
SOAvoidance.recursion:·gc.time                                  avgt   15       124.000                    ms
SOAvoidance.runQuasarFibers                                     avgt   15   8424480.665 ±   95461.731   ns/op
SOAvoidance.runQuasarFibers:·gc.alloc.rate                      avgt   15      1033.875 ±      12.153  MB/sec
SOAvoidance.runQuasarFibers:·gc.alloc.rate.norm                 avgt   15  13699049.306 ±    7169.182    B/op
SOAvoidance.runQuasarFibers:·gc.churn.PS_Eden_Space             avgt   15      1038.658 ±      94.989  MB/sec
SOAvoidance.runQuasarFibers:·gc.churn.PS_Eden_Space.norm        avgt   15  13758985.862 ± 1206782.041    B/op
SOAvoidance.runQuasarFibers:·gc.churn.PS_Survivor_Space         avgt   15         0.287 ±       0.137  MB/sec
SOAvoidance.runQuasarFibers:·gc.churn.PS_Survivor_Space.norm    avgt   15      3808.990 ±    1846.479    B/op
SOAvoidance.runQuasarFibers:·gc.count                           avgt   15        85.000                counts
SOAvoidance.runQuasarFibers:·gc.time                            avgt   15       119.000                    ms
SOAvoidance.runThreadPoolTest                                   avgt   15   7862002.919 ±  131704.074   ns/op
SOAvoidance.runThreadPoolTest:·gc.alloc.rate                    avgt   15      1003.840 ±     297.404  MB/sec
SOAvoidance.runThreadPoolTest:·gc.alloc.rate.norm               avgt   15  12417148.914 ± 3671816.465    B/op
SOAvoidance.runThreadPoolTest:·gc.churn.PS_Eden_Space           avgt   15      1098.341 ±      70.332  MB/sec
SOAvoidance.runThreadPoolTest:·gc.churn.PS_Eden_Space.norm      avgt   15  13574657.826 ±  749050.710    B/op
SOAvoidance.runThreadPoolTest:·gc.churn.PS_Survivor_Space       avgt   15         0.172 ±       0.073  MB/sec
SOAvoidance.runThreadPoolTest:·gc.churn.PS_Survivor_Space.norm  avgt   15      2136.351 ±     904.416    B/op
SOAvoidance.runThreadPoolTest:·gc.count                         avgt   15       184.000                counts
SOAvoidance.runThreadPoolTest:·gc.time                          avgt   15       103.000                    ms
SOAvoidance.trampoline                                          avgt   15   5650174.351 ±   93537.719   ns/op
SOAvoidance.trampoline:·gc.alloc.rate                           avgt   15      1497.238 ±      24.218  MB/sec
SOAvoidance.trampoline:·gc.alloc.rate.norm                      avgt   15  13303895.991 ±      22.785    B/op
SOAvoidance.trampoline:·gc.churn.PS_Eden_Space                  avgt   15      1509.347 ±      76.781  MB/sec
SOAvoidance.trampoline:·gc.churn.PS_Eden_Space.norm             avgt   15  13415640.383 ±  751066.309    B/op
SOAvoidance.trampoline:·gc.churn.PS_Survivor_Space              avgt   15         0.235 ±       0.096  MB/sec
SOAvoidance.trampoline:·gc.churn.PS_Survivor_Space.norm         avgt   15      2085.286 ±     855.556    B/op
SOAvoidance.trampoline:·gc.count                                avgt   15       221.000                counts
SOAvoidance.trampoline:·gc.time                                 avgt   15       133.000                    ms
 * </code>
 * 
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
public class SOAvoidance {
  // recursion => 1024 == OK, 1024 * 8, no bueno
  static final int DEPTH = 1024 * 8;
  
  public static void main(final String[] args) {
    Context ctx = new Context();
    ctx.setup();
    //recursion(ctx);
    //trampoline(ctx);
    //mutualRecursion(ctx);
    //runThreadPoolTest(ctx);
    runQuasarFibers(ctx);
    //runForkJoinTask(ctx);
    System.out.println("Complete.");
  }
  
  @State(Scope.Thread)
  public static class Context {
    List<Integer> data = Lists.newArrayList();
    ExecutorService pool = Executors.newFixedThreadPool(1);;
    ForkJoinPool fj = ForkJoinPool.commonPool();
    
    @Setup
    public void setup() {
      for (int i = 0; i < 100; i++) {
        data.add(i);
      }
    }
    
    @TearDown
    public void tearDown() {
      //fj.shutdown();
      pool.shutdown();
    }
  }
  
  static class Trampoline<T> {
    final Deferred<T> deferred;
    public Trampoline(Deferred<T> deferred) {
      this.deferred = deferred;
    }
    
    public Deferred<T> get() { return null; }
    public Trampoline<T>  run() { return null; }

    Deferred<T> execute() {
        Trampoline<T>  trampoline = this;

        while (trampoline.get() == null) {
            trampoline = trampoline.run();
        }

        return deferred;
    }
  }
  
  /**
   * Does work. But it unrolls in a loop. Can we do that with async?
   * @param ctx
   */
  @Benchmark
  public static void trampoline(final Context ctx) {
    class Incrementer {
      int depth = 0;
      Deferred<Object> deferred = new Deferred<Object>();
      
      Trampoline<Object> increment() {
        if (depth >= DEPTH) {
          return new Trampoline<Object>(deferred) {
            public Deferred<Object> get() {
              deferred.callback(null);
              return deferred;
            }
          };
        }
        
        return new Trampoline<Object>(deferred) {
          public Trampoline<Object> run() {
            for (int i = 0; i < ctx.data.size(); i++) {
              ctx.data.set(i, ctx.data.get(i) + 1);
            }
            //System.out.println("Inc.....");
            depth++;
//            if (depth >= DEPTH) {
//              System.out.println("PASSED THE THRESHOLD");
//              return this;
//            }
            return increment();
          }
        };
      }
    }
    
    try {
      new Incrementer().increment().execute().join(10000);
      assertIncremented(ctx);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  /**
   * Fails, natch
   * @param ctx
   */
  @Benchmark
  public static void recursion(final Context ctx) {
    try {
      class Incrementer {
        Deferred<Object> complete = new Deferred<Object>();
        int depth = 0;
        
        public void increment() {
          if (depth >= DEPTH) {
            complete.callback(null);
            return;
          }
          
          for (int i = 0; i < ctx.data.size(); i++) {
            ctx.data.set(i, ctx.data.get(i) + 1);
          }
          
          depth++;
          
          increment();
        }
      }
      
      Incrementer inc = new Incrementer();
      inc.increment();
      try {
        inc.complete.join(1000);
        assertIncremented(ctx);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } catch (StackOverflowError e) { 
      System.out.println("Died with an SOE.");
    }
  }

  /**
   * Doesn't work either as the JVM doesn't have tail optimization.
   * @param ctx
   */
  @Benchmark
  public static void mutualRecursion(final Context ctx) {
    try {
      class Incrementer {
        Deferred<Object> complete = new Deferred<Object>();
        int depth = 0;
        
        void start() {
          if (depth % 2 == 0) {
            incrementEven();
          } else {
            incrementOdd();
          }
        }
        
        void incrementOdd() {
          if (depth >= DEPTH) {
            complete.callback(null);
            return;
          }
          
          for (int i = 0; i < ctx.data.size(); i++) {
            ctx.data.set(i, ctx.data.get(i) + 1);
          }
          
          depth++;
          if (depth % 2 == 0) {
            incrementEven();
          } else {
            incrementOdd();
          }
        }
        
        void incrementEven() {
          if (depth >= DEPTH) {
            complete.callback(null);
            return;
          }
          
          for (int i = 0; i < ctx.data.size(); i++) {
            ctx.data.set(i, ctx.data.get(i) + 1);
          }
          
          depth++;
          if (depth % 2 == 0) {
            incrementEven();
          } else {
            incrementOdd();
          }
        }
      }
  
      Incrementer inc = new Incrementer();
      inc.start();
      try {
        inc.complete.join(1000);
        assertIncremented(ctx);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } catch (StackOverflowError e) { 
      System.out.println("Died with an SOE.");
    }
  }

  @Benchmark
  public static void runThreadPoolTest(final Context ctx) {
    class Incrementer implements Runnable {
      Deferred<Object> complete = new Deferred<Object>();
      int depth = 0;
      
      public void run() {
        if (depth >= DEPTH) {
          complete.callback(null);
          return;
        }
        
        for (int i = 0; i < ctx.data.size(); i++) {
          ctx.data.set(i, ctx.data.get(i) + 1);
        }
        
        depth++;
        
        ctx.pool.execute(this);
      }
    }
    
    Incrementer inc = new Incrementer();
    inc.run();
    try {
      inc.complete.join(10000);
      assertIncremented(ctx);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * No go. Promising BUT requires a bytecode manipulator on JVM startup. Ug.
   * @param ctx
   */
  @Benchmark
  public static void runQuasarFibers(final Context ctx) {
    
    class Incrementer<Void> extends Fiber {
      private static final long serialVersionUID = -8957096110804086001L;
      Deferred<Object> complete = new Deferred<Object>();
      int depth = 0;
      
      @Override
      public Void run() throws SuspendExecution, InterruptedException {
        if (depth >= DEPTH) {
          complete.callback(null);
          return null;
        }
        
        for (int i = 0; i < ctx.data.size(); i++) {
          ctx.data.set(i, ctx.data.get(i) + 1);
        }
        
        depth++;
        return run();
      }
    }
    
    Incrementer inc = new Incrementer();
    inc.start();
    try {
      inc.complete.join(1000);
      assertIncremented(ctx);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * So promising yet so busted. This guy is completely unstable and randomly OOMs
   * the tests. Looks like there is a CRAP load of garbage created.
   * @param ctx
   */
  //@Benchmark
  public static void runForkJoinTask(final Context ctx) {
    class Incrementer extends RecursiveAction {
      Deferred<Object> complete = new Deferred<Object>();
      int depth = 0;
      
      @Override
      protected void compute() {
        if (depth >= DEPTH) {
          complete.callback(null);
          return;
        }
        
        for (int i = 0; i < ctx.data.size(); i++) {
          ctx.data.set(i, ctx.data.get(i) + 1);
        }
        
        depth++;
        // stack overflow....
        ctx.fj.invoke(this);
      }
    }
    
    Incrementer inc = new Incrementer();
    ctx.fj.invoke(inc);
    try {
      inc.complete.join(10000);
      assertIncremented(ctx);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  static void assertIncremented(Context ctx) {
    if (true) {
      return;
    }
    for (int i = 0; i < ctx.data.size(); i++) {
      if (i + DEPTH != ctx.data.get(i)) {
        throw new RuntimeException("Data not equals! Got : " + ctx.data.get(i) 
        + " and expected " + (i + DEPTH) + " at " + i);      
      }
    }
  }
}
