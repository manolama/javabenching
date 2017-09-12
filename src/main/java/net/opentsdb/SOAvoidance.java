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
