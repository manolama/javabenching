package net.opentsdb;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.Control;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.utils.DateTime;

/**
 * This one is to compare our old Deferreds to native Callable Futures and other
 * implementations..
 * 
 * Interestingly, the native code is quicker but generate more garbage whereas
 * the deferreds are a tad slower but less wasteful. And Listenable Futures are 
 * the slowest but still more efficient than the native.
 * 
 * <code>
 * Benchmark                                                 Mode  Cnt      Score      Error   Units
Benchmark                                                    Mode  Cnt      Score      Error   Units
Async.runCompletableFuture                                   avgt   15  34996.510 ±  544.929   ns/op
Async.runCompletableFuture:·gc.alloc.rate                    avgt   15    443.545 ±    6.845  MB/sec
Async.runCompletableFuture:·gc.alloc.rate.norm               avgt   15  24424.051 ±    0.146    B/op
Async.runCompletableFuture:·gc.churn.PS_Eden_Space           avgt   15    445.751 ±   23.651  MB/sec
Async.runCompletableFuture:·gc.churn.PS_Eden_Space.norm      avgt   15  24547.522 ± 1292.412    B/op
Async.runCompletableFuture:·gc.churn.PS_Survivor_Space       avgt   15      0.144 ±    0.055  MB/sec
Async.runCompletableFuture:·gc.churn.PS_Survivor_Space.norm  avgt   15      7.944 ±    3.124    B/op
Async.runCompletableFuture:·gc.count                         avgt   15    204.000             counts
Async.runCompletableFuture:·gc.time                          avgt   15    102.000                 ms
Async.runDeferred                                            avgt   15  38571.001 ±  924.078   ns/op
Async.runDeferred:·gc.alloc.rate                             avgt   15    332.386 ±    7.939  MB/sec
Async.runDeferred:·gc.alloc.rate.norm                        avgt   15  20176.054 ±    0.154    B/op
Async.runDeferred:·gc.churn.PS_Eden_Space                    avgt   15    331.962 ±   43.131  MB/sec
Async.runDeferred:·gc.churn.PS_Eden_Space.norm               avgt   15  20155.875 ± 2580.361    B/op
Async.runDeferred:·gc.churn.PS_Survivor_Space                avgt   15      0.123 ±    0.077  MB/sec
Async.runDeferred:·gc.churn.PS_Survivor_Space.norm           avgt   15      7.475 ±    4.621    B/op
Async.runDeferred:·gc.count                                  avgt   15    124.000             counts
Async.runDeferred:·gc.time                                   avgt   15     68.000                 ms
Async.runListenableFuture                                    avgt   15  48668.644 ±  648.898   ns/op
Async.runListenableFuture:·gc.alloc.rate                     avgt   15    413.443 ±    5.524  MB/sec
Async.runListenableFuture:·gc.alloc.rate.norm                avgt   15  31680.072 ±    0.199    B/op
Async.runListenableFuture:·gc.churn.PS_Eden_Space            avgt   15    417.796 ±   21.436  MB/sec
Async.runListenableFuture:·gc.churn.PS_Eden_Space.norm       avgt   15  32014.644 ± 1600.476    B/op
Async.runListenableFuture:·gc.churn.PS_Survivor_Space        avgt   15      0.161 ±    0.049  MB/sec
Async.runListenableFuture:·gc.churn.PS_Survivor_Space.norm   avgt   15     12.295 ±    3.749    B/op
Async.runListenableFuture:·gc.count                          avgt   15    191.000             counts
Async.runListenableFuture:·gc.time                           avgt   15     97.000                 ms
</code>
 *
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
public class Async {

  public static void main(final String[] args) {
    Context ctx = new Context();
    ctx.setup();
    try {
      runListenableFuture(ctx, null);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  @State(Scope.Thread)
  public static class Context
  {
    public List<Double> source;
    public ExecutorService pool;
    
    @Setup
    public void setup() {
      source = Lists.newArrayList();
      Random rnd = new Random(DateTime.currentTimeMillis());
      for (int i = 0; i < 100; i++) {
        source.add(rnd.nextDouble());
      }
      pool = Executors.newFixedThreadPool(1);
    }
    
    @TearDown
    public void tearDown() {
      pool.shutdownNow();
    }
  }
  
  @Benchmark
  public void runDeferred(Context context, Blackhole blackHole) throws Exception {
    for (double v : context.source) {
      
      class ToString implements Callback<String, Double> {
        @Override
        public String call(Double arg) throws Exception {
          return Double.toString(arg);
        }
      }
      
      class DoubleUp implements Callback<Double, Double> {
        @Override
        public Double call(Double arg) throws Exception {
          return arg * arg;
        }
      }
      
      Deferred<Double> deferred = new Deferred<Double>();
      deferred.callback(v);
      String result = deferred
          .addCallback(new DoubleUp())
          .addCallback(new ToString())
          .join();
      if (blackHole == null) {
        System.out.println(result);
      } else {
        blackHole.consume(result);
      }
      
    }
  }
  
  @Benchmark
  public static void runListenableFuture(Context context, Blackhole blackHole) throws Exception {
    for (double v : context.source) {
      
      class ToString implements AsyncFunction<Double, String> {
        @Override
        public ListenableFuture<String> apply(Double input) throws Exception {
          SettableFuture<String> f = SettableFuture.<String>create();
          f.set(Double.toString(input));
          return f;
        }
      }
      
      class DoubleUp implements AsyncFunction<Double, Double> {
        @Override
        public ListenableFuture<Double> apply(Double input) throws Exception {
          SettableFuture<Double> f = SettableFuture.<Double>create();
          f.set(input * input);
          return f;
        }
      }
      
      SettableFuture<Double> f = SettableFuture.<Double>create();
      f.set(v);
      
      String result = Futures.transform(Futures.transformAsync(f, new DoubleUp()), new ToString()).get();
      if (blackHole == null) {
        System.out.println(result);
      } else {
        blackHole.consume(result);
      }
      
    }
  }
  
  @Benchmark
  public void runCompletableFuture(Context context, Blackhole blackHole) throws Exception {
    for (double v : context.source) {
//      class ToString implements BiFunction<Double, Exception, String> {
//        @Override
//        public String apply(Double t, Exception u) {
//          return Double.toString(t);
//        }
//      }
//      
//      class DoubleUp implements BiFunction<Double, Exception, Double> {
//        @Override
//        public Double apply(Double t, Exception u) {
//          return t * t;
//        }
//      }
      
      class ToString implements Function<Double, String> {
        @Override
        public String apply(Double t) {
          return Double.toString(t);
        }
      }
      
      class DoubleUp implements Function<Double, Double> {
        @Override
        public Double apply(Double t) {
          return t * t;
        }
      }
      
      CompletableFuture<Double> cf = new CompletableFuture<Double>();
      cf.complete(v);
      final String result = cf.thenApply(new DoubleUp())
                              .thenApply(new ToString())
                              .get();
      if (blackHole == null) {
        System.out.println(result);
      } else {
        blackHole.consume(result);
      }
    }
  }
}
