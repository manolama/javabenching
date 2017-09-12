package net.opentsdb;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.Control;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.utils.DateTime;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
public class Async {

  @State(Scope.Thread)
  public static class Context
  {
    public List<Double> source;
    
    @Setup
    public void setup() {
      source = Lists.newArrayList();
      Random rnd = new Random(DateTime.currentTimeMillis());
      for (int i = 0; i < 100; i++) {
        source.add(rnd.nextDouble());
      }
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
  public void runCallableFuture(Context context, Blackhole blackHole) throws Exception {
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
