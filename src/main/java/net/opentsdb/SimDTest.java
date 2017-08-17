package net.opentsdb;

import java.util.List;
import java.util.Random;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.google.common.collect.Lists;

import net.opentsdb.utils.Bytes;

/**
 * Benching SIMD, etc from Piotr's blog: http://prestodb.rocks/code/simd/
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class SimDTest {
  public static final int SIZE = 1024;
  
  @State(Scope.Thread)
  public static class Context
  {
      public final long[] values = new long[SIZE];
      public final long[] results = new long[SIZE];
      
      public final double[] dvalues = new double[SIZE];
      public final double[] dresults = new double[SIZE];
      
      public final List<Long> lvalues = Lists.newArrayListWithCapacity(SIZE);
      public final List<Long> lresults = Lists.newArrayListWithCapacity(SIZE);

      public final List<Double> ldvalues = Lists.newArrayListWithCapacity(SIZE);
      public final List<Double> ldresults = Lists.newArrayListWithCapacity(SIZE);
      
      @Setup
      public void setup()
      {
          Random random = new Random();
          for (int i = 0; i < SIZE; i++) {
              values[i] = random.nextLong();
              lvalues.add(values[i]);
              dvalues[i] = random.nextDouble();
              ldvalues.add(dvalues[i]);
          }
      }
  }
  
  @Benchmark
  public long[] myIntegerArrayIncrement(Context context, Blackhole blackHole)
  {
      for (int i = 0; i < SIZE; i++) {
          context.results[i] = context.values[i] + 1;
      }
      return context.results;
  }
  
  @Benchmark
  public double[] myDoubleArrayIncrement(Context context, Blackhole blackHole)
  {
      for (int i = 0; i < SIZE; i++) {
          context.dresults[i] = context.dvalues[i] + 1;
      }
      return context.dresults;
  }
  
  @Benchmark
  public List<Long> myIntegerListIncrement(Context context, Blackhole blackHole)
  {
      // sucks
      context.lresults.clear();
      for (long i : context.lvalues) {
          context.lresults.add(i + 1);
      }
      return context.lresults;
  }
  
  @Benchmark
  public List<Double> myDoubleListIncrement(Context context, Blackhole blackHole)
  {
      // sucks
    context.ldresults.clear();
      for (double i : context.ldvalues) {
          context.ldresults.add(i + 1);
      }
      return context.ldresults;
  }
  
  static class MyDP {
    final long ts;
    final byte[] value;
    final boolean isFloat;
    
    public MyDP(final int v) {
      ts = 1;
      value = Bytes.fromLong(v);
      isFloat = false;
    }
    
    public MyDP(final double v) {
      ts = 1;
      value = Bytes.fromLong(Double.doubleToRawLongBits(v));
      isFloat = true;
    }
    
    public boolean isFloat() {
      return isFloat;
    }
    
    public double getDouble() {
      return Double.longBitsToDouble(Bytes.getLong(value));
    }
    
    public long getLong() {
      return Bytes.getLong(value);
    }
  }
}
