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
    public final double[] dvalues = new double[SIZE];
    public final List<Long> lvalues = Lists.newArrayListWithCapacity(SIZE);
    public final List<Double> ldvalues = Lists.newArrayListWithCapacity(SIZE);
    public final MyDP[] ldps = new MyDP[SIZE];
    public final MyDP[] ddps = new MyDP[SIZE];
     
    @Setup
    public void setup()
    {
      Random random = new Random();
      for (int i = 0; i < SIZE; i++) {
        values[i] = random.nextLong();
        lvalues.add(values[i]);
        ldps[i] = new MyDP(values[i]);
        dvalues[i] = random.nextDouble();
        ldvalues.add(dvalues[i]);
        ddps[i] = new MyDP(dvalues[i]);
      }
    }
  }
  
  @Benchmark
  public void myIntegerArrayIncrement(Context context, Blackhole blackHole)
  {
    final long[] results = new long[SIZE];
    for (int i = 0; i < SIZE; i++) {
      results[i] = context.values[i] + 1;
    }
    blackHole.consume(results);
  }
  
  @Benchmark
  public void myDoubleArrayIncrement(Context context, Blackhole blackHole)
  {
    final double[] results = new double[SIZE];
    for (int i = 0; i < SIZE; i++) {
      results[i] = context.dvalues[i] + 1;
    }
    blackHole.consume(results);
  }
  
  @Benchmark
  public void myIntegerListIncrement(Context context, Blackhole blackHole)
  {
    // sucks
    final List<Long> results = Lists.newArrayListWithCapacity(SIZE);
    for (long i : context.lvalues) {
      results.add(i + 1);
    }
    blackHole.consume(results);
  }
  
  @Benchmark
  public void myDoubleListIncrement(Context context, Blackhole blackHole)
  {
    // sucks
    final List<Double> results = Lists.newArrayListWithCapacity(SIZE);
    for (double i : context.ldvalues) {
      results.add(i + 1);
    }
    blackHole.consume(results);
  }
  
  @Benchmark
  public void myIntegerArrayDP(Context context, Blackhole blackHole) {
    final MyDP[] results = new MyDP[SIZE];
    for (int i = 0; i < SIZE; i++) {
      if (context.ldps[i].isFloat()) {
        results[i] = new MyDP(context.ldps[i].getDouble() + 1);
      } else {
        results[i] = new MyDP(context.ldps[i].getLong() + 1);
      }
    }
    blackHole.consume(results);
  }
  
  @Benchmark
  public void myDoubleArrayDP(Context context, Blackhole blackHole) {
    final MyDP[] results = new MyDP[SIZE];
    for (int i = 0; i < SIZE; i++) {
      if (context.ddps[i].isFloat()) {
        results[i] = new MyDP(context.ddps[i].getDouble() + 1);
      } else {
        results[i] = new MyDP(context.ddps[i].getLong() + 1);
      }
    }
    blackHole.consume(results);
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
