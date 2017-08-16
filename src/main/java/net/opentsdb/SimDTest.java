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

import com.google.common.collect.Lists;

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
      public final int[] values = new int[SIZE];
      public final int[] results = new int[SIZE];
      
      public final double[] dvalues = new double[SIZE];
      public final double[] dresults = new double[SIZE];
      
      public final List<Integer> lvalues = Lists.newArrayListWithCapacity(SIZE);
      public final List<Integer> lresults = Lists.newArrayListWithCapacity(SIZE);

      public final List<Double> ldvalues = Lists.newArrayListWithCapacity(SIZE);
      public final List<Double> ldresults = Lists.newArrayListWithCapacity(SIZE);
      
      @Setup
      public void setup()
      {
          Random random = new Random();
          for (int i = 0; i < SIZE; i++) {
              values[i] = random.nextInt(Integer.MAX_VALUE / 32);
              lvalues.add(values[i]);
              dvalues[i] = random.nextDouble();
              ldvalues.add(dvalues[i]);
          }
      }
  }
  
  @Benchmark
  public int[] myIntegerArrayIncrement(Context context)
  {
      for (int i = 0; i < SIZE; i++) {
          context.results[i] = context.values[i] + 1;
      }
      return context.results;
  }
  
  @Benchmark
  public double[] myDoubleArrayIncrement(Context context)
  {
      for (int i = 0; i < SIZE; i++) {
          context.dresults[i] = context.dvalues[i] + 1;
      }
      return context.dresults;
  }
  
  @Benchmark
  public List<Integer> myIntegerListIncrement(Context context)
  {
      // sucks
      context.lresults.clear();
      for (int i : context.lvalues) {
          context.lresults.add(i + 1);
      }
      return context.lresults;
  }
  
  @Benchmark
  public List<Double> myDoubleListIncrement(Context context)
  {
      // sucks
    context.ldresults.clear();
      for (double i : context.ldvalues) {
          context.ldresults.add(i + 1);
      }
      return context.ldresults;
  }
}
