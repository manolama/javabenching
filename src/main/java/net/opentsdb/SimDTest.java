package net.opentsdb;

import java.util.Random;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

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

      @Setup
      public void setup()
      {
          Random random = new Random();
          for (int i = 0; i < SIZE; i++) {
              values[i] = random.nextInt(Integer.MAX_VALUE / 32);
          }
      }
  }
  
  @Benchmark
  public int[] increment(Context context)
  {
      for (int i = 0; i < SIZE; i++) {
          context.results[i] = context.values[i] + 1;
      }
      return context.results;
  }
}
