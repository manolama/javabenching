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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
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
    public final long[] values2 = new long[SIZE];
    public final double[] dvalues = new double[SIZE];
    public final List<Long> lvalues = Lists.newArrayListWithCapacity(SIZE);
    public final List<Double> ldvalues = Lists.newArrayListWithCapacity(SIZE);
    public final MyDP[] ldps = new MyDP[SIZE];
    public final MyDP[] ddps = new MyDP[SIZE];
    public final MyDPPrimitive[] pldps = new MyDPPrimitive[SIZE];
    public final MyDPPrimitive[] pddps = new MyDPPrimitive[SIZE];
    public final LongArrayList full = new LongArrayList(SIZE);
    public final DoubleArrayList fudl = new DoubleArrayList(SIZE);
     
    @Setup
    public void setup()
    {
      Random random = new Random();
      for (int i = 0; i < SIZE; i++) {
        values[i] = random.nextLong();
        values2[i] = random.nextLong();
        lvalues.add(values[i]);
        ldps[i] = new MyDP(values[i]);
        pldps[i] = new MyDPPrimitive(values[i]);
        full.add(values[i]);
        dvalues[i] = random.nextDouble();
        ldvalues.add(dvalues[i]);
        ddps[i] = new MyDP(dvalues[i]);
        pddps[i] = new MyDPPrimitive(dvalues[i]);
        fudl.add(dvalues[i]);
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
  
  @Benchmark
  public void myIntegerArrayDPPrimitive(Context context, Blackhole blackHole) {
    final MyDPPrimitive[] results = new MyDPPrimitive[SIZE];
    for (int i = 0; i < SIZE; i++) {
      if (context.pldps[i].isFloat()) {
        results[i] = new MyDPPrimitive(context.pldps[i].getDouble() + 1);
      } else {
        results[i] = new MyDPPrimitive(context.pldps[i].getLong() + 1);
      }
    }
    blackHole.consume(results);
  }
  
  @Benchmark
  public void myDoubleArrayDPPrimitive(Context context, Blackhole blackHole) {
    final MyDPPrimitive[] results = new MyDPPrimitive[SIZE];
    for (int i = 0; i < SIZE; i++) {
      if (context.pddps[i].isFloat()) {
        results[i] = new MyDPPrimitive(context.pddps[i].getDouble() + 1);
      } else {
        results[i] = new MyDPPrimitive(context.pddps[i].getLong() + 1);
      }
    }
    blackHole.consume(results);
  }
  
  @Benchmark
  public void myIntegerFastUtilIncrement(Context context, Blackhole blackHole)
  {
    // sucks
    final LongArrayList results = new LongArrayList(SIZE);
    for (long i : context.full) {
      results.add(i + 1);
    }
    blackHole.consume(results);
  }
  
  @Benchmark
  public void myDoubleFastUtilIncrement(Context context, Blackhole blackHole)
  {
    // sucks
    final DoubleArrayList results = new DoubleArrayList(SIZE);
    for (double i : context.fudl) {
      results.add(i + 1);
    }
    blackHole.consume(results);
  }
  
  @Benchmark
  public void myVectorSum(Context context, Blackhole blackHole) {
    final long[] results = new long[SIZE];
    for (int i = 0; i < SIZE; i++) {
      results[i] = context.values[i] + context.values2[i];
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
  
  static class MyDPPrimitive {
    final long ts;
    final long lv;
    final double dv;
    final boolean isFloat;
    
    public MyDPPrimitive(final long v) {
      ts = 1;
      lv = v;
      dv = 0;
      isFloat = false;
    }
    
    public MyDPPrimitive(final double v) {
      ts = 1;
      lv = 0;
      dv = v;
      isFloat = true;
    }
    
    public boolean isFloat() {
      return isFloat;
    }
    
    public long getLong() {
      return lv;
    }
    
    public double getDouble() {
      return dv;
    }
  }
}
