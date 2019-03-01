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

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import com.google.common.reflect.TypeToken;

import avro.shaded.com.google.common.collect.Lists;
import net.opentsdb.GroupByAndSum.Context;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;

@State(Scope.Thread)
public class NumbersV3 {

  
  public static void main(String[] args) {
    long fbit = Long.MAX_VALUE;
    fbit = fbit >> 7;
    System.out.println(fbit);
    System.out.println(Long.toBinaryString(fbit));
    
    Context ctx = new Context();
    ctx.setup();
    
    iterateMutables(ctx, null);
    System.out.println("----------------------");
    iterateIteratorNewVector(ctx, null);
  }
  
  @State(Scope.Thread)
  public static class Context {
    int values = 64;
    long start_ts = 1551466782;
    
    Random rnd = new Random(System.currentTimeMillis());
    List<MutableNumericValue> mutables = Lists.newArrayList();
    long[] new_vector = new long[values * 2];
    
    @Setup
    public void setup() {
      long ts = start_ts;
      int idx = 0;
      for (int i = 0; i < values; i++) {
        MutableNumericValue v = new MutableNumericValue();
        if (rnd.nextBoolean()) {
          long mts = (ts * 1000) + rnd.nextInt(999);
          if (rnd.nextBoolean()) {
            // double value
            double d = rnd.nextDouble();
            v.reset(new MillisecondTimeStamp(mts), d);
            mutables.add(v);
            
            mts <<= 8;
            mts |= 1; // float
            mts |= 2; // millis;
            new_vector[idx++] = mts;
            new_vector[idx++] = Double.doubleToRawLongBits(d);
          } else {
            // long value
            long l = rnd.nextLong();
            v.reset(new MillisecondTimeStamp(mts), l);
            mutables.add(v);

            mts <<= 8;
            mts |= 2; // millis
            new_vector[idx++] = mts;
            new_vector[idx++] = l;
          }
        } else {
          long sts = ts;
          if (rnd.nextBoolean()) {
            // double value
            double d = rnd.nextDouble();
            v.reset(new SecondTimeStamp(sts), d);
            mutables.add(v);
            
            sts <<= 8;
            sts |= 1; // float
            new_vector[idx++] = sts;
            new_vector[idx++] = Double.doubleToRawLongBits(d);
          } else {
            // long value
            long l = rnd.nextLong();
            v.reset(new SecondTimeStamp(sts), l);
            mutables.add(v);
            
            sts <<= 8;
            new_vector[idx++] = sts;
            new_vector[idx++] = l;
          }
        }
        
        ts += 60;
      }
    }
  }
  
  @Benchmark
  public static void iterateMutables(Context context, Blackhole blackHole) {
    for (final MutableNumericValue v : context.mutables) {
      if (blackHole == null) {
        System.out.println(v.timestamp().msEpoch() + "  " + (v.isInteger() ? 
            Long.toString(v.longValue()) : Double.toString(v.doubleValue())));
        continue;
      }
      blackHole.consume(v.timestamp().msEpoch());
      if (v.isInteger()) {
        blackHole.consume(v.longValue());
      } else {
        blackHole.consume(v.doubleValue());
      }
    }
  }
  
  @Benchmark
  public static void iterateNewVector(Context context, Blackhole blackHole) {
    for (int i = 0; i < context.new_vector.length; i += 2) {
      boolean is_float = (context.new_vector[i] & 1) != 0;
      boolean is_milli = (context.new_vector[i] & 2) != 0;
      
      long ts = context.new_vector[i] >> 8;
      if (!is_milli) {
        ts *= 1000;
      }
      
      if (blackHole == null) {
        System.out.println(ts + "  " + (is_float ? 
            Double.toString(Double.longBitsToDouble(context.new_vector[i + 1])) :
              Long.toString(context.new_vector[i + 1])));
        continue;
      }
      blackHole.consume(ts);
      
      if (is_float) {
        blackHole.consume(Double.longBitsToDouble(context.new_vector[i + 1]));
      } else {
        blackHole.consume(context.new_vector[i + 1]);
      }
    }
  }
  
  @Benchmark
  public static void iterateIteratorNewVector(Context context, Blackhole blackHole) {
    
    
    class MyIterator implements Iterator<TimeSeriesValue<NumericType>>, TimeSeriesValue<NumericType>, NumericType {
      int idx = 0;
      boolean is_float;
      MillisecondTimeStamp ts = new MillisecondTimeStamp(0L);
      
      @Override
      public boolean hasNext() {
        return idx < context.new_vector.length;
      }

      @Override
      public TimeSeriesValue<NumericType> next() {
        is_float = (context.new_vector[idx] & 1) != 0;
        boolean is_milli = (context.new_vector[idx] & 2) != 0;
        long ts = context.new_vector[idx] >> 8;
        if (!is_milli) {
          ts *= 1000;
        }
        this.ts.updateMsEpoch(ts);
        idx += 2;
        return this;
      }

      @Override
      public double doubleValue() {
        if (is_float) {
          return Double.longBitsToDouble(context.new_vector[idx - 1]);
        }
        return 0;
      }

      @Override
      public boolean isInteger() {
        return !is_float;
      }

      @Override
      public long longValue() {
        if (!is_float) {
          return context.new_vector[idx - 1];
        }
        return 0;
      }

      @Override
      public double toDouble() {
        if (is_float) {
          return Double.longBitsToDouble(context.new_vector[idx - 1]);
        }
        return context.new_vector[idx - 1];
      }

      @Override
      public TypeToken<NumericType> type() {
        return NumericType.TYPE;
      }

      @Override
      public TimeStamp timestamp() {
        return ts;
      }

      @Override
      public NumericType value() {
        return this;
      }
      
    }
    
    MyIterator it = new MyIterator();
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = it.next();
      if (blackHole == null) {
        System.out.println(v.timestamp().msEpoch() + "  " + (
            v.value().isInteger() ? Long.toString(v.value().longValue()) : 
              Double.toString(v.value().doubleValue())));
        continue;
      }
      
      blackHole.consume(v.timestamp().msEpoch());
      if (v.value().isInteger()) {
        blackHole.consume(v.value().longValue());
      } else {
        blackHole.consume(v.value().doubleValue());
      }
    }
  }
}
