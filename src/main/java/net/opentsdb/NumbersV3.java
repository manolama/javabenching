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
//    long fbit = Long.MAX_VALUE;
//    fbit = fbit >> 7;
//    System.out.println(fbit);
//    System.out.println(Long.toBinaryString(fbit));
    
    Context ctx = new Context();
    ctx.setup();
    
    //iterateMutables(ctx, null);
    System.out.println("----------------------");

    // works for 2 bytes aligned
//    long v = 0x3C03;
//    System.out.println(Long.toBinaryString(v));
//    byte b = 0;
//    b = (byte) (v >>> 8);
//    
//    byte b2 = 0;
//    b2 = (byte) (v >>> 0);
//    System.out.println(Integer.toBinaryString(b));
//    System.out.println(Integer.toBinaryString(b2));
//    
//    long new_v = (b & 0xFFL) << 8;
//    new_v |= (b2 & 0xFFL) << 0;
//    System.out.println(v + "  =>  " + b + "  =>  " + new_v);
    
    // funky offset with 4 8 4
/*    long v = 0x3C03;
    System.out.println(v + "  " + Long.toBinaryString(v));
    System.out.println("-----------");
    byte b = (byte) (v >>> 12);
    byte b2 = (byte) (v >>> 4);
    byte b3 = (byte) (v << 4);
    System.out.println(Integer.toBinaryString(b));
    System.out.println(Integer.toBinaryString(b2));
    System.out.println(Integer.toBinaryString(b3));
    
    // restore
    long new_v = (b & 0xFFL) << 12;
    new_v |= (b2 & 0xFFL) << 4;
    new_v |= (b3 >> 4);
    System.out.println(new_v);
  */  
    iterateNewVector(ctx, null);
    //iterateBytes(ctx, null);
  }
  
  @State(Scope.Thread)
  public static class Context {
    int values = 3600;
    long start_ts = 1551466782;
    
    Random rnd = new Random(System.currentTimeMillis());
    List<MutableNumericValue> mutables = Lists.newArrayList();
    long[] new_vector = new long[values * 2];
    byte[] super_packing = new byte[values * 17];
    
    @Setup
    public void setup() {
      long ts = start_ts;
      int idx = 0;
      
      int byte_idx = 0;
      int bit_idx = 0;
      byte cur_byte = 0;
      
      for (int i = 0; i < values; i++) {
        MutableNumericValue v = new MutableNumericValue();
        if (rnd.nextBoolean()) {
          long mts = (ts * 1000) + rnd.nextInt(999);
          if (rnd.nextBoolean()) {
            // double value
            double d = rnd.nextDouble();
            v.reset(new MillisecondTimeStamp(mts), d);
            mutables.add(v);
            
            // LONG VECTOR
            mts &= 0xFFFFFFFFFFFFFFFL; // clear
            mts |= 0x8000000000000000L; // float
            mts |= 0x4000000000000000L; // millis;
            new_vector[idx++] = mts;
            new_vector[idx++] = Double.doubleToRawLongBits(d);
            
            // PACKED BYTES
            if (bit_idx == 0) {
              cur_byte = (byte) 0xC0;
              bit_idx = 4;
            } else {
              cur_byte |= (byte) 0xC;
              bit_idx = 0;
              super_packing[byte_idx++] = cur_byte;
            }
            
            boolean was_offset = bit_idx == 4;
            for (int x = 0; x < 7; x++) {
              if (bit_idx == 0) {
                super_packing[byte_idx++] = (byte) (mts << 8);
              } else {
                cur_byte |= (byte) (mts << 4);
                super_packing[byte_idx++] = cur_byte;
                bit_idx = 0;
              }
            }
            
            // last one for the timestamp
            if (was_offset) {
              cur_byte = (byte) (mts << 8);
              bit_idx = 4;
            } else {
              super_packing[byte_idx++] = (byte) (mts << 8);
            }
            
            // now for the value
            long lv = Double.doubleToLongBits(d);
            was_offset = bit_idx == 4;
            for (int x = 0; x < 7; x++) {
              if (bit_idx == 0) {
                super_packing[byte_idx++] = (byte) (lv << 8);
              } else {
                cur_byte |= (byte) (lv << 4);
                super_packing[byte_idx++] = cur_byte;
                bit_idx = 0;
              }
            }
            
            // last one for the value
            if (was_offset) {
              cur_byte = 0;
              cur_byte = (byte) (lv << 8);
              bit_idx = 4;
            } else {
              super_packing[byte_idx++] = (byte) (lv << 8);
            }
            
          } else {
            // long value ----------------------------------------------------
            long l = rnd.nextLong();
            v.reset(new MillisecondTimeStamp(mts), l);
            mutables.add(v);

            mts &= 0xFFFFFFFFFFFFFFFL; // clear
            //mts |= 0x8000000000000000L; // float
            mts |= 0x4000000000000000L; // millis;
            new_vector[idx++] = mts;
            new_vector[idx++] = l;
            
            // PACKED BYTES
            if (bit_idx == 0) {
              cur_byte = (byte) 0x40;
              bit_idx = 4;
            } else {
              cur_byte |= (byte) 0x4;
              bit_idx = 0;
              super_packing[byte_idx++] = cur_byte;
            }
            
            boolean was_offset = bit_idx == 4;
            for (int x = 0; x < 7; x++) {
              if (bit_idx == 0) {
                super_packing[byte_idx++] = (byte) (mts << 8);
              } else {
                cur_byte |= (byte) (mts << 4);
                super_packing[byte_idx++] = cur_byte;
                bit_idx = 0;
              }
            }
            
            // last one for the timestamp
            if (was_offset) {
              cur_byte = (byte) (mts << 8);
              bit_idx = 4;
            } else {
              super_packing[byte_idx++] = (byte) (mts << 8);
            }
            
            // now for the value
            was_offset = bit_idx == 4;
            for (int x = 0; x < 7; x++) {
              if (bit_idx == 0) {
                super_packing[byte_idx++] = (byte) (l << 8);
              } else {
                cur_byte |= (byte) (l << 4);
                super_packing[byte_idx++] = cur_byte;
                bit_idx = 0;
              }
            }
            
            // last one for the value
            if (was_offset) {
              cur_byte = 0;
              cur_byte = (byte) (l << 8);
              bit_idx = 4;
            } else {
              super_packing[byte_idx++] = (byte) (l << 8);
            }
          }
        } else {
          long sts = ts;
          if (rnd.nextBoolean()) {
            // double value
            double d = rnd.nextDouble();
            v.reset(new SecondTimeStamp(sts), d);
            mutables.add(v);
            
            sts &= 0xFFFFFFFFFFFFFFFL; // clear
            sts |= 0x8000000000000000L; // float
            //sts |= 0x4000000000000000L; // millis;
            new_vector[idx++] = sts;
            new_vector[idx++] = Double.doubleToRawLongBits(d);
            
            // PACKED BYTES
            if (bit_idx == 0) {
              cur_byte = (byte) 0x80;
              bit_idx = 4;
            } else {
              cur_byte |= (byte) 0x8;
              bit_idx = 0;
              super_packing[byte_idx++] = cur_byte;
            }
            
            boolean was_offset = bit_idx == 4;
            for (int x = 0; x < 3; x++) {
              if (bit_idx == 0) {
                super_packing[byte_idx++] = (byte) (sts << 8);
              } else {
                cur_byte |= (byte) (sts << 4);
                super_packing[byte_idx++] = cur_byte;
                bit_idx = 0;
              }
            }
            
            // last one for the timestamp
            if (was_offset) {
              cur_byte = (byte) (sts << 8);
              bit_idx = 4;
            } else {
              super_packing[byte_idx++] = (byte) (sts << 8);
            }
            
            // now for the value
            long lv = Double.doubleToLongBits(d);
            was_offset = bit_idx == 4;
            for (int x = 0; x < 7; x++) {
              if (bit_idx == 0) {
                super_packing[byte_idx++] = (byte) (lv << 8);
              } else {
                cur_byte |= (byte) (lv << 4);
                super_packing[byte_idx++] = cur_byte;
                bit_idx = 0;
              }
            }
            
            // last one for the value
            if (was_offset) {
              cur_byte = 0;
              cur_byte = (byte) (lv << 8);
              bit_idx = 4;
            } else {
              super_packing[byte_idx++] = (byte) (lv << 8);
            }
            
          } else {
            // long value
            long l = rnd.nextLong();
            v.reset(new SecondTimeStamp(sts), l);
            mutables.add(v);
            
            sts &= 0xFFFFFFFFFFFFFFFL; // clear
            //sts |= 0x8000000000000000L; // float
            //sts |= 0x4000000000000000L; // millis;
            new_vector[idx++] = sts;
            new_vector[idx++] = l;
            sts = v.timestamp().epoch();
            
            // PACKED BYTES
            if (bit_idx == 0) {
              cur_byte = 0;
              bit_idx = 4;
            } else {
              cur_byte |= (byte) 0x0;
              bit_idx = 0;
              super_packing[byte_idx++] = cur_byte;
            }
            
            boolean was_offset = bit_idx == 4;
            int shift = was_offset ? 20 : 24;
            for (int x = 0; x < 3; x++) {
              if (was_offset) {
                cur_byte |= (byte) (sts >>> shift);
                super_packing[byte_idx++] = cur_byte;
                bit_idx = 0;
                shift -= 8;
              } else {
                super_packing[byte_idx++] = (byte) (sts >>> shift);
                shift -= 8;
              }
            }
            
            // last one for the timestamp
            if (was_offset) {
              cur_byte = 0;
              cur_byte |= (byte) (sts << 4);
              bit_idx = 4;
            } else {
              super_packing[byte_idx++] = (byte) (sts >>> shift);
            }
            
            // now for the value
            was_offset = bit_idx == 4;
            for (int x = 0; x < 7; x++) {
              if (bit_idx == 0) {
                super_packing[byte_idx++] = (byte) (l >>> 8);
              } else {
                cur_byte |= (byte) (l >>> 4);
                super_packing[byte_idx++] = cur_byte;
                bit_idx = 0;
              }
            }
            
            // last one for the value
            if (was_offset) {
              cur_byte = 0;
              cur_byte = (byte) (l << 8);
              bit_idx = 4;
            } else {
              super_packing[byte_idx++] = (byte) (l << 8);
            }
            
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
      boolean is_float = (context.new_vector[i] & 0x8000000000000000L) != 0;
      boolean is_milli = (context.new_vector[i] & 0x4000000000000000L) != 0;
      
      long ts = context.new_vector[i] & 0xFFFFFFFFFFFFFFFL;
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
        is_float = (context.new_vector[idx] & 0x8000000000000000L) != 0;
        boolean is_milli = (context.new_vector[idx] & 0x4000000000000000L) != 0;
        long ts = context.new_vector[idx] & 0xFFFFFFFFFFFFFFFL;
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

  @Benchmark
  public static void iterateBytes(Context context, Blackhole blackHole) {
    boolean offset = false;
    byte state = 0; // 0 == header, 1 == timestamp, 2 == value
    boolean is_float = false;
    boolean in_millis = false;
    int read_values = 0;
    int idx = 0;
    
    long timestamp = 0;
    long value = 0;
    
    while (true) {
      if (state == 0) {
        // READ HEADER
        if (offset) {
          is_float = (context.super_packing[idx] & (byte) 0X8) != 0;
          in_millis = (context.super_packing[idx] & (byte) 0x4) != 0;
          offset = false;
          idx++;
        } else {
          is_float = (context.super_packing[idx] & (byte) 0X80) != 0;
          in_millis = (context.super_packing[idx] & (byte) 0x40) != 0;
          offset = true;
        }
        
        state = 1;
        timestamp = 0;
        value = 0;
      } else if (state == 1) {
        // READ A TIMESTAMP
        if (in_millis) {
          int off = offset ? 52 : 56;
          boolean was_offset = offset;
          if (offset) {
            timestamp = (context.super_packing[idx++] & 0xFFL) << off;
            offset = false;
          } else {
            timestamp = (context.super_packing[idx++] & 0xFFL) << off;
          }
          off -= 8;
          
          for (int i = 1; i < 7; i++) {
            timestamp |= (context.super_packing[idx++] & 0xFFL) << off;
            off -= 8;
          }
          
          if (was_offset) {
            timestamp = (context.super_packing[idx++]) >> 4;
            offset = true;
          } else {
            timestamp |= (context.super_packing[idx++] & 0xFFL) << off;
          }
        } else {
          boolean was_offset = offset;
          int off = offset ? 20 : 24;
          if (offset) {
            timestamp = (context.super_packing[idx++] & 0xFFL) << off;
            offset = false;
          } else {
            timestamp |= (context.super_packing[idx++] & 0xFFL) << off;
          }
          off -= 8;
          
          for (int i = 1; i < 3; i++) {
            timestamp |= (context.super_packing[idx++] & 0xFFL) << off;
            off -= 8;
          }
          
          if (was_offset) {
            timestamp |= (context.super_packing[idx++] & 0xFFL) >> 4;
            offset = true;
          } else {
            timestamp |= (context.super_packing[idx++] & 0xFFL) << off;
          }
        }
        
        state = 2;
      } else {
        // READ A VALUE
        boolean was_offset = offset;
        if (offset) {
          value <<= (context.super_packing[idx++] & (byte) 0xF) << 4;
          offset = false;
        } else {
          value <<= context.super_packing[idx++];
        }
        
        for (int i = 1; i < 7; i++) {
          value <<= context.super_packing[idx++];
        }
        
        if (was_offset) {
          value <<= (context.super_packing[idx] << 4);
          offset = true;
        } else {
          value <<= context.super_packing[idx++];
        }
        
        state = 0;
        
        // WOOT!
        if (blackHole == null) {
          System.out.println(timestamp + "  " + (!is_float ? 
              Long.toString(value) : Double.toString(Double.longBitsToDouble(value))));
        } else {
          blackHole.consume(timestamp);
          if (is_float) {
            blackHole.consume(Double.longBitsToDouble(value));
          } else {
            blackHole.consume(value);
          }
        }
        
        read_values++;
        if (read_values >= context.values) {
          break;
        }
      }
    }
  }
}
