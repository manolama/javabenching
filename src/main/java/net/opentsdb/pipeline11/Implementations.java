// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.pipeline11;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.google.common.reflect.TypeToken;

import avro.shaded.com.google.common.base.Objects;
import avro.shaded.com.google.common.collect.Lists;
import net.openhft.hashing.LongHashFunction;
import net.opentsdb.common.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.pipeline11.Abstracts.*;
import net.opentsdb.pipeline11.Interfaces.*;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes;

public class Implementations {
  
  public static class DefaultTimeSpec implements TimeSpec {

    TimeStamp start;
    TimeStamp end;
    long interval;
    ChronoUnit units;
    
    public DefaultTimeSpec(TimeStamp start, TimeStamp end, long interval, ChronoUnit units) {
      this.start = start;
      this.end = end;
      this.interval = interval;
      this.units = units;
    }
    
    @Override
    public TimeStamp start() {
      return start;
    }

    @Override
    public TimeStamp end() {
      return end;
    }

    @Override
    public long interval() {
      return interval;
    }

    @Override
    public ChronoUnit units() {
      return units;
    }

    @Override
    public void updateTimestamp(int idx, TimeStamp ts) {
      // TEMP assume ms
      ts.updateMsEpoch(start.msEpoch() + (long) (interval * idx));
    }
    
  }
  
  public static class ArrayBackedLongTS extends BaseTS {
    protected byte[] dps;
    
    public ArrayBackedLongTS(TSByteId id, TimeSpec time_spec) {
      super(id);
    }

    @Override
    public Iterator<TSValue<?>> iterator(TypeToken<?> type) {
      return new It();
    }
    
    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NType.TYPE);
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
    }
    
    public void nextChunk(byte[] data) {
      dps = data;
    }
    
    class It extends LocalIterator implements TSValue<NType>, NType {
      TimeStamp ts = new MillisecondTimeStamp(0);
      long value = 0;
      
      @Override
      public TSValue<NType> next() {
        ts.updateMsEpoch(Bytes.getLong(dps, idx));
        idx += 8;
        value = Bytes.getLong(dps, idx);
        idx += 8;
        return this;
      }

      @Override
      public TimeStamp timestamp() {
        return ts;
      }

      @Override
      public NumberType numberType() {
        return NumberType.INTEGER;
      }

      @Override
      public long longValue() {
        return value;
      }

      @Override
      public double doubleValue() {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public long unsignedLongValue() {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public double toDouble() {
        return (double) value;
      }

      @Override
      public NType value() {
        return this;
      }

      @Override
      public boolean hasNext() {
        return idx < dps.length;
      }

      @Override
      public TypeToken<? extends TimeSeriesDataType> type() {
        // TODO Auto-generated method stub
        return null;
      }
      
    }
  }

//  public static class ArrayBackedStringTS extends BaseTS {
//    TimeStamp ts = new MillisecondTimeStamp(0);
//    String value = null;
//    
//    public ArrayBackedStringTS(TSByteId id, TimeSpec time_spec) {
//      super(id, time_spec);
//    }
//
//    @Override
//    public Iterator<TSValue<?>> iterator(TypeToken<?> type) {
//      return new It();
//    }
//    
//    @Override
//    public Collection<TypeToken<?>> types() {
//      return Lists.newArrayList(StringType.TYPE);
//    }
//
//    @Override
//    public void close() {
//      // TODO Auto-generated method stub
//      
//    }
//    
//    class It extends LocalIterator implements TSValue<StringType>, StringType {
//
//      @Override
//      public TSValue<StringType> next() {
//        ts.updateMsEpoch(Bytes.getLong(dps, idx));
//        idx += 8;
//        byte[] s = new byte[3];
//        System.arraycopy(dps, idx, s, 0, 3);
//        idx += 3;
//        value = new String(s, Const.UTF8_CHARSET);
//        return this;
//      }
//
//      @Override
//      public TimeStamp timestamp() {
//        return ts;
//      }
//
//      @Override
//      public List<String> values() {
//        return Lists.newArrayList(value);
//      }
//
//      @Override
//      public StringType value() {
//        return this;
//      }
//      
//    }
//  }

}
