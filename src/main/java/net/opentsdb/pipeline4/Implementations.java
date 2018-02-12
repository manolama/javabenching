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
package net.opentsdb.pipeline4;

import java.util.Iterator;
import java.util.List;

import com.google.common.reflect.TypeToken;

import avro.shaded.com.google.common.collect.Lists;
import net.opentsdb.common.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.pipeline4.Abstracts.*;
import net.opentsdb.pipeline4.Interfaces.*;
import net.opentsdb.utils.Bytes;

public class Implementations {

  public static class ArrayBackedLongTS extends BaseTS<NType> {
    
    public ArrayBackedLongTS(TimeSeriesId id) {
      super(id);
    }

    @Override
    public Iterator<TSValue<NType>> iterator() {
      return new It();
    }

    @Override
    public TypeToken<NType> type() {
      return NType.TYPE;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
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
      
    }
  }

  public static class ArrayBackedStringTS extends BaseTS<StringType> {
    TimeStamp ts = new MillisecondTimeStamp(0);
    String value = null;
    
    public ArrayBackedStringTS(TimeSeriesId id) {
      super(id);
    }

    @Override
    public Iterator<TSValue<StringType>> iterator() {
      return new It();
    }

    @Override
    public TypeToken<StringType> type() {
      return StringType.TYPE;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
    }
    
    class It extends LocalIterator implements TSValue<StringType>, StringType {

      @Override
      public TSValue<StringType> next() {
        ts.updateMsEpoch(Bytes.getLong(dps, idx));
        idx += 8;
        byte[] s = new byte[3];
        System.arraycopy(dps, idx, s, 0, 3);
        idx += 3;
        value = new String(s, Const.UTF8_CHARSET);
        return this;
      }

      @Override
      public TimeStamp timestamp() {
        return ts;
      }

      @Override
      public List<String> values() {
        return Lists.newArrayList(value);
      }

      @Override
      public StringType value() {
        return this;
      }
      
    }
  }
}
