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
package net.opentsdb.pipeline2;

import java.util.List;

import com.google.common.reflect.TypeToken;

import avro.shaded.com.google.common.collect.Lists;
import net.opentsdb.common.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline2.Abstracts.*;
import net.opentsdb.utils.Bytes;

public class Implementations {

  public static class LocalNumericTS extends MyTS<NumericType> {

    public LocalNumericTS(TimeSeriesStringId id) {
      super(id);
    }

    @Override
    public TypeToken<NumericType> type() {
      return NumericType.TYPE;
    }

    @Override
    public List<TimeSeriesValue<NumericType>> data() {
      List<TimeSeriesValue<NumericType>> results = Lists.newArrayList();
      int idx = 0;
      while (idx < dps.length) {
        TimeStamp ts = new MillisecondTimeStamp(Bytes.getLong(dps, idx));
        idx += 8;
        results.add(new MutableNumericValue(ts, Bytes.getLong(dps, idx)));
        idx += 8;
      }
      return results;
    }
    
  }
  
  public static class ArrayBackedStringTS extends MyTS<StringType> {

    public ArrayBackedStringTS(TimeSeriesStringId id) {
      super(id);
    }

    @Override
    public TypeToken<StringType> type() {
      return StringType.TYPE;
    }

    @Override
    public List<TimeSeriesValue<StringType>> data() {
      List<TimeSeriesValue<StringType>> results = Lists.newArrayList();
      int idx = 0;
      while(idx < dps.length) {
        TimeStamp ts = new MillisecondTimeStamp(Bytes.getLong(dps, idx));
        idx += 8;
        byte[] s = new byte[3];
        System.arraycopy(dps, idx, s, 0, 3);
        idx += 3;
        results.add(new MutableStringType(ts, Lists.newArrayList(new String(s, Const.UTF8_CHARSET))));      }
      return results;
    }
    
  }

  public static class MutableStringType extends StringType implements TimeSeriesValue<StringType> {
    /** The timestamp for this data point. */
    private TimeStamp timestamp;
    
    private List<String> values = Lists.newArrayList();
    
    public MutableStringType() {
      timestamp = new MillisecondTimeStamp(0);
    }
    
    public MutableStringType(TimeStamp ts, List<String> values) {
      this.timestamp = ts;
      this.values = values;
    }
    
    public void reset(TimeStamp ts, List<String> values, int reals) {
      timestamp.update(ts);
      this.values = values;
    }
    
    @Override
    public List<String> values() {
      return values;
    }
    
    @Override
    public TimeStamp timestamp() {
      return timestamp;
    }

    @Override
    public StringType value() {
      return this;
    }

    @Override
    public TypeToken<StringType> type() {
      return StringType.TYPE;
    }
  }
}
