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
package net.opentsdb.pipeline;

import java.util.Iterator;
import java.util.List;

import com.google.common.reflect.TypeToken;

import avro.shaded.com.google.common.collect.Lists;
import net.opentsdb.common.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline.Abstracts.*;
import net.opentsdb.utils.Bytes;

public class Implementations {

  public static class ArrayBackedLongTS extends MyTS<NumericType> implements Iterator<TimeSeriesValue<NumericType>> {
    TimeStamp ts = new MillisecondTimeStamp(0);
    MutableNumericType dp;
    
    public ArrayBackedLongTS(final TimeSeriesId id) {
      super(id);
      dp = new MutableNumericType();
    }
    
    @Override
    public Iterator<TimeSeriesValue<NumericType>> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return idx < dps.length;
    }

    @Override
    public TimeSeriesValue<NumericType> next() {
      ts.updateMsEpoch(Bytes.getLong(dps, idx));
      idx += 8;
      dp.reset(ts, Bytes.getLong(dps, idx));
      idx += 8;
      return dp;
    }

    @Override
    public TypeToken<NumericType> type() {
      return NumericType.TYPE;
    }
  }
  
  public static class ArrayBackedStringTS extends MyTS<StringType> implements Iterator<TimeSeriesValue<StringType>> {
    TimeStamp ts = new MillisecondTimeStamp(0);
    MutableStringType dp;
    
    public ArrayBackedStringTS(TimeSeriesId id) {
      super(id);
      dp = new MutableStringType();
    }

    @Override
    public Iterator<TimeSeriesValue<StringType>> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return idx < dps.length;
    }

    @Override
    public TimeSeriesValue<StringType> next() {
      ts.updateMsEpoch(Bytes.getLong(dps, idx));
      idx += 8;
      byte[] s = new byte[3];
      System.arraycopy(dps, idx, s, 0, 3);
      idx += 3;
      dp.reset(ts, Lists.newArrayList(new String(s, Const.UTF8_CHARSET)));
      return dp;
    }
    
    @Override
    public TypeToken<StringType> type() {
      return StringType.TYPE;
    }
  }
  
  public static class MutableStringType extends StringType implements TimeSeriesValue<StringType> {

    /** The timestamp for this data point. */
    private TimeStamp timestamp;
    
    private List<String> values = Lists.newArrayList();
    
    public MutableStringType() {
      timestamp = new MillisecondTimeStamp(0);
    }
    
    public void reset(TimeStamp ts, List<String> values) {
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
