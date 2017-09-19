package net.opentsdb.pipeline;

import java.util.Collection;
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
import net.opentsdb.pipeline.Interfaces.*;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Pair;

public class Implementations {

  public static class ArrayBackedLongTS extends MyTS<NumericType> implements Iterator<TimeSeriesValue<NumericType>> {
    TimeStamp ts = new MillisecondTimeStamp(0);
    MutableNumericType dp;
    
    public ArrayBackedLongTS(final TimeSeriesId id) {
      super(id);
      dp = new MutableNumericType(id);
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
      dp.reset(ts, Bytes.getLong(dps, idx), 1);
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
      dp = new MutableStringType(id);
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
      dp.reset(ts, Lists.newArrayList(new String(s, Const.UTF8_CHARSET)), 1);
      return dp;
    }
    
    @Override
    public TypeToken<StringType> type() {
      return StringType.TYPE;
    }
  }
  
  public static class MutableStringType extends StringType implements TimeSeriesValue<StringType> {
    /** A reference to the ID of the series this data point belongs to. */
    private final TimeSeriesId id;
    
    /** The timestamp for this data point. */
    private TimeStamp timestamp;
    
    private List<String> values = Lists.newArrayList();
    
    /** The number of real values behind this data point. */
    private int reals = 0;
    
    public MutableStringType(TimeSeriesId id) {
      this.id = id;
      timestamp = new MillisecondTimeStamp(0);
    }
    
    public void reset(TimeStamp ts, List<String> values, int reals) {
      timestamp.update(ts);
      this.values = values;
      this.reals = reals;
    }
    
    @Override
    public List<String> values() {
      return values;
    }
    
    @Override
    public TimeSeriesId id() {
      return id;
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
    public int realCount() {
      return reals;
    }

    @Override
    public TypeToken<?> type() {
      return StringType.TYPE;
    }

    @Override
    public TimeSeriesValue<StringType> getCopy() {
      // TODO Auto-generated method stub
      return null;
    }
  }
}
