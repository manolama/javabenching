package net.opentsdb.pipeline2;

import java.util.List;

import com.google.common.reflect.TypeToken;

import avro.shaded.com.google.common.collect.Lists;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline2.Abstracts.*;

public class Implementations {

  public static class LocalNumericTS extends MyTS<NumericType> {

    public LocalNumericTS(TimeSeriesId id) {
      super(id);
    }

    @Override
    public TypeToken<NumericType> type() {
      return NumericType.TYPE;
    }
    
  }
  
  public static class ListBackedStringTS extends MyTS<StringType> {

    public ListBackedStringTS(TimeSeriesId id) {
      super(id);
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
    
    public MutableStringType(TimeSeriesId id, TimeStamp ts, List<String> values) {
      this.id = id;
      this.timestamp = ts;
      this.values = values;
      reals = 1;
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
