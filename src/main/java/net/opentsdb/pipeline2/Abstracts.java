package net.opentsdb.pipeline2;

import java.util.List;

import com.google.common.reflect.TypeToken;

import avro.shaded.com.google.common.collect.Lists;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.pipeline2.Interfaces.TS;

public class Abstracts {
  public static abstract class MyTS<T extends TimeSeriesDataType> implements TS<T> {
    protected List<TimeSeriesValue<T>> dps;
    protected TimeSeriesId id;
    
    public MyTS(final TimeSeriesId id) {
      this.id = id;
    }
    
    @Override
    public TimeSeriesId id() {
      return id;
    }
    
    @Override
    public List<TimeSeriesValue<T>> data() {
      return dps;
    }
    
    public void addData(List<TimeSeriesValue<?>> data) {
      dps = Lists.newArrayList();
      for (final TimeSeriesValue<? extends TimeSeriesDataType> v : data) {
        dps.add((TimeSeriesValue<T>) v);
      }
    }
    
  }
  
  public static abstract class StringType implements TimeSeriesDataType {
    /** The data type reference to pass around. */
    public static final TypeToken<StringType> TYPE = 
        TypeToken.of(StringType.class);
    
    /** Returns a list as processors (group by, downsample) may accumulate strings. */
    public abstract List<String> values();
  }
  
}
