package net.opentsdb.pipeline;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.pipeline.Interfaces.TS;

public class Abstracts {

  public static abstract class MyTS<T extends TimeSeriesDataType> implements TS<T> {
    protected byte[] dps;
    protected List<byte[]> dps_cached;
    protected TimeSeriesId id;
    protected int idx;
    
    public MyTS(final TimeSeriesId id) {
      this.id = id;
    }
    
    @Override
    public TimeSeriesId id() {
      return id;
    }
    
    @Override
    public void setCache(boolean cache) {
      if (cache) {
        dps_cached = Lists.newArrayList();
      }
    }
    
    public void nextChunk(final byte[] data) {
      dps = data;
      if (dps_cached != null) {
        dps_cached.add(data);
      }
      idx = 0;
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
