package net.opentsdb.pipeline10;

import java.util.Iterator;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.pipeline10.Interfaces.*;

public class Abstracts {
  public static abstract class BaseTS implements TS {
    protected TSByteId id;
    
    public BaseTS(final TSByteId id) {
      this.id = id;
    }
    
    @Override
    public TSByteId id() {
      return id;
    }
    
    public abstract class LocalIterator<T extends TimeSeriesDataType> implements Iterator<TSValue<T>> {
      protected int idx;
      protected int time_idx;
    }
  }
}
