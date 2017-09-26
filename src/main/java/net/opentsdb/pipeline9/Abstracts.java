package net.opentsdb.pipeline9;

import java.util.Iterator;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.pipeline9.Interfaces.*;

public class Abstracts {
  public static abstract class BaseTS<T extends TimeSeriesDataType> implements TS<T> {
    protected byte[] dps;
    protected TimeSpec time_spec;
    protected TSByteId id;
    
    public BaseTS(final TSByteId id, TimeSpec time_spec) {
      this.id = id;
      this.time_spec = time_spec;
    }
    
    @Override
    public TSByteId id() {
      return id;
    }
    
    public void nextChunk(final byte[] data) {
      dps = data;
    }
    
    public abstract class LocalIterator implements Iterator<TSValue<T>> {
      protected int idx;
      protected int time_idx;
      
      @Override
      public boolean hasNext() {
        return idx < dps.length;
      }
    }
  }
}
