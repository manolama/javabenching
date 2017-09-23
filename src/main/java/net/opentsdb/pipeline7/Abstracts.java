package net.opentsdb.pipeline7;

import java.util.Iterator;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.pipeline7.Interfaces.*;

public class Abstracts {
  public static abstract class BaseTS<T extends TimeSeriesDataType> implements TS<T> {
    protected byte[] dps;
    protected TSByteId id;
    
    public BaseTS(final TSByteId id) {
      this.id = id;
    }
    
    @Override
    public TSByteId id() {
      return id;
    }
    
    public void setChunk(final byte[] data) {
      dps = data;
    }
    
    public abstract class LocalIterator implements Iterator<TSValue<T>> {
      protected int idx;
      
      @Override
      public boolean hasNext() {
        return idx < dps.length;
      }
    }
  }
}