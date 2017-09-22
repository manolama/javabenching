package net.opentsdb.pipeline4;

import java.util.Iterator;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.pipeline4.Interfaces.*;

public class Abstracts {

  public static abstract class BaseTS<T extends TimeSeriesDataType> implements TS<T> {
    protected byte[] dps;
    protected TimeSeriesId id;
    
    
    public BaseTS(final TimeSeriesId id) {
      this.id = id;
    }
    
    @Override
    public TimeSeriesId id() {
      return id;
    }
    
    public void nextChunk(final byte[] data) {
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
