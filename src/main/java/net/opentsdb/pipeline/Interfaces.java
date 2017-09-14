package net.opentsdb.pipeline;

import java.util.Collection;
import java.util.Iterator;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericType;

public class Interfaces {
  private Interfaces() { }
  
  public interface QResult {
    public Collection<TS<?>> series();
    public Throwable exception();
    public boolean hasException();
  }
  
  public interface StreamListener {
    public void onComplete();
    public void onNext(QResult next);
    public void onError(Throwable t);
  }
  
  /** Time series interface */
  public interface TS<T extends TimeSeriesDataType> {
    public TimeSeriesId id();
    public Iterator<TimeSeriesValue<T>> iterator();
    public void setCache(boolean cache);
  }
  
  public interface TSProcessor<T extends TimeSeriesDataType> {
    public TS<T> source();
  }
  
  public interface QPipeline {
    public void setListener(StreamListener l);
    public boolean endOfStream();
    public void fetchNext();
  }
  
  public interface QExecution {
    public void setListener(StreamListener listener);
    public boolean endOfStream();
    public void fetchNext();
  }
  
  public interface QExecutor {
    
  }
}
