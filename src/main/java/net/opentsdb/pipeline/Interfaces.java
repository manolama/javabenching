package net.opentsdb.pipeline;

import java.util.Collection;
import java.util.Iterator;

import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pipeline.Interfaces.StreamListener;

public class Interfaces {
  private Interfaces() { }
  
  public enum QueryMode {
    SINGLE,             /** All in one. Tight limits. */ 
    CLIENT_STREAM,      /** Client is responsible for requesting the next chunk. */
    SERVER_SYNC_STREAM, /** Server will auto push AFTER the current chunk is done. */
    SERVER_ASYNC_STREAM /** Server will push as fast as it can. */
  }
  
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
    
  }

  public interface QExecutionPipeline {
    public void setListener(StreamListener listener);
    public StreamListener getListener();
    public void fetchNext();
    public QExecutionPipeline getMultiPassClone(StreamListener listener);
    public void setCache(boolean cache);
    public QueryMode getMode();
  }
  
  public interface QExecutor {
    
  }
}
