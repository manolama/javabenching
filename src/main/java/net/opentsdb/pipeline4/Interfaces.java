package net.opentsdb.pipeline4;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeStamp;

public class Interfaces {
  public enum QueryMode {
    SINGLE,             /** All in one. Tight limits. */ 
    CLIENT_STREAM,      /** Client is responsible for requesting the next chunk. */
    SERVER_SYNC_STREAM, /** Server will auto push AFTER the current chunk is done. */
    SERVER_ASYNC_STREAM /** Server will push as fast as it can. */
  }
  
  public interface QResult {
    public Collection<TS<?>> series();
  }
  
  public interface StreamListener {
    public void onComplete();
    public void onNext(QResult next);
    public void onError(Throwable t);
  }
  
  public interface TS<T extends TimeSeriesDataType> {
    public TimeSeriesId id();
    public Iterator<TSValue<T>> iterator();
    public TypeToken<T> type();
    public void close(); // release resources
  }
  
  public interface TSValue<T extends TimeSeriesDataType> {
    public TimeStamp timestamp();
    public T value();
  }
  
  public interface TSProcessor {
    
  }
  
  public interface NType extends TimeSeriesDataType {
    public enum NumberType {
      INTEGER,
      DOUBLE,
      UNSIGNED_INTEGER,
      // eventually we could have decimal, etc.
    }
    
    public static final TypeToken<NType> TYPE = TypeToken.of(NType.class);
    
    public NumberType numberType();
    public long longValue();
    public double doubleValue();
    public long unsignedLongValue();
    public double toDouble();
    
  }
  
  public interface StringType extends TimeSeriesDataType {
    /** The data type reference to pass around. */
    public static final TypeToken<StringType> TYPE = 
        TypeToken.of(StringType.class);
    
    /** Returns a list as processors (group by, downsample) may accumulate strings. */
    public List<String> values();
  }
  
  public interface QExecutionPipeline {
    public void setListener(StreamListener listener);
    public StreamListener getListener();
    public void fetchNext();
    public QExecutionPipeline getMultiPassClone(StreamListener listener);
    public void setCache(boolean cache);
    public QueryMode getMode();
  }

}
