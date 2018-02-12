// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.pipeline10;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.utils.ByteSet;

public interface Interfaces {

  public interface TSByteId extends Comparable<TSByteId>{
    public String alias();
    public List<String> namespaces();
    public List<String> metrics();
    public Map<String, String> tags();
    public List<String> aggregatedTags();
    public List<String> disjointTags();
    public ByteSet uniqueIds();
  }
  
  public enum QueryMode {
    SINGLE,             /** All in one. Tight limits. */ 
    CLIENT_STREAM,      /** Client is responsible for requesting the next chunk. */
    SERVER_SYNC_STREAM, /** Server will auto push AFTER the current chunk is done. */
    SERVER_ASYNC_STREAM /** Server will push as fast as it can. */
  }
  
  public interface TimeSpec {
    public TimeStamp start();
    public TimeStamp end();
    public long interval();
    public ChronoUnit units();
    public void updateTimestamp(int idx, TimeStamp ts);
  }
  
  public interface QResult {
    public TimeSpec timeSpec();
    public Collection<TS> series();
  }
  
  public interface StreamListener {
    public void onComplete();
    public void onNext(QResult next);
    public void onError(Throwable t);
  }
  
  public interface TS {
    public TSByteId id();
    
    // kaithinkiq - only emit dps with quality measurement > 90%
    // hijklmno  - show dps within annotation range
    public Iterator<TSValue<? extends TimeSeriesDataType>> iterator(TypeToken<?> type);
    public Collection<TypeToken<?>> types();
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
