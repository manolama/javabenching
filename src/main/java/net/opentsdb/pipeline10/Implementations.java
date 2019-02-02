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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.google.common.reflect.TypeToken;

import avro.shaded.com.google.common.base.Objects;
import avro.shaded.com.google.common.collect.Lists;
import net.openhft.hashing.LongHashFunction;
import net.opentsdb.common.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.pipeline10.Abstracts.*;
import net.opentsdb.pipeline10.Interfaces.*;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes;

public class Implementations {

  public static class StringTSByteId implements TSByteId {
    
    /** An optional alias. */
    protected String alias;
    
    /** An optional list of namespaces for the ID. */
    protected List<String> namespaces;
    
    /** An optional list of metrics for the ID. */
    protected List<String> metrics;
    
    /** A map of tag key/value pairs for the ID. */
    protected Map<String, String> tags;
    
    /** An optional list of aggregated tags for the ID. */
    protected List<String> aggregated_tags;
    
    /** An optional list of disjoint tags for the ID. */
    protected List<String> disjoint_tags;
    
    /** A list of unique IDs rolled up into the ID. */
    protected ByteSet unique_ids;
    
    public StringTSByteId() { }
    
    @Override
    public String alias() {
      return null;
    }

    @Override
    public List<String> namespaces() {
      return namespaces;
    }

    @Override
    public List<String> metrics() {
      return metrics;
    }

    @Override
    public Map<String, String> tags() {
      return tags;
    }

    @Override
    public List<String> aggregatedTags() {
      return aggregated_tags;
    }

    @Override
    public List<String> disjointTags() {
      return disjoint_tags;
    }

    @Override
    public ByteSet uniqueIds() {
      return unique_ids;
    }

    @Override
    public int compareTo(TSByteId o) {
      return ComparisonChain.start()
          .compare(alias, o.alias())
          .compare(namespaces, o.namespaces(), 
              Ordering.<String>natural().lexicographical().nullsFirst())
          .compare(metrics, o.metrics(), 
              Ordering.<String>natural().lexicographical().nullsFirst())
          .compare(tags, o.tags(), STR_MAP_CMP)
          .compare(aggregated_tags, o.aggregatedTags(), 
              Ordering.<String>natural().lexicographical().nullsFirst())
          .compare(disjoint_tags, o.disjointTags(), 
              Ordering.<String>natural().lexicographical().nullsFirst())
          .compare(unique_ids, o.uniqueIds(), ByteSet.BYTE_SET_CMP)
          .result();
    }
    
    public static final StringMapComparator STR_MAP_CMP = new StringMapComparator();
    static class StringMapComparator implements Comparator<Map<String, String>> {
      private StringMapComparator() { }
      @Override
      public int compare(final Map<String, String> a, final Map<String, String> b) {
        if (a == b || a == null && b == null) {
          return 0;
        }
        if (a == null && b != null) {
          return -1;
        }
        if (b == null && a != null) {
          return 1;
        }
        if (a.size() > b.size()) {
          return -1;
        }
        if (b.size() > a.size()) {
          return 1;
        }
        for (final Entry<String, String> entry : a.entrySet()) {
          final String b_value = b.get(entry.getKey());
          if (b_value == null && entry.getValue() != null) {
            return 1;
          }
          final int cmp = entry.getValue().compareTo(b_value);
          if (cmp != 0) {
            return cmp;
          }
        }
        return 0;
      }
    }
    
    /** Just the one's were using for this test */
    public void addMetric(final String m) {
      if (metrics == null) {
        metrics = Lists.newArrayListWithCapacity(1);
      }
      metrics.add(m);
      if (metrics.size() > 0) {
        Collections.sort(metrics);
      }
    }
    
    public void addAggTag(final String t) {
      if (aggregated_tags == null) {
        aggregated_tags = Lists.newArrayListWithCapacity(1);
      }
      aggregated_tags.add(t);
      if (aggregated_tags.size() > 0) {
        Collections.sort(aggregated_tags);
      }
    }
    
    public void addTag(final String k, final String v) {
      if (tags == null) {
        tags = new TreeMap<String, String>();
      }
      tags.put(k, v);
    }
    
    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (o == null || !(o instanceof TSByteId))
        return false;
      
      final TSByteId id = (TSByteId) o;
      
      // long slog through byte arrays.... :(
      if (!Objects.equal(alias, id.alias())) {
        return false;
      }
      if (!Objects.equal(namespaces(), id.namespaces())) {
        return false;
      }
      if (!Objects.equal(metrics(), id.metrics())) {
        return false;
      }
      if (!Objects.equal(tags(), id.tags())) {
        return false;
      }
      if (!Objects.equal(aggregatedTags(), id.aggregatedTags())) {
        return false;
      }
      if (!Objects.equal(disjointTags(), id.disjointTags())) {
        return false;
      }
      if (unique_ids != null && id.uniqueIds() == null) {
        return false;
      }
      if (unique_ids == null && id.uniqueIds() != null) {
        return false;
      }
      if (unique_ids != null && id.uniqueIds() != null) {
        return unique_ids.equals(id.uniqueIds());
      }
      return true;
    }
  
    protected int cached_hash;
    @Override
    public int hashCode() {
      if (cached_hash == 0) {
        cached_hash = Long.hashCode(buildHashCode());
      }
      return cached_hash;
    }
    
    /** @return A HashCode object for deterministic, non-secure hashing */
  public long buildHashCode() {
      StringBuilder buf = new StringBuilder();
      if (alias != null) {
        buf.append(alias);
      }
      if (namespaces != null) {
        for (final String ns : namespaces) {
          buf.append(ns);
        }
      }
      if (metrics != null) {
        for (final String m : metrics) {
          buf.append(m);
        }
      }
      if (tags != null) {
        for (final Entry<String, String> pair : tags.entrySet()) {
          buf.append(pair.getKey());
          buf.append(pair.getValue());
        }
      }
      if (aggregated_tags != null) {
        for (final String t : aggregated_tags) {
          buf.append(t);
        }
      }
      if (disjoint_tags != null) {
        for (final String t : disjoint_tags) {
          buf.append(t);
        }
      }
      return (int) LongHashFunction.xx_r39().hashChars(buf.toString());
    }
  
    @Override
    public String toString() {
      final StringBuilder buf = new StringBuilder()
          .append("alias=")
          .append(alias != null ? alias : "null")
          .append(", namespaces=")
          .append(namespaces)
          .append(", metrics=")
          .append(metrics)
          .append(", tags=")
          .append(tags)
          .append(", aggregated_tags=")
          .append(aggregated_tags)
          .append(", disjoint_tags=")
          .append(disjoint_tags)
          .append(", uniqueIds=")
          .append(unique_ids);
      return buf.toString();
    }
  }
  
  public static class DefaultTimeSpec implements TimeSpec {

    TimeStamp start;
    TimeStamp end;
    long interval;
    ChronoUnit units;
    
    public DefaultTimeSpec(TimeStamp start, TimeStamp end, long interval, ChronoUnit units) {
      this.start = start;
      this.end = end;
      this.interval = interval;
      this.units = units;
    }
    
    @Override
    public TimeStamp start() {
      return start;
    }

    @Override
    public TimeStamp end() {
      return end;
    }

    @Override
    public long interval() {
      return interval;
    }

    @Override
    public ChronoUnit units() {
      return units;
    }

    @Override
    public void updateTimestamp(int idx, TimeStamp ts) {
      // TEMP assume ms
      ts.updateMsEpoch(start.msEpoch() + (long) (interval * idx));
    }
    
  }
  
  public static class ArrayBackedLongTS extends BaseTS {
    protected byte[] dps;
    
    public ArrayBackedLongTS(TSByteId id, TimeSpec time_spec) {
      super(id);
    }

    @Override
    public Iterator<TSValue<?>> iterator(TypeToken<?> type) {
      return new It();
    }
    
    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NType.TYPE);
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
    }
    
    public void nextChunk(byte[] data) {
      dps = data;
    }
    
    class It extends LocalIterator implements TSValue<NType>, NType {
      TimeStamp ts = new MillisecondTimeStamp(0);
      long value = 0;
      
      @Override
      public TSValue<NType> next() {
        ts.updateMsEpoch(Bytes.getLong(dps, idx));
        idx += 8;
        value = Bytes.getLong(dps, idx);
        idx += 8;
        return this;
      }

      @Override
      public TimeStamp timestamp() {
        return ts;
      }

      @Override
      public NumberType numberType() {
        return NumberType.INTEGER;
      }

      @Override
      public long longValue() {
        return value;
      }

      @Override
      public double doubleValue() {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public long unsignedLongValue() {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public double toDouble() {
        return (double) value;
      }

      @Override
      public NType value() {
        return this;
      }

      @Override
      public boolean hasNext() {
        return idx < dps.length;
      }

      @Override
      public TypeToken<? extends TimeSeriesDataType> type() {
        // TODO Auto-generated method stub
        return null;
      }
      
    }
  }

//  public static class ArrayBackedStringTS extends BaseTS {
//    TimeStamp ts = new MillisecondTimeStamp(0);
//    String value = null;
//    
//    public ArrayBackedStringTS(TSByteId id, TimeSpec time_spec) {
//      super(id, time_spec);
//    }
//
//    @Override
//    public Iterator<TSValue<?>> iterator(TypeToken<?> type) {
//      return new It();
//    }
//    
//    @Override
//    public Collection<TypeToken<?>> types() {
//      return Lists.newArrayList(StringType.TYPE);
//    }
//
//    @Override
//    public void close() {
//      // TODO Auto-generated method stub
//      
//    }
//    
//    class It extends LocalIterator implements TSValue<StringType>, StringType {
//
//      @Override
//      public TSValue<StringType> next() {
//        ts.updateMsEpoch(Bytes.getLong(dps, idx));
//        idx += 8;
//        byte[] s = new byte[3];
//        System.arraycopy(dps, idx, s, 0, 3);
//        idx += 3;
//        value = new String(s, Const.UTF8_CHARSET);
//        return this;
//      }
//
//      @Override
//      public TimeStamp timestamp() {
//        return ts;
//      }
//
//      @Override
//      public List<String> values() {
//        return Lists.newArrayList(value);
//      }
//
//      @Override
//      public StringType value() {
//        return this;
//      }
//      
//    }
//  }

}
