package net.opentsdb.pipeline5;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.reflect.TypeToken;

import avro.shaded.com.google.common.collect.Lists;
import net.opentsdb.common.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.pipeline5.Abstracts.*;
import net.opentsdb.pipeline5.Interfaces.*;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Bytes.ByteMap;

public class Implementations {

  public static class StringTSByteId implements TSByteId {
    
    /** An optional alias. */
    protected byte[] alias;
    
    /** An optional list of namespaces for the ID. */
    protected List<byte[]> namespaces;
    
    /** An optional list of metrics for the ID. */
    protected List<byte[]> metrics;
    
    /** A map of tag key/value pairs for the ID. */
    protected ByteMap<byte[]> tags;
    
    /** An optional list of aggregated tags for the ID. */
    protected List<byte[]> aggregated_tags;
    
    /** An optional list of disjoint tags for the ID. */
    protected List<byte[]> disjoint_tags;
    
    /** A list of unique IDs rolled up into the ID. */
    protected ByteSet unique_ids;
    
    public StringTSByteId() { }
    
    @Override
    public byte[] alias() {
      return null;
    }

    @Override
    public List<byte[]> namespaces() {
      return namespaces;
    }

    @Override
    public List<byte[]> metrics() {
      return metrics;
    }

    @Override
    public ByteMap<byte[]> tags() {
      return tags;
    }

    @Override
    public List<byte[]> aggregatedTags() {
      return aggregated_tags;
    }

    @Override
    public List<byte[]> disjointTags() {
      return disjoint_tags;
    }

    @Override
    public ByteSet uniqueIds() {
      return unique_ids;
    }

    @Override
    public int compareTo(TSByteId o) {
      return ComparisonChain.start()
          .compare(alias, o.alias(), Bytes.MEMCMP)
          .compare(namespaces, o.namespaces(), 
              Ordering.from(Bytes.MEMCMP).lexicographical().nullsFirst())
          .compare(metrics, o.metrics(), 
              Ordering.from(Bytes.MEMCMP).lexicographical().nullsFirst())
          .compare(tags, o.tags(), Bytes.BYTE_MAP_CMP)
          .compare(aggregated_tags, o.aggregatedTags(), 
              Ordering.from(Bytes.MEMCMP).lexicographical().nullsFirst())
          .compare(disjoint_tags, o.disjointTags(), 
              Ordering.from(Bytes.MEMCMP).lexicographical().nullsFirst())
          .compare(unique_ids, o.uniqueIds(), ByteSet.BYTE_SET_CMP)
          .result();
    }
    
    /** Just the one's were using for this test */
    public void addMetric(final String m) {
      if (metrics == null) {
        metrics = Lists.newArrayListWithCapacity(1);
      }
      metrics.add(m.getBytes(Const.UTF8_CHARSET));
    }
    
    public void addAggTag(final String t) {
      if (aggregated_tags == null) {
        aggregated_tags = Lists.newArrayListWithCapacity(1);
      }
      aggregated_tags.add(t.getBytes(Const.UTF8_CHARSET));
    }
    
    public void addTag(final String k, final String v) {
      if (tags == null) {
        tags = new ByteMap<byte[]>();
      }
      tags.put(k.getBytes(Const.UTF8_CHARSET), v.getBytes(Const.UTF8_CHARSET));
    }
    
    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (o == null || !(o instanceof TSByteId))
        return false;
      
      final TSByteId id = (TSByteId) o;
      
      // long slog through byte arrays.... :(
      if (Bytes.memcmpMaybeNull(alias(), id.alias()) != 0) {
        return false;
      }
      if (!Bytes.equals(namespaces(), id.namespaces())) {
        return false;
      }
      if (!Bytes.equals(metrics(), id.metrics())) {
        return false;
      }
      if (!Bytes.equals(tags(), id.tags())) {
        return false;
      }
      if (!Bytes.equals(aggregatedTags(), id.aggregatedTags())) {
        return false;
      }
      if (!Bytes.equals(disjointTags(), id.disjointTags())) {
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
        cached_hash = buildHashCode().asInt();
      }
      return cached_hash;
    }
    
    /** @return A HashCode object for deterministic, non-secure hashing */
    public HashCode buildHashCode() {
      return Const.HASH_FUNCTION().newHasher()
          .putBytes(alias == null ? new byte[]{ } : alias)
          .putObject(namespaces, Bytes.BYTE_LIST_FUNNEL)
          .putObject(metrics, Bytes.BYTE_LIST_FUNNEL)
          .putObject(tags, Bytes.BYTE_MAP_FUNNEL)
          .putObject(aggregated_tags, Bytes.BYTE_LIST_FUNNEL)
          .putObject(disjoint_tags, Bytes.BYTE_LIST_FUNNEL)
          .putObject(unique_ids, ByteSet.BYTE_SET_FUNNEL)
          .hash();
    }
  
    @Override
    public String toString() {
      final StringBuilder buf = new StringBuilder()
          .append("alias=")
          .append(alias != null ? new String(alias, Const.UTF8_CHARSET) : "null")
          .append(", namespaces=")
          .append(Bytes.toString(namespaces, Const.UTF8_CHARSET))
          .append(", metrics=")
          .append(Bytes.toString(metrics, Const.UTF8_CHARSET))
          .append(", tags=")
          .append(Bytes.toString(tags, Const.UTF8_CHARSET, Const.UTF8_CHARSET))
          .append(", aggregated_tags=")
          .append(Bytes.toString(aggregated_tags, Const.UTF8_CHARSET))
          .append(", disjoint_tags=")
          .append(Bytes.toString(disjoint_tags, Const.UTF8_CHARSET))
          .append(", uniqueIds=")
          .append(unique_ids);
      return buf.toString();
    }
  }
  
  public static class ArrayBackedLongTS extends BaseTS<NType> {
    
    public ArrayBackedLongTS(TSByteId id) {
      super(id);
    }

    @Override
    public Iterator<TSValue<NType>> iterator() {
      return new It();
    }

    @Override
    public TypeToken<NType> type() {
      return NType.TYPE;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
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
      
    }
  }

  public static class ArrayBackedStringTS extends BaseTS<StringType> {
    TimeStamp ts = new MillisecondTimeStamp(0);
    String value = null;
    
    public ArrayBackedStringTS(TSByteId id) {
      super(id);
    }

    @Override
    public Iterator<TSValue<StringType>> iterator() {
      return new It();
    }

    @Override
    public TypeToken<StringType> type() {
      return StringType.TYPE;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
    }
    
    class It extends LocalIterator implements TSValue<StringType>, StringType {

      @Override
      public TSValue<StringType> next() {
        ts.updateMsEpoch(Bytes.getLong(dps, idx));
        idx += 8;
        byte[] s = new byte[3];
        System.arraycopy(dps, idx, s, 0, 3);
        idx += 3;
        value = new String(s, Const.UTF8_CHARSET);
        return this;
      }

      @Override
      public TimeStamp timestamp() {
        return ts;
      }

      @Override
      public List<String> values() {
        return Lists.newArrayList(value);
      }

      @Override
      public StringType value() {
        return this;
      }
      
    }
  }

}
