// automatically generated, do not modify

package net.opentsdb.flatbuffers;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class TimeSeriesId extends Table {
  public static TimeSeriesId getRootAsTimeSeriesId(ByteBuffer _bb) { return getRootAsTimeSeriesId(_bb, new TimeSeriesId()); }
  public static TimeSeriesId getRootAsTimeSeriesId(ByteBuffer _bb, TimeSeriesId obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public TimeSeriesId __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public String alias() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer aliasAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public ByteArray namespaces(int j) { return namespaces(new ByteArray(), j); }
  public ByteArray namespaces(ByteArray obj, int j) { int o = __offset(6); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int namespacesLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public ByteArray metrics(int j) { return metrics(new ByteArray(), j); }
  public ByteArray metrics(ByteArray obj, int j) { int o = __offset(8); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int metricsLength() { int o = __offset(8); return o != 0 ? __vector_len(o) : 0; }
  public ByteArray tagKeys(int j) { return tagKeys(new ByteArray(), j); }
  public ByteArray tagKeys(ByteArray obj, int j) { int o = __offset(10); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int tagKeysLength() { int o = __offset(10); return o != 0 ? __vector_len(o) : 0; }
  public ByteArray tagValues(int j) { return tagValues(new ByteArray(), j); }
  public ByteArray tagValues(ByteArray obj, int j) { int o = __offset(12); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int tagValuesLength() { int o = __offset(12); return o != 0 ? __vector_len(o) : 0; }
  public ByteArray aggregatedTags(int j) { return aggregatedTags(new ByteArray(), j); }
  public ByteArray aggregatedTags(ByteArray obj, int j) { int o = __offset(14); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int aggregatedTagsLength() { int o = __offset(14); return o != 0 ? __vector_len(o) : 0; }
  public ByteArray aggregatedTagValues(int j) { return aggregatedTagValues(new ByteArray(), j); }
  public ByteArray aggregatedTagValues(ByteArray obj, int j) { int o = __offset(16); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int aggregatedTagValuesLength() { int o = __offset(16); return o != 0 ? __vector_len(o) : 0; }
  public ByteArray disjointTags(int j) { return disjointTags(new ByteArray(), j); }
  public ByteArray disjointTags(ByteArray obj, int j) { int o = __offset(18); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int disjointTagsLength() { int o = __offset(18); return o != 0 ? __vector_len(o) : 0; }
  public ByteArray disjoingTagValues(int j) { return disjoingTagValues(new ByteArray(), j); }
  public ByteArray disjoingTagValues(ByteArray obj, int j) { int o = __offset(20); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int disjoingTagValuesLength() { int o = __offset(20); return o != 0 ? __vector_len(o) : 0; }

  public static int createTimeSeriesId(FlatBufferBuilder builder,
      int alias,
      int namespaces,
      int metrics,
      int tag_keys,
      int tag_values,
      int aggregated_tags,
      int aggregated_tag_values,
      int disjoint_tags,
      int disjoing_tag_values) {
    builder.startObject(9);
    TimeSeriesId.addDisjoingTagValues(builder, disjoing_tag_values);
    TimeSeriesId.addDisjointTags(builder, disjoint_tags);
    TimeSeriesId.addAggregatedTagValues(builder, aggregated_tag_values);
    TimeSeriesId.addAggregatedTags(builder, aggregated_tags);
    TimeSeriesId.addTagValues(builder, tag_values);
    TimeSeriesId.addTagKeys(builder, tag_keys);
    TimeSeriesId.addMetrics(builder, metrics);
    TimeSeriesId.addNamespaces(builder, namespaces);
    TimeSeriesId.addAlias(builder, alias);
    return TimeSeriesId.endTimeSeriesId(builder);
  }

  public static void startTimeSeriesId(FlatBufferBuilder builder) { builder.startObject(9); }
  public static void addAlias(FlatBufferBuilder builder, int aliasOffset) { builder.addOffset(0, aliasOffset, 0); }
  public static void addNamespaces(FlatBufferBuilder builder, int namespacesOffset) { builder.addOffset(1, namespacesOffset, 0); }
  public static int createNamespacesVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startNamespacesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addMetrics(FlatBufferBuilder builder, int metricsOffset) { builder.addOffset(2, metricsOffset, 0); }
  public static int createMetricsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startMetricsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addTagKeys(FlatBufferBuilder builder, int tagKeysOffset) { builder.addOffset(3, tagKeysOffset, 0); }
  public static int createTagKeysVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startTagKeysVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addTagValues(FlatBufferBuilder builder, int tagValuesOffset) { builder.addOffset(4, tagValuesOffset, 0); }
  public static int createTagValuesVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startTagValuesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addAggregatedTags(FlatBufferBuilder builder, int aggregatedTagsOffset) { builder.addOffset(5, aggregatedTagsOffset, 0); }
  public static int createAggregatedTagsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startAggregatedTagsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addAggregatedTagValues(FlatBufferBuilder builder, int aggregatedTagValuesOffset) { builder.addOffset(6, aggregatedTagValuesOffset, 0); }
  public static int createAggregatedTagValuesVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startAggregatedTagValuesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addDisjointTags(FlatBufferBuilder builder, int disjointTagsOffset) { builder.addOffset(7, disjointTagsOffset, 0); }
  public static int createDisjointTagsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startDisjointTagsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addDisjoingTagValues(FlatBufferBuilder builder, int disjoingTagValuesOffset) { builder.addOffset(8, disjoingTagValuesOffset, 0); }
  public static int createDisjoingTagValuesVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startDisjoingTagValuesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endTimeSeriesId(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishTimeSeriesIdBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
};

