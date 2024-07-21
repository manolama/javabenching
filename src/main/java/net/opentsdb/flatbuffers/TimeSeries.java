// automatically generated, do not modify

package net.opentsdb.flatbuffers;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class TimeSeries extends Table {
  public static TimeSeries getRootAsTimeSeries(ByteBuffer _bb) { return getRootAsTimeSeries(_bb, new TimeSeries()); }
  public static TimeSeries getRootAsTimeSeries(ByteBuffer _bb, TimeSeries obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public TimeSeries __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public TimeSeriesId id() { return id(new TimeSeriesId()); }
  public TimeSeriesId id(TimeSeriesId obj) { int o = __offset(4); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }
  public long basetime() { int o = __offset(6); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public int encoding() { int o = __offset(8); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int payload(int j) { int o = __offset(10); return o != 0 ? bb.get(__vector(o) + j * 1) & 0xFF : 0; }
  public int payloadLength() { int o = __offset(10); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer payloadAsByteBuffer() { return __vector_as_bytebuffer(10, 1); }
  public long timestamps(int j) { int o = __offset(12); return o != 0 ? bb.getLong(__vector(o) + j * 8) : 0; }
  public int timestampsLength() { int o = __offset(12); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer timestampsAsByteBuffer() { return __vector_as_bytebuffer(12, 8); }
  public double values(int j) { int o = __offset(14); return o != 0 ? bb.getDouble(__vector(o) + j * 8) : 0; }
  public int valuesLength() { int o = __offset(14); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer valuesAsByteBuffer() { return __vector_as_bytebuffer(14, 8); }

  public static int createTimeSeries(FlatBufferBuilder builder,
      int id,
      long basetime,
      int encoding,
      int payload,
      int timestamps,
      int values) {
    //builder.startObject(6);
    TimeSeries.addBasetime(builder, basetime);
    TimeSeries.addValues(builder, values);
    TimeSeries.addTimestamps(builder, timestamps);
    TimeSeries.addPayload(builder, payload);
    TimeSeries.addEncoding(builder, encoding);
    TimeSeries.addId(builder, id);
    return TimeSeries.endTimeSeries(builder);
  }

  public static void startTimeSeries(FlatBufferBuilder builder) {
    //builder.startObject(6);
  }
  public static void addId(FlatBufferBuilder builder, int idOffset) { builder.addOffset(0, idOffset, 0); }
  public static void addBasetime(FlatBufferBuilder builder, long basetime) { builder.addLong(1, basetime, 0); }
  public static void addEncoding(FlatBufferBuilder builder, int encoding) { builder.addInt(2, encoding, 0); }
  public static void addPayload(FlatBufferBuilder builder, int payloadOffset) { builder.addOffset(3, payloadOffset, 0); }
  public static int createPayloadVector(FlatBufferBuilder builder, byte[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]); return builder.endVector(); }
  public static void startPayloadVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static void addTimestamps(FlatBufferBuilder builder, int timestampsOffset) { builder.addOffset(4, timestampsOffset, 0); }
  public static int createTimestampsVector(FlatBufferBuilder builder, long[] data) { builder.startVector(8, data.length, 8); for (int i = data.length - 1; i >= 0; i--) builder.addLong(data[i]); return builder.endVector(); }
  public static void startTimestampsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(8, numElems, 8); }
  public static void addValues(FlatBufferBuilder builder, int valuesOffset) { builder.addOffset(5, valuesOffset, 0); }
  public static int createValuesVector(FlatBufferBuilder builder, double[] data) { builder.startVector(8, data.length, 8); for (int i = data.length - 1; i >= 0; i--) builder.addDouble(data[i]); return builder.endVector(); }
  public static void startValuesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(8, numElems, 8); }
  public static int endTimeSeries(FlatBufferBuilder builder) {
    int o = 0;//builder.endObject();
    return o;
  }
};

