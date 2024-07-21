// automatically generated, do not modify

package net.opentsdb.flatbuffers;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class ByteArray extends Table {
  public static ByteArray getRootAsByteArray(ByteBuffer _bb) { return getRootAsByteArray(_bb, new ByteArray()); }
  public static ByteArray getRootAsByteArray(ByteBuffer _bb, ByteArray obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public ByteArray __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public int bytearray(int j) { int o = __offset(4); return o != 0 ? bb.get(__vector(o) + j * 1) & 0xFF : 0; }
  public int bytearrayLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer bytearrayAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }

  public static int createByteArray(FlatBufferBuilder builder,
      int bytearray) {
    //builder.startObject(1);
    ByteArray.addBytearray(builder, bytearray);
    return ByteArray.endByteArray(builder);
  }

  public static void startByteArray(FlatBufferBuilder builder) {
    //builder.startObject(1);
  }
  public static void addBytearray(FlatBufferBuilder builder, int bytearrayOffset) { builder.addOffset(0, bytearrayOffset, 0); }
  public static int createBytearrayVector(FlatBufferBuilder builder, byte[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]); return builder.endVector(); }
  public static void startBytearrayVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static int endByteArray(FlatBufferBuilder builder) {
    int o = 0;//builder.endObject();
    return o;
  }
};

