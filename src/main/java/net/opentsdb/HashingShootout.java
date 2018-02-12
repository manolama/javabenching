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
package net.opentsdb;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import net.openhft.hashing.LongHashFunction;
import net.opentsdb.common.Const;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
public class HashingShootout {

  public static void main(String[] args) {
    Context ctx = new Context();
    ctx.setup();
    runGuavaBuilder(ctx, null);
    runGuavaFlatten(ctx, null);
    runxxHashFlatten(ctx, null);
  }
  
  @State(Scope.Thread)
  public static class Context {
    byte[][] metrics;
    byte[][] aggTags;
    byte[][][] tags;
    
    public byte[] flatten() {
      try {
        ByteArrayOutputStream bais = new ByteArrayOutputStream();
        for (final byte[] b : metrics) {
          bais.write(b);
        }
        for (final byte[] b : aggTags) {
          bais.write(b);
        }
        for (final byte[][] pair : tags) {
          for (final byte[] b : pair) {
            bais.write(b);
          }
        }
        
        bais.close();
        return bais.toByteArray();
      } catch (IOException e) {
        throw new RuntimeException("WTF?", e);
      }
    }
    
    @Setup
    public void setup() {
      metrics = new byte[2][];
      metrics[0] = "sys.if.in".getBytes(Const.UTF8_CHARSET);
      metrics[1] = "sys.if.out".getBytes(Const.UTF8_CHARSET);
      
      aggTags = new byte[2][];
      aggTags[0] = "dc".getBytes(Const.UTF8_CHARSET);
      aggTags[1] = "dept".getBytes(Const.UTF8_CHARSET);
      
      
      tags = new byte[3][][];
      tags[0] = new byte[2][];
      tags[0][0] = "host".getBytes(Const.UTF8_CHARSET);
      tags[0][1] = "web01".getBytes(Const.UTF8_CHARSET);
      
      tags[1] = new byte[2][];
      tags[1][0] = "owner".getBytes(Const.UTF8_CHARSET);
      tags[1][1] = "bob".getBytes(Const.UTF8_CHARSET);
      
      tags[2] = new byte[2][];
      tags[2][0] = "type".getBytes(Const.UTF8_CHARSET);
      tags[2][1] = "websvr".getBytes(Const.UTF8_CHARSET);
    }
  }
  
  @Benchmark
  public static void runGuavaBuilder(Context context, Blackhole blackHole) {
    long hash = Const.HASH_FUNCTION().newHasher()
        .putObject(context.metrics, BYTE_ARRAY_FUNNEL)
        .putObject(context.tags, BYTE_MAP_FUNNEL)
        .putObject(context.aggTags, BYTE_ARRAY_FUNNEL)
        .hash()
        .asLong();
    if (blackHole == null) {
      System.out.println("Hash: " + hash);
    } else {
      blackHole.consume(hash);
    }
  }
  
  @Benchmark
  public static void runGuavaFlatten(Context context, Blackhole blackHole) {
    long hash = Const.HASH_FUNCTION().newHasher()
        .putBytes(context.flatten())
        .hash()
        .asLong();
    if (blackHole == null) {
      System.out.println("Hash: " + hash);
    } else {
      blackHole.consume(hash);
    }
  }
  
  @Benchmark
  public static void runxxHashFlatten(Context context, Blackhole blackHole) {
    long hash = LongHashFunction.xx_r39().hashBytes(context.flatten());
    
    if (blackHole == null) {
      System.out.println("Hash: " + hash);
    } else {
      blackHole.consume(hash);
    }
  }
  
  public static class ByteArrayFunnel implements Funnel<byte[][]> {
    private static final long serialVersionUID = 4102688353857386395L;
    private ByteArrayFunnel() { }
    @Override
    public void funnel(final byte[][] bytes, final PrimitiveSink sink) {
      if (bytes == null || bytes.length < 1) {
        return;
      }
      for (final byte[] entry : bytes) {
        sink.putBytes(entry);
      }
    }
  }
  public static ByteArrayFunnel BYTE_ARRAY_FUNNEL = new ByteArrayFunnel();
  
  public static class ByteMapFunnel implements Funnel<byte[][][]> {
    private static final long serialVersionUID = 4102688353857386395L;
    private ByteMapFunnel() { }
    @Override
    public void funnel(final byte[][][] bytes, final PrimitiveSink sink) {
      if (bytes == null || bytes.length < 1) {
        return;
      }
      for (final byte[][] pair : bytes) {
        for (final byte[] tag : pair) {
          sink.putBytes(tag);
        }
      }
    }
  }
  public static ByteMapFunnel BYTE_MAP_FUNNEL = new ByteMapFunnel();
}
