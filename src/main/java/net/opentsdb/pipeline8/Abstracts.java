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
package net.opentsdb.pipeline8;

import java.util.Iterator;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.pipeline8.Interfaces.*;

public class Abstracts {
  public static abstract class BaseTS<T extends TimeSeriesDataType> implements TS<T> {
    protected byte[] dps;
    protected TimeSpec time_spec;
    protected TSByteId id;
    
    public BaseTS(final TSByteId id, TimeSpec time_spec) {
      this.id = id;
      this.time_spec = time_spec;
    }
    
    @Override
    public TSByteId id() {
      return id;
    }
    
    public void nextChunk(final byte[] data) {
      dps = data;
    }
    
    public abstract class LocalIterator implements Iterator<TSValue<T>> {
      protected int idx;
      protected int time_idx;
      
      @Override
      public boolean hasNext() {
        return idx < dps.length;
      }
    }
  }
}
