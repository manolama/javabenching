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
package net.opentsdb.pipeline2;

import java.util.List;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.pipeline2.Interfaces.TS;

public class Abstracts {
  public static abstract class MyTS<T extends TimeSeriesDataType> implements TS<T> {
    protected byte[] dps;
    protected TimeSeriesId id;
    protected int idx;
    
    public MyTS(final TimeSeriesId id) {
      this.id = id;
    }
    
    @Override
    public TimeSeriesId id() {
      return id;
    }
    
    public void nextChunk(final byte[] data) {
      dps = data;
      idx = 0;
    }
  }
  
  public static abstract class StringType implements TimeSeriesDataType {
    /** The data type reference to pass around. */
    public static final TypeToken<StringType> TYPE = 
        TypeToken.of(StringType.class);
    
    /** Returns a list as processors (group by, downsample) may accumulate strings. */
    public abstract List<String> values();
  }
  
}
