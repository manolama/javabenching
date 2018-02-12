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
package net.opentsdb.timeseries;

import org.openjdk.jmh.infra.Blackhole;

public interface TimeSeriesBench {

  public void addDP(final long timestamp, final double value);
  
  public void setDPs(final byte[] dps);
  
  public void addMetric(final String metric);
  
  public void addNamespace(final String namespace);
  
  public void addTag(final String tagk, final String tagv);
  
  public void addAggTag(final String tagk);
  
  public void setBasetime(final long base);
  
  public void setEncoding(final int encoding);
  
  public byte[] getBytes();
  
  public void consume(final Blackhole blackHole);
}
