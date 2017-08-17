/**
 * Copyright 2017 Chris Larsen.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.opentsdb;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.LinuxPerfProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class Benchmarks {

  public static void main(String[] args) {
//    if (true) {
//      Context ctx = new Context();
//      ctx.setup();
//      GroupByAndSum.runTraditionalParallel(ctx, null);
//    }
    
    Options options = new OptionsBuilder()
        //.include(GroupByAndSum.class.getSimpleName())
        .include(SimDTest.class.getSimpleName())
        .forks(1)
        .warmupIterations(5)
        .measurementIterations(10)
        .timeUnit(TimeUnit.NANOSECONDS)
        .addProfiler(GCProfiler.class)
        //addProfiler(LinuxPerfProfiler.class)
        .build();
    try {
      new Runner(options).run();
    } catch (RunnerException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
