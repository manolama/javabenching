package net.opentsdb;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class Benchmarks {

  public static void main(String[] args) {
    Options options = new OptionsBuilder()
        .include(GroupByAndSum.class.getSimpleName())
        .forks(1)
        .warmupIterations(5)
        .measurementIterations(10)
        .timeUnit(TimeUnit.MILLISECONDS)
        .build();
    try {
      new Runner(options).run();
    } catch (RunnerException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
