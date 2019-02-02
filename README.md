## Java Benching
This is a bunch of testing and scratch code to play with various Java'y bits to determine what would be in OpenTSDB's best interest for version 3.0. Eventually I'll move it into the OpenTSDB repo, most likely.

It's built around JMH (for benchmarking) and JOL (to fetch class layout) so you can run it using the common JMH flags.

E.g. ``mvn clean package && java -jar target/benchmarks.jar .*pipeline.* -f 1 -wi 5 -i 25 -tu ns -prof gc`` to run the TSDB pipeline tests.

Run ``java -jar target/benchmarks.jar -help`` to print out the various options available.

A script is available ``tableit.py`` to convert the JMH output to a CSV that aligns the test results into columns instead of rows so it's easier to sort. To use it either dump the JMH run to a log file and then pipe that into the script or pipe the JMH run directly into the script. E.g.

``mvn clean package && java -jar target/benchmarks.jar .*pipeline.* -f 1 -wi 5 -i 25 -tu ns -prof gc > gc.log``
``cat gc.log | python tableit.py --csv my.csv``

## Pipelines

OpenTSDB 3.x's new query pipeline must be as performant yet flexible as possible so I've banged out a number of various implementations all performing the same tasks:

* Read data for multiple time series for a chunk of time, e.g. hour 1, then hour 2, etc. (Similar to the way we store data in HBase). The chunks must be streamed through functions to a consumer.
* Apply functions to the data in the following order:
  * Filter out numeric data points when an associated string is != ``foo``.
  * Group by a host tag
  * Compute the difference of each data point for a time series from the standard deviation of that series. This entails a multi-pass iteration over all time chunks for a series to find the standard deviation, then a second pass applying that deviation.
  * Compute the sum of two metrics

The important bits here are that the functions must be compossible (arranged in any order), support caching and multi-pass operations, and offer an API that's easy for developers to work with.

For each pipeline, the following list describes the type of value returned in a time series object.

1. Iterative over data complex data points.
2. List of complex data points.
3. Array of values within complex data.
4. Iterative over data complex data points.
5. Iterative over data complex data points.
6. Iterative over data complex data points.
7. Iterative over data complex data points.
8. Iterative over data complex data points.
9. Iterative over data complex data points.
10. Iterative over data complex data points.

## SerDes

I want to provide a compressible and memory efficient serialization framework for passing messages between TSDs and other components. Therefore I look at multi-language compatible serialization frameworks including Thrift, ProtoBuf, Capnp, Flatbuffers, Avro and JSON. 

After benching, it looks like the ProtoBufs out perform the others a tiny bit with our use case. FlatBuffers is a real pain to code with so I wasn't able to complete that implementation though it may work nicely.

## Async

OpenTSDB uses the custom Deferred library instead of Futures because at the time, Java didn't have a compossible callback chaining mechanism. Now we have native CompletableFutures as well as Guava's ListenableFutures so we need to test em all.

Interestingly, the new CompletableFutures are the quickest but generate the most garbage. ListenableFutures are the slowest but trash the heap less than the native tools. And good ole Deferreds are a little slower than the natives but are the most memory efficient. 

## SIMD

This is an attempt at trying various coding bits to force the JVM into optimizing with SIMD calls. It definitely helps a bit but there may be better trade-offs for performance.

## Streams Vs Iterators

In TSDB, data points are passed through iterators when querying. Java 8 has a nifty new Stream feature that lets you compose flows and even run operations in parallel! Sounds perfect for iterating over batches of time series in parallel. But in practice, stream performance is *much* slower than good olde iteration. So while streams *look* like they'll save you a ton of code (and they can), you make a major performance tradeoff unless you're doing something so inane that an iterative mechanism would add a few more lines of code.

## Distributed Queue's

For 3.x we'd like a query protection mechanism that tracks queries by users and throttles them so that one user doesn't overwhelm the system. One way to do this is fair-use queues that requires a query coordination system with shared state. I'm looking at various shared queued options here to see what works the best. Ideally I'd like a distributed priority queue but haven't found one yet.

## Stack Overflow Avoidance

OpenTSDB is async. Lots of apps are going async these days since it's a great way to efficiently use resources without having to spin-wait CPU's or block threads, etc. With this comes the possibility of StackOverflowError's in Java as asynchronous recursions within the same thread can easily blow the stack. So we need a way to combat it and there are some methods to try out. The high-level methods are:

* Run a task in a thread pool. Resets the stack each time it's pulled from the work queue.
* Trampoline by converting a recursion into an iteration.

Since TSD is dealing with asynchronous resource calls and not just a recursive algorithm (though we have those too), the thread mechanism makes the most sense.

Using native Java threadpools surprisingly doesn't add much overhead in this bench (since there's only one thread with one work queue that grabs, does some work, submits back to the queue). But I was interested in looking at so-called lightweight threads to see if they're more performant. Quasar was the only library I found that implemented such threads and running the test showed that it was a little less performant than the native pool but saved a lot on GC overhead. Also I really don't like the idea of having to bootstrap the Quasar agent in the JVM.

If you want to run the Quasar bench, you have to start up with the Quasar agent ala:

``mvn clean package && java -javaagent:~/.m2/repository/co/paralleluniverse/quasar-core/0.7.9/quasar-core-0.7.9.jar -jar target/benchmarks.jar .*Async* -f 1 -wi 5 -i 15 -tu ns``