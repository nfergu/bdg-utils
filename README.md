bdg-utils
=========

General (non-omics) code used across BDG products. Apache 2 licensed.

## Instrumentation

### Basic Usage

First, initialize the `Metrics` object and create a Spark listener:

```scala
import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.instrumentation.{RecordedMetrics, MetricsListener}
Metrics.initialize(sparkContext)
val metricsListener = new MetricsListener(new RecordedMetrics())
sparkContext.addSparkListener(metricsListener)
```

Then, to instrument a Spark RDD called `rdd`:

```scala
import org.apache.spark.rdd.MetricsContext._
val instrumentedRDD = rdd.instrument()
```

Then, when any operations are performed on `instrumentedRDD` the RDD operation will be instrumented, along
with any functions that operate on its data. All subsequent RDD operations will be instrumented until
the `unInstrument` method is called on an RDD. For example, consider the following code:

```scala
val array = instrumentedRDD.map(_+1).keyBy(_%2).groupByKey().collect()
val writer = new PrintWriter(new OutputStreamWriter(System.out))
Metrics.print(writer, None)
writer.close()
```

This will result in output like the following:

```
Timings
+-------------------------------+-------------+-------------+-------+---------+------+--------+
|            Metric             | Worker Time | Driver Time | Count |  Mean   | Min  |  Max   |
+-------------------------------+-------------+-------------+-------+---------+------+--------+
| └─ map at Ins.scala:30        |           - |           - |     1 |       - |    - |      - |
|     └─ function call          |      642 µs |           - |    10 | 64.2 µs | 6 µs | 550 µs |
| └─ keyBy at Ins.scala:30      |           - |           - |     1 |       - |    - |      - |
|     └─ function call          |      140 µs |           - |    10 |   14 µs | 5 µs |  64 µs |
| └─ groupByKey at Ins.scala:30 |           - |           - |     1 |       - |    - |      - |
| └─ collect at Ins.scala:30    |           - |           - |     1 |       - |    - |      - |
+-------------------------------+-------------+-------------+-------+---------+------+--------+

Spark Operations
+----------------------------+--------------+----------+----------+
|         Operation          | Is Blocking? | Duration | Stage ID |
+----------------------------+--------------+----------+----------+
| map at Ins.scala:30        | false        |        - | -        |
| keyBy at Ins.scala:30      | true         |    91 ms | 1        |
| groupByKey at Ins.scala:30 | false        |        - | -        |
| collect at Ins.scala:30    | true         |    40 ms | 0        |
+----------------------------+--------------+----------+----------+
```

The first table contains each Spark operation, as well as timings for each of the functions that the Spark
operations use. The "Worker Time" column is the total time spent executing a particular function in the Spark workers,
and the "Count" column is the number of times that the function was called.

The second table contains more details about the Spark operations. The "Is Blocking?" column specifies whether a
particular operation was blocking (it must complete before subsequent operations on the same RDD can proceed). For
blocking operations the "Duration" column contains the duration of the Spark stage that corresponds to this operation.
That is, this is *the duration of this operation plus all of the preceding operations on the same RDD, up until the
previous blocking operation*.

IMPORTANT: it is not a good idea to import `SparkContext._`, as the implicit conversions in there
may conflict with those in here - instead it is better to import only the specific parts of `SparkContext`
that are needed.

### Instrumenting Function Calls

As well as instrumenting top-level functions used Spark operations, it is possible to instrument nested function calls.
For example, consider the following code:

```
object MyTimers extends Metrics {
  val DriverFunctionTopLevel = timer("Driver Function Top Level")
  val DriverFunctionNested = timer("Driver Function Nested")
  val WorkerFunctionTopLevel = timer("Worker Function Top Level")
  val WorkerFunctionNested = timer("Worker Function Nested")
}

import MyTimers._

DriverFunctionTopLevel.time {
  DriverFunctionNested.time {
    val array = instrumentedRDD.map(e => {
      WorkerFunctionTopLevel.time {
        WorkerFunctionNested.time {
          e+1
        }
      }
    }).collect()
  }
}

val writer = new PrintWriter(new OutputStreamWriter(System.out, "UTF-8"))
Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
writer.close()
```

This will result in output like the following:

```
Timings
+-----------------------------------------------+-------------+-------------+-------+----------+-------+---------+
|                    Metric                     | Worker Time | Driver Time | Count |   Mean   |  Min  |   Max   |
+-----------------------------------------------+-------------+-------------+-------+----------+-------+---------+
| └─ Driver Function Top Level                  |           - |     8.93 ms |     1 |  8.93 ms |     - |       - |
|     └─ Driver Function Nested                 |           - |     7.01 ms |     1 |  7.01 ms |     - |       - |
|         ├─ map at Ins.scala:50                |           - |           - |     1 |        - |     - |       - |
|         │   └─ function call                  |     1.75 ms |           - |    10 | 175.3 µs | 27 µs | 1.36 ms |
|         │       └─ Worker Function Top Level  |      916 µs |           - |    10 |  91.6 µs | 15 µs |  717 µs |
|         │           └─ Worker Function Nested |       77 µs |           - |    10 |   7.7 µs |  4 µs |   38 µs |
|         └─ collect at Ins.scala:56            |           - |           - |     1 |        - |     - |       - |
+-----------------------------------------------+-------------+-------------+-------+----------+-------+---------+
```

Note that there are two columns in the output to represent the total time spent on a particular function call:
"Driver Time" and "Worker Time". A particular function call will be in one or the other.
Driver Time is the total time spent on the function call in the Spark driver (in other words, outside of any RDD
operations). Worker Time is the total time spent in a Spark worker (in other words, within an RDD operation).

Note that the Driver Time does not include the time taken to execute Spark operations. This is because, in Spark,
operations are performed lazily: that is, for a particular RDD most operations take hardly any time at all, and a few
take a long time. Therefore it would be misleading to include the time taken to execute Spark operations in the driver
time.

### Additional Spark Statistics

It is possible to get additional metrics about Spark tasks. For example, consider the following code:

```
import org.bdgenomics.utils.instrumentation.{RecordedMetrics, MetricsListener}
val metricsListener = new MetricsListener(new RecordedMetrics())
sparkContext.addSparkListener(metricsListener)

val array = instrumentedRDD.map(_+1).keyBy(_%2).groupByKey().collect()
val writer = new PrintWriter(new OutputStreamWriter(System.out, "UTF-8"))
metricsListener.metrics.sparkMetrics.print(writer)
writer.close()
```

This will result in output similar to this:

```
Task Timings
+-------------------------------+------------+-------+--------+-------+-------+
|            Metric             | Total Time | Count |  Mean  |  Min  |  Max  |
+-------------------------------+------------+-------+--------+-------+-------+
| Task Duration                 |     128 ms |     2 |  64 ms | 46 ms | 82 ms |
| Executor Run Time             |      86 ms |     2 |  43 ms | 42 ms | 44 ms |
| Executor Deserialization Time |      15 ms |     2 | 7.5 ms |  1 ms | 14 ms |
| Result Serialization Time     |       2 ms |     2 |   1 ms |     0 |  2 ms |
+-------------------------------+------------+-------+--------+-------+-------+

Task Timings By Host
+-------------------------------+-----------+------------+-------+--------+-------+-------+
|            Metric             |   Host    | Total Time | Count |  Mean  |  Min  |  Max  |
+-------------------------------+-----------+------------+-------+--------+-------+-------+
| Task Duration                 | localhost |     128 ms |     2 |  64 ms | 46 ms | 82 ms |
| Executor Run Time             | localhost |      86 ms |     2 |  43 ms | 42 ms | 44 ms |
| Executor Deserialization Time | localhost |      15 ms |     2 | 7.5 ms |  1 ms | 14 ms |
| Result Serialization Time     | localhost |       2 ms |     2 |   1 ms |     0 |  2 ms |
+-------------------------------+-----------+------------+-------+--------+-------+-------+

Task Timings By Stage
+-------------------------------+----------------------------+------------+-------+-------+-------+-------+
|            Metric             |      Stage ID & Name       | Total Time | Count | Mean  |  Min  |  Max  |
+-------------------------------+----------------------------+------------+-------+-------+-------+-------+
| Task Duration                 | 1: keyBy at Ins.scala:30   |      82 ms |     1 | 82 ms | 82 ms | 82 ms |
| Task Duration                 | 0: collect at Ins.scala:30 |      46 ms |     1 | 46 ms | 46 ms | 46 ms |
| Executor Run Time             | 1: keyBy at Ins.scala:30   |      44 ms |     1 | 44 ms | 44 ms | 44 ms |
| Executor Run Time             | 0: collect at Ins.scala:30 |      42 ms |     1 | 42 ms | 42 ms | 42 ms |
| Executor Deserialization Time | 1: keyBy at Ins.scala:30   |      14 ms |     1 | 14 ms | 14 ms | 14 ms |
| Executor Deserialization Time | 0: collect at Ins.scala:30 |       1 ms |     1 |  1 ms |  1 ms |  1 ms |
| Result Serialization Time     | 0: collect at Ins.scala:30 |       2 ms |     1 |  2 ms |  2 ms |  2 ms |
| Result Serialization Time     | 1: keyBy at Ins.scala:30   |          0 |     1 |     0 |     0 |     0 |
+-------------------------------+----------------------------+------------+-------+-------+-------+-------+
```

The tables contain times for various parts of executing a Spark task, as well as the same timings broken down by
host and Spark Stage.

# Getting In Touch

## Mailing List

This project is maintained by the same developers as the [ADAM
project](https://www.github.com/bigdatagenomics/adam). As such, [the ADAM mailing
list](https://groups.google.com/forum/#!forum/adam-developers) is a good
way to sync up with other people who use the bdg-utils code, including the core developers.
You can subscribe by sending an email to `adam-developers+subscribe@googlegroups.com` or
just post using the [web forum page](https://groups.google.com/forum/#!forum/adam-developers).

## IRC Channel

A lot of the developers are hanging on the [#adamdev](http://webchat.freenode.net/?channels=adamdev)
freenode.net channel. Come join us and ask questions.

# License

bdg-utils is released under an [Apache 2.0 license](LICENSE.txt).
