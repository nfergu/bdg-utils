bdg-utils
=========

General (non-omics) code used across BDG products. Apache 2 licensed.

# Instrumentation

## Basic Usage

To instrument a Spark RDD called `rdd`:

```scala
import org.bdgenomics.utils.instrumentation.Metrics
import org.apache.spark.rdd.MetricsContext._

Metrics.initialize(sparkContext)
val instrumentedRDD = rdd.instrument()
```

Any operations on `instrumentedRDD`, and other RDDs created from it, will then be instrumented. For example,
the following code:

```scala
val array = instrumentedRDD.map(_+1).keyBy(_%2).groupByKey().collect()
val writer = new PrintWriter(new OutputStreamWriter(System.out))
Metrics.print(writer, None)
writer.close()
```

Will produce something like this:

```
Timings
+-----------------------------------+-------------+-------------+-------+---------+------+--------+
|              Metric               | Worker Time | Driver Time | Count |  Mean   | Min  |  Max   |
+-----------------------------------+-------------+-------------+-------+---------+------+--------+
| └─ map at InsTest.scala:26        |           - |           - |     1 |       - |    - |      - |
|     └─ function call              |      399 µs |           - |    10 | 39.9 µs | 5 µs | 329 µs |
| └─ keyBy at InsTest.scala:26      |           - |           - |     1 |       - |    - |      - |
|     └─ function call              |      117 µs |           - |    10 | 11.7 µs | 5 µs |  58 µs |
| └─ groupByKey at InsTest.scala:26 |           - |           - |     1 |       - |    - |      - |
| └─ collect at InsTest.scala:26    |           - |           - |     1 |       - |    - |      - |
+-----------------------------------+-------------+-------------+-------+---------+------+--------+
```




Contains implicit conversions which enable instrumentation of Spark operations. This class should be used instead
 * of [[org.apache.spark.SparkContext]] when instrumentation is required.  Usage is as follows:
 *
 * {{{
 *
 * }}}
 *
 * Then, when any operations are performed on `instrumentedRDD` the RDD operation will be instrumented, along
 * with any functions that operate on its data. All subsequent RDD operations will be instrumented until
 * the `unInstrument` method is called on an RDD.
 *
 * @note When using this class, it is not a good idea to import `SparkContext._`, as the implicit conversions in there
 *       may conflict with those in here -- instead it is better to import only the specific parts of `SparkContext`
 *       that are needed.

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
