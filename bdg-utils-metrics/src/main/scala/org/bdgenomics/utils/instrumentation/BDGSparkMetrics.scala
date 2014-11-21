package org.bdgenomics.utils.instrumentation

/**
 * Metrics collected from Spark.
 */
class BDGSparkMetrics extends SparkMetrics {
  val executorRunTime = taskTimer("Executor Run Time")
  val executorDeserializeTime = taskTimer("Executor Deserialization Time")
  val resultSerializationTime = taskTimer("Result Serialization Time")
  val duration = taskTimer("Task Duration")
}
