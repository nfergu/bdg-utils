package org.apache.spark.rdd

import org.bdgenomics.utils.instrumentation.{ Metrics, Clock, Timer }
import org.apache.spark.util.Utils
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag
import org.apache.hadoop.mapreduce.{ OutputFormat => NewOutputFormat, OutputCommitter, JobContext, RecordWriter, TaskAttemptContext }
import org.apache.hadoop.conf.Configuration
import org.bdgenomics.utils.instrumentation.MetricsRecorder

abstract class InstrumentedRDDFunctions(clock: Clock) {

  // TODO NF: Do benchmarking of this class

  // TODO NF: Could we make this more efficient by leaving the value in the thread local for the next function call?

  def recordOperation[A](operation: => A): A = {
    rddOperationTimer().time {
      operation
    }
  }

  def recordFunction[B](function: => B, recorder: Option[MetricsRecorder], functionTimer: Timer): B = {
    if (recorder.isDefined) {
      Metrics.Recorder.withValue(recorder) {
        functionTimer.time(function)
      }
    }
    else {
      function
    }
  }

  def metricsRecorder(): Option[MetricsRecorder] = {
    val existingRegistryOption = Metrics.Recorder.value
    // Make a copy of the existing registry, as otherwise the stack will be unwound without having measured
    // the timings within the RDD operation
    if (existingRegistryOption.isDefined) Some(existingRegistryOption.get.copy()) else None
  }

  def instrumentedSaveAsNewAPIHadoopFile[K, V](rdd: RDD[(K, V)], recorder: Option[MetricsRecorder], path: String,
      keyClass: Class[_], valueClass: Class[_], outputFormatClass: Class[_ <: InstrumentedOutputFormat[_, _]],
      conf: Configuration)(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null) {
    // TODO NF: Should we remove the recorder from the thread local after we are done?
    // The call to the map operation here is to ensure that the registry is populated (in thread local storage)
    // for the output format to use. This works only because Spark combines the map operation and the subsequent
    // call to saveAsNewAPIHadoopFile into a single task, which is executed in a single thread. This is a bit of
    // a nasty hack, but is the only option for instrumenting the output format until SPARK-3051 is fixed.
    rdd.map(e => { if (recorder.isDefined) Metrics.Recorder.value = recorder; e })
      .saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass, conf)
  }

  private def rddOperationTimer(): Timer = {
    // We can only do this because we are in an org.apache.spark package (Utils is private to Spark). When we fix that
    // we'll have to implement our own getCallSite function
    val callSite = Utils.getCallSite.shortForm
    new Timer(callSite, clock = clock, recorder = None,
      sequenceId = Some(Metrics.generateNewSequenceId()), isRDDOperation = true)
  }

}

abstract class InstrumentedOutputFormat[K, V] extends NewOutputFormat[K, V] {

  val delegate = outputFormatClass().newInstance
  def getRecordWriter(context: TaskAttemptContext): RecordWriter[K, V] = {
    new InstrumentedRecordWriter(delegate.getRecordWriter(context), timerName())
  }
  def checkOutputSpecs(context: JobContext) = {
    delegate.checkOutputSpecs(context)
  }
  def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    delegate.getOutputCommitter(context)
  }
  def outputFormatClass(): Class[_ <: NewOutputFormat[K, V]]
  def timerName(): String

}

private class InstrumentedRecordWriter[K, V](delegate: RecordWriter[K, V], timerName: String) extends RecordWriter[K, V] {

  // This value must be computed lazily, as when then record write is instantiated the registry may not be in place yet.
  // This is because that calls it may not have been executed yet, since Spark executes everything lazily. However by
  // the time we come to actually write the record this function must have been called.
  lazy val writeRecordTimer = new Timer(timerName, recorder = Metrics.Recorder.value)
  def write(key: K, value: V) = writeRecordTimer.time {
    delegate.write(key, value)
  }
  def close(context: TaskAttemptContext) = {
    delegate.close(context)
  }

}
