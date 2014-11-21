/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.utils.instrumentation

import org.bdgenomics.utils.misc.SparkFunSuite

import org.apache.spark.SparkContext.IntAccumulatorParam
import org.scalatest.concurrent.{ IntegrationPatience, Eventually }

class MetricsListenerSuite extends SparkFunSuite with Eventually with IntegrationPatience {

  sparkTest("Listener accumulates metrics when registered with Spark") {

    val sparkMetrics = new BDGSparkMetrics()
    val listener = new MetricsListener(sparkMetrics)
    sc.addSparkListener(listener)

    // Doesn't really matter what we do here -- we just need to do something that spawns some tasks
    val accumulator = sc.accumulator(0)
    sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8), numSlices = 8).foreach(x => {
      accumulator += x
    })

    eventually {
      assert(accumulator.value === 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8)
    }

    eventually {
      // There's nothing sensible we can assert based on the timings, so just assert based on the counts
      assert(sparkMetrics.duration.getOverallTimings.getCount === 8)
      assert(sparkMetrics.executorRunTime.getOverallTimings.getCount === 8)
      assert(sparkMetrics.executorDeserializeTime.getOverallTimings.getCount === 8)
      assert(sparkMetrics.resultSerializationTime.getOverallTimings.getCount === 8)
      assert(sparkMetrics.stageTimes.iterator.hasNext)
    }

  }

}
