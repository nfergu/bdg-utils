package org.bdgenomics.utils.instrumentation

class TestingClock extends Clock {
  var currentTime = 0L
  override def nanoTime(): Long = currentTime
}
