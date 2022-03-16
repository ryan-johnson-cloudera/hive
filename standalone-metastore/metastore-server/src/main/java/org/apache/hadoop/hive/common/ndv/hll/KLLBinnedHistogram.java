/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.hadoop.hive.common.ndv.hll;

import org.apache.datasketches.kll.KllFloatsSketch;



public class KLLBinnedHistogram {

  private KllFloatsSketch sketch;
  private long lenStream;
  private int accuracy;
  private double[] buckets;
  private long tupsPerBucket;

  public KLLBinnedHistogram(int accuracy) {
    this.accuracy = accuracy;
    this.sketch = new KllFloatsSketch(this.accuracy);
    this.lenStream = 0;
  }

  public void put(float value) {
    sketch.update(value);
    lenStream = sketch.getN();
  }

  public void computeHistogram(int numBins) {

    if (numBins == -1) {
      numBins = (int)Math.ceil((sketch.getMaxValue() - sketch.getMinValue()) /
          (2 * (sketch.getQuantile(0.75) - sketch.getQuantile(0.25)) / Math. cbrt(lenStream)));
    }


    tupsPerBucket = lenStream / numBins;
    buckets = new double[numBins + 1];
    double q = 1.0 / numBins;
    for (int i = 0; i < numBins; i++) {
      buckets[i] = sketch.getQuantile(q * i);
    }
    buckets[numBins] = sketch.getMaxValue();
  }

  public double rangedSelectivity(float val1, float val2) {
    double totalTups = 0;
    int bin = 1;
    boolean betweenBoundaries = false;
    double width;
    double percentageBucket;
    if (val1 > sketch.getMaxValue() || val2 < sketch.getMinValue()) {
      return 0;
    }
    while (true) {
      if (!betweenBoundaries && val1 < buckets[bin] && (val2 < buckets[bin] || bin == buckets.length - 1)) {
        width = buckets[bin] - buckets[bin - 1];
        percentageBucket = (val2 - val1) / width;
        totalTups += Math.min(percentageBucket, 1) * tupsPerBucket;
        break;
      } else if (!betweenBoundaries && val1 < buckets[bin]) {
        width = buckets[bin] - buckets[bin - 1];
        percentageBucket = (buckets[bin] - val1) / width;
        totalTups += Math.min(percentageBucket, 1 ) * tupsPerBucket;
        betweenBoundaries = true;
      } else if (betweenBoundaries && (val2 < buckets[bin] || bin == buckets.length - 1)) {
        width = buckets[bin] - buckets[bin - 1];
        percentageBucket = (val2 - buckets[bin - 1]) / width;
        totalTups += Math.min(percentageBucket, 1) * tupsPerBucket;
        break;
      } else if (betweenBoundaries) {
        totalTups += tupsPerBucket;
      }
      bin += 1;
    }
    return totalTups / lenStream;
  }


  public double greaterThanSelectivity(float val) {
    double totalTups = 0;
    int bin = 1;
    boolean betweenBoundaries = false;
    double width;
    double percentageBucket;
    if (val > sketch.getMaxValue()) {
      return 0;
    }
    while (bin < buckets.length) {
      if (!betweenBoundaries && val < buckets[bin]) {
        width = buckets[bin] - buckets[bin - 1];
        percentageBucket = (buckets[bin] - val) / width;
        totalTups += Math.min(percentageBucket, 1) * tupsPerBucket;
        betweenBoundaries = true;
      } else if (betweenBoundaries) {
        totalTups += tupsPerBucket;
      }
      bin += 1;
    }
    return totalTups / lenStream;
  }

  public double lessThanSelectivity(float val) {
    double totalTups = 0;
    int bin = 1;
    double width;
    double percentageBucket;
    if (val < sketch.getMinValue()) {
      return 0;
    }
    while (true) {
      if (val < buckets[bin] || bin == buckets.length - 1) {
        width = buckets[bin] - buckets[bin - 1];
        percentageBucket = (val - buckets[bin - 1]) / width;
        totalTups += Math.min(percentageBucket, 1) * tupsPerBucket;
        break;
      } else {
        totalTups += tupsPerBucket;
      }
      bin += 1;
    }
    return totalTups / lenStream;
  }

  public long lenStream() {
    return lenStream;
  }
}
