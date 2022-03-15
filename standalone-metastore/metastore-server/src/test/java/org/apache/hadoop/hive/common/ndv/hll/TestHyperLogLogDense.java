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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
@Category(MetastoreUnitTest.class)
public class TestHyperLogLogDense {

  // 5% tolerance for long range bias and 3% for short range bias
  private float longRangeTolerance = 5.0f;
  private float shortRangeTolerance = 3.0f;

  private int size;

  public TestHyperLogLogDense(int n) {
    this.size = n;
  }

  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] { { 2 }, { 10 }, { 100 }, { 1000 }, { 10000 }, { 100000 },
        { 1000000 } };
    return Arrays.asList(data);
  }

  @Test
  public void testHLLAdd() {
    Random rand = new Random(size);
    HyperLogLog hll = HyperLogLog.builder().setEncoding(HyperLogLog.EncodingType.DENSE).build();
    int size = 100;
    for (int i = 0; i < size; i++) {
      hll.addLong(rand.nextLong());
    }
    double threshold = size > 40000 ? longRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    assertEquals((double) size, (double) hll.count(), delta);
  }

  @Test
  public void testHLLAddHalfDistinct() {
    Random rand = new Random(size);
    HyperLogLog hll = HyperLogLog.builder().setEncoding(HyperLogLog.EncodingType.DENSE).build();
    int unique = size / 2;
    Set<Long> hashset = new HashSet<>();
    for (int i = 0; i < size; i++) {
      long val = rand.nextInt(unique);
      hashset.add(val);
      hll.addLong(val);
    }
    double threshold = size > 40000 ? longRangeTolerance : shortRangeTolerance;
    double delta = threshold * hashset.size() / 100;
    assertEquals((double) hashset.size(), (double) hll.count(), delta);
  }

  @Test
  public void testKLLSimple() {
    // Setup of KLL Sketch and stream the data
    KllFloatsSketch sketch = new KllFloatsSketch();
    int size = 1000000;
    for (int i = 0; i < size; i++) {
      sketch.update(i);
    }

    long lenStream = sketch.getN();

    // Create the histogram bins for an equi-height histogram
    int numBins = 10;
    long tupsPerBucket = lenStream / numBins;
    double[] buckets = new double[numBins + 1];
    double q = 1.0 / numBins;
    for (int i = 0; i < numBins; i++) {
      buckets[i] = sketch.getQuantile(q * i);
    }
    buckets[numBins] = sketch.getMaxValue();

    // Confirm that the size streamed is correct
    double threshold = size > 40000 ? longRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    assertEquals((double) size, (double) lenStream, delta);
    System.out.println(Arrays.toString(buckets));
  }

  @Test
  public void testKLLSimpleSkewed() {
    // Setup of KLL Sketch and stream the data
    // Size variable is the number of elements streamed to the sketch, the first for loop streams 1000000 elements
    // The second for loop streams 250000 elements
    KllFloatsSketch sketch = new KllFloatsSketch();
    int size = 3000000;
    for (int i = 0; i < 1000000; i++) {
      sketch.update(i);
      sketch.update(i);
    }
    for (int i = 1000000; i < 2000000; i++) {
      sketch.update(i);
    }

    long lenStream = sketch.getN();

    // Create the histogram bins for an equi-height histogram
    int numBins = 10;
    long tupsPerBucket = lenStream / numBins;
    double[] buckets = new double[numBins];
    double q = 1.0 / numBins;
    for (int i = 0; i < numBins; i++) {
      buckets[i] = sketch.getQuantile(q * i);
    }

    // Confirm that the size streamed is correct
    double threshold = size > 40000 ? longRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    assertEquals((double) size, (double) lenStream, delta);
    System.out.println(Arrays.toString(buckets));
  }

  @Test
  public void testKLLSimpleRandom() {
    // Setup of KLL Sketch and stream the data
    KllFloatsSketch sketch = new KllFloatsSketch();
    Random rand = new Random(size);
    int size = 1000000;
    for (int i = 0; i < size; i++) {
      sketch.update(rand.nextInt(500));
    }

    long lenStream = sketch.getN();

    // Create the histogram bins for an equi-height histogram
    int numBins = 10;
    long tupsPerBucket = lenStream / numBins;
    double[] buckets = new double[numBins];
    double q = 1.0 / numBins;
    for (int i = 0; i < numBins; i++) {
      buckets[i] = sketch.getQuantile(q * i);
    }

    // Confirm that the size streamed is correct
    double threshold = size > 40000 ? longRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    assertEquals((double) size, (double) lenStream, delta);
    System.out.println(Arrays.toString(buckets));
  }

  @Test
  public void testKLLCDF() {
    KllFloatsSketch sketch = new KllFloatsSketch();
//    sketch.update(1);
//    sketch.update(1);
//    sketch.update(2);
//    sketch.update(3);
//    sketch.update(4);
//    sketch.update(5);
//    sketch.update(6);
//    sketch.update(7);
//    sketch.update(8);
//    sketch.update(9);
//    sketch.update(10);
//    sketch.update(6);
//    sketch.update(6);
//    sketch.update(7);
//    sketch.update(8);
//    sketch.update(9);
//    sketch.update(10);
//    sketch.update(11);
//    sketch.update(12);
//    sketch.update(13);
//    sketch.update(14);
//    sketch.update(15);
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    sketch.update(5);
    sketch.update(6);
    sketch.update(7);
    sketch.update(8);
    sketch.update(9);
    sketch.update(10);


    float[] temp = new float[2];
    temp[0] = 8;
    temp[1] = 9;
    double[] x = sketch.getCDF(temp);
    double val = (x[1] - x[0]) / 2;
    System.out.println(val);
    System.out.println(Arrays.toString(sketch.getCDF(temp)));
  }

  @Test
  public void testKLLHistogram() {
    KLLHistogram sketch = new KLLHistogram(200);
    for (int i = 0; i < 1000; i++) {
      sketch.put(i);
    }
    int size = 1000;
    System.out.println("OUTPUT");
    System.out.println(sketch.pointSelectivity(990) * size);
    System.out.println(sketch.rangedSelectivity(200, 550) * size);
    System.out.println(sketch.greaterThanSelectivity(400) * size);
    System.out.println(sketch.lessThanSelectivity(200) * size);
  }

  @Test
  public void testKLLHistogramSkewed() {
    KLLHistogram sketch = new KLLHistogram(200);
    int size = 3000;
    for (int i = 0; i < 1000; i++) {
      sketch.put(i);
      sketch.put(i);
    }
    for (int i = 1000; i < 2000; i++) {
      sketch.put(i);
    }
    System.out.println("OUTPUT");
    System.out.println(sketch.pointSelectivity(10) * size);
    System.out.println(sketch.rangedSelectivity(200, 550) * size);
    System.out.println(sketch.greaterThanSelectivity(400) * size);
    System.out.println(sketch.lessThanSelectivity(400) * size);
  }

  @Test
  public void binnedHistogramSimple() {
    KLLBinnedHistogram sketch = new KLLBinnedHistogram(200);
    for (int i = 0; i < 1000; i++) {
      sketch.put(i);
    }
    sketch.computeHistogram(10);
    System.out.println("OUTPUT");
    System.out.println(sketch.rangedSelectivity(400, 900) * 1000);

  }
}
