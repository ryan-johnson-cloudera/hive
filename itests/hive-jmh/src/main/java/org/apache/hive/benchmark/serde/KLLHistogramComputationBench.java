/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.benchmark.serde;

import java.util.concurrent.TimeUnit;

import java.util.Random;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.apache.hadoop.hive.common.ndv.hll.KLLBinnedHistogram;

/**
 * java -cp target/benchmarks.jar org.apache.hive.benchmark.serde.HyperLogLogBench
 */
@State(Scope.Benchmark)
public class KLLHistogramComputationBench {
  public static final int DEFAULT_ITER_TIME = 1000000;

  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @State(Scope.Thread)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static abstract class Abstract {

    @Setup
    public abstract void setup();

    @Benchmark
    @Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(iterations = 15, time = 2, timeUnit = TimeUnit.MILLISECONDS)
    public void bench() {

    }
  }

  public abstract static class SizeOptimizedSparseStressN extends Abstract {

    private KLLBinnedHistogram kllBinned;
    private final int stressN;
    private final TestData dataSet;
    private final int numBuckets;

    public SizeOptimizedSparseStressN(int stressN ,TestData dataSet, int numBuckets) {
      this.stressN = stressN;
      this.dataSet = dataSet;
      this.numBuckets = numBuckets;
    }

    @Override
    public void setup() {
      kllBinned = new KLLBinnedHistogram(200);
      if (dataSet == TestData.UNIFORM) {
        for (int i = 0; i < stressN; i++) {
          kllBinned.put(i);
        }
      } else if (dataSet == TestData.SKEWED) {
        int i = 0;
        while (kllBinned.lenStream() < stressN) {
          if (kllBinned.lenStream() < stressN / 100) {
            for (int j = 0; j < 20; j++) {
              kllBinned.put(i);
            }
          } else if (kllBinned.lenStream() < (stressN / 100) * 3) {
            for (int j = 0; j < 10; j++) {
              kllBinned.put(i);
            }
          } else if (kllBinned.lenStream() < (stressN / 100) * 5) {
            for (int j = 0; j < 5; j++) {
              kllBinned.put(i);
            }
          } else {
            kllBinned.put(i);
          }
          i += 1;
        }
      } else if (dataSet == TestData.RANDOM) {
        Random rand = new Random(stressN);
        for (int i = 0; i < stressN; i++) {
          kllBinned.put(i);
        }
      }

    }

    @Override
    public void bench() {
      for (int i = 0; i < 1000; i ++) {
        kllBinned.computeHistogram(numBuckets);
      }
    }
  }

  public enum TestData {
    UNIFORM,
    SKEWED,
    RANDOM
  }

  public static class HistogramComputationBig1 extends SizeOptimizedSparseStressN {
    public HistogramComputationBig1() {
      super(1000000, TestData.UNIFORM, -1);
    }
  }

  public static class HistogramComputationSmall1 extends SizeOptimizedSparseStressN {
    public HistogramComputationSmall1() {
      super(1000, TestData.UNIFORM, -1);
    }
  }

  public static class HistogramComputationBig2 extends SizeOptimizedSparseStressN {
    public HistogramComputationBig2() {
      super(1000000, TestData.SKEWED, -1);
    }
  }

  public static class HistogramComputationSmall2 extends SizeOptimizedSparseStressN {
    public HistogramComputationSmall2() {
      super(1000, TestData.SKEWED, -1);
    }
  }

  public static class HistogramComputationBig3 extends SizeOptimizedSparseStressN {
    public HistogramComputationBig3() {
      super(1000000, TestData.RANDOM, -1);
    }
  }

  public static class HistogramComputationSmall3 extends SizeOptimizedSparseStressN {
    public HistogramComputationSmall3() {
      super(1000, TestData.RANDOM, -1);
    }
  }

  public static class HistogramComputationBucketsBig extends SizeOptimizedSparseStressN {
    public HistogramComputationBucketsBig() {
      super(1000000, TestData.UNIFORM, 1000);
    }
  }

  public static class HistogramComputationBucketsMed extends SizeOptimizedSparseStressN {
    public HistogramComputationBucketsMed() {
      super(1000000, TestData.UNIFORM, 500);
    }
  }

  public static class HistogramComputationBucketsSmall extends SizeOptimizedSparseStressN {
    public HistogramComputationBucketsSmall() {
      super(1000000, TestData.UNIFORM, 100);
    }
  }


  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(".*" + KLLHistogramComputationBench.class.getSimpleName() + ".*").build();
    new Runner(opt).run();
  }
}
