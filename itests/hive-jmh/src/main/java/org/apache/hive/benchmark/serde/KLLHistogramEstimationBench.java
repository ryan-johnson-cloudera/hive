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
import org.apache.hadoop.hive.common.ndv.hll.KLLHistogram;
import org.apache.hadoop.hive.common.ndv.hll.KLLBinnedHistogram;

/**
 * java -cp target/benchmarks.jar org.apache.hive.benchmark.serde.HyperLogLogBench
 */
@State(Scope.Benchmark)
public class KLLHistogramEstimationBench {
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

    private KLLHistogram kll;
    private KLLBinnedHistogram kllBinned;
    private final int stressN;
    private final TestData dataSet;
    private final boolean binned;

    public SizeOptimizedSparseStressN(int stressN,TestData dataSet, boolean binned) {
      this.stressN = stressN;
      this.dataSet = dataSet;
      this.binned = binned;
    }

    @Override
    public void setup() {
      kll = new KLLHistogram(200);
      kllBinned = new KLLBinnedHistogram(200);
      if (dataSet == TestData.UNIFORM) {
        for (int i = 0; i < stressN; i++) {
          kll.put(i);
          kllBinned.put(i);
        }
      } else if (dataSet == TestData.SKEWED) {
        int i = 0;
        while (kllBinned.lenStream() < stressN) {
          if (kllBinned.lenStream() < stressN / 100) {
            for (int j = 0; j < 20; j++) {
              kllBinned.put(i);
              kll.put(i);
            }
          } else if (kllBinned.lenStream() < (stressN / 100) * 3) {
            for (int j = 0; j < 10; j++) {
              kllBinned.put(i);
              kll.put(i);
            }
          } else if (kllBinned.lenStream() < (stressN / 100) * 5) {
            for (int j = 0; j < 5; j++) {
              kllBinned.put(i);
              kll.put(i);
            }
          } else {
            kllBinned.put(i);
            kll.put(i);
          }
          i += 1;
        }
      } else if (dataSet == TestData.RANDOM) {
        Random rand = new Random(stressN);
        for (int i = 0; i < stressN; i++) {
          kll.put(rand.nextInt(stressN));
          kllBinned.put(i);
        }
      }
      kllBinned.computeHistogram(1000);
    }

    @Override
    public void bench() {
      if (binned) {
//        for (int i = 1; i < stressN; i += 5) {
//          kllBinned.rangedSelectivity(0, i);
//        }
//        for (int i = stressN / 2 + 1; i < stressN; i += 5) {
//          kllBinned.rangedSelectivity(stressN / 2, i);
//        }
//        for (int i = 0; i < stressN; i += stressN * 0.01) {
//          kllBinned.rangedSelectivity(i, (float) (i + stressN * 0.01));
//        }
        for (int i = 1; i < 1000000; i += 1) {
          kllBinned.rangedSelectivity(0, stressN);
        }
      } else {
        for (int i = 1; i < 1000000; i += 1) {
          kllBinned.rangedSelectivity(0, stressN);
        }
//        for (int i = 1; i < stressN; i += 5) {
//          kll.rangedSelectivity(0, i);
//        }
//        for (int i = stressN / 2 + 1; i < stressN; i += 5) {
//          kll.rangedSelectivity(stressN / 2, i);
//        }
//        for (int i = 0; i < stressN; i += stressN * 0.01) {
//          kll.rangedSelectivity(i, (float) (i + stressN * 0.01));
//        }
      }
    }
  }

  public enum TestData {
    UNIFORM,
    SKEWED,
    RANDOM
  }

  public static class KLLHistogramBigDS1 extends SizeOptimizedSparseStressN {
    public KLLHistogramBigDS1() {
      super(1000000, TestData.UNIFORM, false);
    }
  }

  public static class KLLHistogramSmallDS1 extends SizeOptimizedSparseStressN {
    public KLLHistogramSmallDS1() {
      super(50000, TestData.UNIFORM, false);
    }
  }

  public static class KLLHistogramBigDS2 extends SizeOptimizedSparseStressN {
    public KLLHistogramBigDS2() {
      super(1000000, TestData.SKEWED, false);
    }
  }

  public static class KLLHistogramSmallDS2 extends SizeOptimizedSparseStressN {
    public KLLHistogramSmallDS2() {
      super(50000, TestData.SKEWED, false);
    }
  }

  public static class KLLHistogramBigDS3 extends SizeOptimizedSparseStressN {
    public KLLHistogramBigDS3() {
      super(1000000, TestData.RANDOM, false);
    }
  }

  public static class KLLHistogramSmallDS3 extends SizeOptimizedSparseStressN {
    public KLLHistogramSmallDS3() {
      super(50000, TestData.RANDOM, false);
    }
  }


  public static class BinnedKLLHistogramBigDS1 extends SizeOptimizedSparseStressN {
    public BinnedKLLHistogramBigDS1() {
      super(1000000, TestData.UNIFORM, true);
    }
  }

  public static class BinnedKLLHistogramSmallDS1 extends SizeOptimizedSparseStressN {
    public BinnedKLLHistogramSmallDS1() {
      super(50000, TestData.UNIFORM, true);
    }
  }

  public static class BinnedKLLHistogramBigDS2 extends SizeOptimizedSparseStressN {
    public BinnedKLLHistogramBigDS2() {
      super(1000000, TestData.SKEWED, true);
    }
  }

  public static class BinnedKLLHistogramSmallDS2 extends SizeOptimizedSparseStressN {
    public BinnedKLLHistogramSmallDS2() {
      super(50000,  TestData.SKEWED, true);
    }
  }

  public static class BinnedKLLHistogramBigDS3 extends SizeOptimizedSparseStressN {
    public BinnedKLLHistogramBigDS3() {
      super(1000000, TestData.RANDOM, true);
    }
  }

  public static class BinnedKLLHistogramSmallDS3 extends SizeOptimizedSparseStressN {
    public BinnedKLLHistogramSmallDS3() {
      super(50000, TestData.RANDOM, true);
    }
  }


  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(".*" + KLLHistogramEstimationBench.class.getSimpleName() + ".*").build();
    new Runner(opt).run();
  }
}
