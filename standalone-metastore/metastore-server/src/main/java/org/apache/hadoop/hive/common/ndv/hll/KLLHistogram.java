package org.apache.hadoop.hive.common.ndv.hll;

import org.apache.datasketches.kll.KllFloatsSketch;



public class KLLHistogram {

  private KllFloatsSketch sketch;
  private long lenStream;
  private int accuracy;

  public KLLHistogram(int accuracy) {
    this.accuracy = accuracy;
    this.sketch = new KllFloatsSketch(this.accuracy);
    this.lenStream = 0;
  }

  public void put(float value){
    sketch.update(value);
    lenStream = sketch.getN();
  }

  public double rangedSelectivity(float val1, float val2){
    float[] splitPoints = {val1, val2};
    double[] boundaries = sketch.getCDF(splitPoints);
    return (boundaries[1] - boundaries[0]);
  }

  public double singleSelectivity(float val){
    float offset = (float)(0.01 * lenStream);
    double rangedEstimate = rangedSelectivity(val, val + offset);
    return rangedEstimate / offset;
  }

  public double greaterThanSelectivity(float val){
    return rangedSelectivity(val, sketch.getMaxValue());
  }

  public double lessThanSelectivity(float val){
    return rangedSelectivity(sketch.getMinValue(), val);
  }

  public long lenStream(){
    return lenStream;
  }
}
