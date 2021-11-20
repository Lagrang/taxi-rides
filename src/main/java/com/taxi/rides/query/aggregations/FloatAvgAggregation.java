package com.taxi.rides.query.aggregations;

public final class FloatAvgAggregation implements AggregationOperator<Float> {

  private Float sum;
  private int count;

  public FloatAvgAggregation() {
    this(0, 0);
  }

  private FloatAvgAggregation(float sum, int count) {
    this.sum = sum;
    this.count = count;
  }

  @Override
  public void add(Float value) {
    sum += value;
    count++;
  }

  @Override
  public FloatAvgAggregation merge(AggregationOperator<Float> otherAgg) {
    var otherAvg = (FloatAvgAggregation) otherAgg;
    return new FloatAvgAggregation(sum + otherAvg.sum, count + otherAvg.count);
  }

  @Override
  public Float computeResult() {
    return sum / count;
  }
}
