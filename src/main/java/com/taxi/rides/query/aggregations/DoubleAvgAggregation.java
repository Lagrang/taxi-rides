package com.taxi.rides.query.aggregations;

public final class DoubleAvgAggregation implements AggregationOperator<Double> {

  private Double sum;
  private int count;

  public DoubleAvgAggregation() {
    this(0, 0);
  }

  private DoubleAvgAggregation(double sum, int count) {
    this.sum = sum;
    this.count = count;
  }

  @Override
  public void add(Double value) {
    sum += value;
    count++;
  }

  @Override
  public DoubleAvgAggregation merge(AggregationOperator<Double> otherAgg) {
    var otherAvg = (DoubleAvgAggregation) otherAgg;
    return new DoubleAvgAggregation(sum + otherAvg.sum, count + otherAvg.count);
  }

  @Override
  public Double computeResult() {
    return sum / count;
  }
}
