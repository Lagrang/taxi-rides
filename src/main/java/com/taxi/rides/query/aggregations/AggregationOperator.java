package com.taxi.rides.query.aggregations;

public interface AggregationOperator<R> {

  void add(R value);

  AggregationOperator<R> merge(AggregationOperator<R> otherAgg);

  R computeResult();
}
