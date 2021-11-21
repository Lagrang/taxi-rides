package com.taxi.rides.storage.index;

import static org.assertj.core.api.Assertions.assertThatObject;

import com.google.common.collect.Range;
import com.taxi.rides.storage.QueryPredicate.NotEqual;
import com.taxi.rides.storage.schema.Column;
import com.taxi.rides.storage.schema.datatypes.LongDataType;
import org.junit.jupiter.api.Test;

public class NotNullColumnIndexTest {

  @Test
  void emptyIndex() {
    var column = new Column<>("col", new LongDataType());
    var index = new NotNullColumnIndex<>(column);
    assertThatObject(index.evaluateNotEquals(new NotEqual<>(column, null))).isEqualTo(Range.all());
  }

  @Test
  void allNulls() {
    var column = new Column<>("col", new LongDataType());
    var index = new NotNullColumnIndex<>(column);
    for (int i = 0; i < 1000; i++) {
      index.addEntry(i, null);
    }

    assertThatObject(index.evaluateNotEquals(new NotEqual<>(column, null)))
        .matches(r -> r.encloses(Range.atLeast(1000L)));
  }

  @Test
  void allNotNulls() {
    var column = new Column<>("col", new LongDataType());
    var index = new NotNullColumnIndex<>(column);
    for (int i = 0; i < 1000; i++) {
      index.addEntry(i, 1L);
    }

    assertThatObject(index.evaluateNotEquals(new NotEqual<>(column, null)))
        .matches(r -> r.encloses(Range.closed(0L, 999L)));
  }

  @Test
  void mixed() {
    var column = new Column<>("col", new LongDataType());
    var index = new NotNullColumnIndex<>(column);
    index.addEntry(0, null);
    index.addEntry(1, 1L);
    index.addEntry(2, 1L);
    index.addEntry(3, null);
    index.addEntry(4, null);

    assertThatObject(index.evaluateNotEquals(new NotEqual<>(column, null)))
        .matches(r -> r.encloses(Range.closed(1L, 2L)));
  }
}
