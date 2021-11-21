package com.taxi.rides.storage;

import com.taxi.rides.storage.schema.Schema;
import java.util.Iterator;

public interface RowReader extends Iterator<Row>, AutoCloseable {

  /** Returns schema of rows returned by this reader. */
  Schema schema();

  /** Print reader internal statistics to stdout. */
  void printStats();

  static RowReader empty(Schema schema) {
    return new Empty(schema);
  }

  class Empty implements RowReader {

    private final Schema schema;

    public Empty(Schema schema) {
      this.schema = schema;
    }

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public void printStats() {}

    @Override
    public void close() {}

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public Row next() {
      throw new UnsupportedOperationException();
    }
  }
}
