package com.taxi.rides.storage;

import com.taxi.rides.storage.schema.Schema;
import java.util.Iterator;

public interface RowReader extends Iterator<Row>, AutoCloseable {

  /**
   * Returns schema of rows returned by this reader.
   */
  Schema schema();
}
