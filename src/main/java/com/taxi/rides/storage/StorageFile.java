package com.taxi.rides.storage;

import com.taxi.rides.storage.schema.Column;
import java.io.IOException;
import java.util.List;

public interface StorageFile {
  RowReader openReader(List<Column> requiredColumns, QueryPredicate predicate) throws IOException;
}
