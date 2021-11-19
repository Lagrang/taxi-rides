package com.taxi.rides.storage;

import com.taxi.rides.storage.index.ColumnIndex;
import com.taxi.rides.storage.index.RowLocator;
import com.taxi.rides.storage.schema.Column;
import com.taxi.rides.storage.schema.Schema;
import de.siegmar.fastcsv.reader.CloseableIterator;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
import de.siegmar.fastcsv.reader.NamedCsvReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class CsvStorageFile {

  private final Path csvPath;
  private final Schema csvSchema;
  private final RowLocator rowLocator;

  public CsvStorageFile(
      Path csvPath,
      Schema expectedSchema,
      RowLocator rowLocator,
      List<ColumnIndex> indexesToPopulate)
      throws IOException {
    this.csvPath = Objects.requireNonNull(csvPath, "CSV file path missed");

    try (var reader = NamedCsvReader.builder().fieldSeparator(',').build(csvPath)) {
      // redefine schema object according to order of columns inside CSV file
      var columns = new ArrayList<Column>();
      for (String colName : reader.getHeader()) {
        // no schema evolution right now: fail if we can't find some required column in CSV
        Column column =
            expectedSchema
                .getColumn(colName)
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            colName + " column not exists in CSV file " + csvPath));
        columns.add(column);
      }
      this.csvSchema = new Schema(columns);
    }

    try (var reader = CsvReader.builder().fieldSeparator(',').build(csvPath)) {
      populateIndexes(reader, csvSchema, rowLocator, indexesToPopulate);
    }
    this.rowLocator = rowLocator;
  }

  private static void populateIndexes(
      CsvReader reader, Schema schema, RowLocator rowLocator, List<ColumnIndex> indexesToPopulate) {

    var indexes =
        indexesToPopulate.stream()
            .map(
                index ->
                    new IndexState(index, schema.getColumnIndex(index.column().name()).getAsInt()))
            .collect(Collectors.toList());

    reader
        .iterator()
        .forEachRemaining(
            row -> {
              rowLocator.addRow(row.getOriginalLineNumber(), row.getStartingOffset());
              for (IndexState indexState : indexes) {
                var rawColVal = row.getField(indexState.columnIndex);
                var colValue = indexState.index.column().dataType().parseFrom(rawColVal);
                indexState.index.addEntry(row.getOriginalLineNumber(), colValue);
              }
            });
  }

  public RowReader newReader(String[] requiredColumns) throws IOException {
    int[] colIdx = new int[requiredColumns.length];
    int i = 0;
    for (String reqCol : requiredColumns) {
      int index =
          csvSchema
              .getColumnIndex(reqCol)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          reqCol + " not contained in CSV file" + csvPath));
      colIdx[i++] = index;
    }

    return new CsvIter(colIdx);
  }

  record IndexState(ColumnIndex index, int columnIndex) {}

  private class CsvIter implements RowReader {

    private final Schema readerSchema;
    private final CsvReader csvReader;
    private final CloseableIterator<CsvRow> rowIter;
    private final int[] colIdx;

    public CsvIter(int[] colIdx) throws IOException {
      this.colIdx = colIdx;
      csvReader = CsvReader.builder().fieldSeparator(',').build(csvPath);
      rowIter = csvReader.iterator();
      readerSchema =
          new Schema(
              Arrays.stream(colIdx).mapToObj(csvSchema::getColumnAt).collect(Collectors.toList()));
    }

    @Override
    public void close() throws Exception {
      csvReader.close();
    }

    @Override
    public boolean hasNext() {
      return rowIter.hasNext();
    }

    @Override
    public Row next() {
      var result = new Row(colIdx.length);
      var row = rowIter.next();
      for (int idx : colIdx) {
        String rawVal = row.getField(idx);
        var columnValue = csvSchema.getColumnAt(idx).dataType().parseFrom(rawVal);
        result.set(idx, columnValue);
      }
      return result;
    }

    @Override
    public Schema schema() {
      return readerSchema;
    }
  }
}
