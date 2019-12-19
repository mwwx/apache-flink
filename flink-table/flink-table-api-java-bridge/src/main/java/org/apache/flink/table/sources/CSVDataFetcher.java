package org.apache.flink.table.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.table.sources.lookup.DataFetcher;
import org.apache.flink.types.Row;

import org.mapdb.HTreeMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * CSVDataFetcher.
 */
public class CSVDataFetcher implements DataFetcher {

	private final CsvTableSource.CsvInputFormatConfig config;

	private int[] keyIndex;

	public CSVDataFetcher(CsvTableSource.CsvInputFormatConfig config, String[] lookupKeys) {
		this.config = config;
		this.keyIndex = new int[lookupKeys.length];
		List<String> fields = Arrays.asList(config.getSelectedFieldNames());
		for (int i = 0; i < lookupKeys.length; i++) {
			keyIndex[i] = fields.indexOf(lookupKeys[i]);
		}
	}

	@Override
	public void open() {
	}

	@Override
	public void loadAllData(HTreeMap<Row, List<Row>> cache) throws Exception {
		TypeInformation<Row> rowType = getResultType();

		RowCsvInputFormat inputFormat = config.createInputFormat();
		FileInputSplit[] inputSplits = inputFormat.createInputSplits(1);
		for (FileInputSplit split : inputSplits) {
			inputFormat.open(split);
			Row row = new Row(rowType.getArity());
			for ( ; ; ) {
				Row r = inputFormat.nextRecord(row);
				if (r == null) {
					break;
				} else {
					Row key = Row.project(r, keyIndex);
					List<Row> rows = cache.computeIfAbsent(key, k -> new ArrayList<>());
					rows.add(Row.copy(r));
					cache.put(key, rows);
				}
			}
			inputFormat.close();
		}
		inputFormat.closeInputFormat();
	}

	@Override
	public List<Row> syncRequestByKey(Row key) throws Exception {
		throw new UnsupportedOperationException("CSVDataFetcher doesn't support request data. ");
	}

	@Override
	public boolean supportAsync() {
		return false;
	}

	@Override
	public RowTypeInfo getResultType() {
		return new RowTypeInfo(config.getSelectedFieldTypes(), config.getSelectedFieldNames());
	}

	@Override
	public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
		return new TypeInformation[0];
	}

	@Override
	public void close() throws Exception {

	}
}
