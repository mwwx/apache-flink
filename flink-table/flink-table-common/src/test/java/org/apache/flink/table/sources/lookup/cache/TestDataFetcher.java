package org.apache.flink.table.sources.lookup.cache;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.sources.lookup.DataFetcher;
import org.apache.flink.types.Row;

import org.mapdb.HTreeMap;

import java.util.ArrayList;
import java.util.List;

/**
 * TestDataFetcher.
 */
public class TestDataFetcher implements DataFetcher {

	@Override
	public void open() {
	}

	@Override
	public void loadAllData(HTreeMap<Row, List<Row>> cache) throws Exception {
		for (int i = 0; i < 100; i++) {
			List<Row> list = new ArrayList<>();
			list.add(Row.of(i));
			cache.put(Row.of(i), list);
		}
	}

	@Override
	public List<Row> syncRequestByKey(Row key) throws Exception {
		List<Row> list = new ArrayList<>();
		list.add(key);
		return list;
	}

	@Override
	public boolean supportAsync() {
		return false;
	}

	@Override
	public RowTypeInfo getResultType() {
		return new RowTypeInfo(new TypeInformation[]{Types.INT}, new String[]{"a"});
	}

	@Override
	public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
		return new TypeInformation[0];
	}

	@Override
	public void close() throws Exception {

	}
}
