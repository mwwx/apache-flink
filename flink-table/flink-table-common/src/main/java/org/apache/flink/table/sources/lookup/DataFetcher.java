package org.apache.flink.table.sources.lookup;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import org.mapdb.HTreeMap;

import java.io.Serializable;
import java.util.List;

/**
 * DataFetcher for lookup.
 */
public interface DataFetcher extends Serializable {

	void open();

	void loadAllData(HTreeMap<Row, List<Row>> cache) throws Exception;

	List<Row> syncRequestByKey(Row key) throws Exception;

	boolean supportAsync();

	RowTypeInfo getResultType();

	TypeInformation<?>[] getParameterTypes(Class<?>[] signature);

	void close() throws Exception;

}
