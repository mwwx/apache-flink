package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.types.logical.RowType;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for Common.
 */
public class CommonRowConverter extends AbstractJdbcRowConverter {

	private static final long serialVersionUID = 1L;

	@Override
	public String converterName() {
		return "MySQL";
	}

	public CommonRowConverter(RowType rowType) {
		super(rowType);
	}
}
