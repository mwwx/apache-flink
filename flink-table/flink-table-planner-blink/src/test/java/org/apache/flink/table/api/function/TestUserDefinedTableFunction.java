package org.apache.flink.table.api.function;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

/**
 * TestUserDefinedTableFunction.
 */
public class TestUserDefinedTableFunction extends DynamicReturnTypeTableFunction {

	private RowTypeInfo rowTypeInfo;

	public TestUserDefinedTableFunction() {

	}

	public TestUserDefinedTableFunction(Object params) {
		rowTypeInfo = new RowTypeInfo(new TypeInformation[]{Types.STRING}, new String[]{"str"});
	}

	public void eval(Integer integer) {
		collect(Row.of(integer.toString()));
	}

	@Override
	protected RowTypeInfo getReturnType() {
		if (rowTypeInfo == null) {
			return new RowTypeInfo();
		}
		return rowTypeInfo;
	}

	@Override
	public String getFunctionName() {
		return "fun";
	}
}
