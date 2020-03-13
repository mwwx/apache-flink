package org.apache.flink.table.api.function;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDateLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlTimeLiteral;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.sql.SqlUnresolvedFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * TableFunction interceptor.
 */
public class TableFunctionInterceptor {

	private Map<String, DynamicReturnTypeTableFunction> registeredUnusableTableFunction = new HashMap<>();

	private TableFunctionInterceptor() {
	}

	public static TableFunctionInterceptor getInstance() {
		return Inner.INSTANCE;
	}

	public void register(String tableFunctionName, DynamicReturnTypeTableFunction unUseableTableFunction) {
		Preconditions.checkNotNull(tableFunctionName, "function name must not be null.");
		Preconditions.checkNotNull(unUseableTableFunction, "function must not be null.");
		registeredUnusableTableFunction.put(tableFunctionName.toUpperCase(), unUseableTableFunction);
	}

	public synchronized void intercept(StreamTableEnvironment env, SqlUnresolvedFunction function, SqlNode[] operands) {
		Preconditions.checkNotNull(env, "tableEnvironment must not be null.");
		Preconditions.checkNotNull(function, "function must not be null.");
		Preconditions.checkNotNull(operands, "operands must not be null.");

		String tableFunctionName = function.getName().toUpperCase();

		//TODO:正确性待验证
		Object[] args = new Object[operands.length];
		for (int i = 0; i < operands.length; i++) {
			SqlNode param = operands[i];
			if (param instanceof SqlIdentifier) {
				args[i] = operands[i].toString();
			} else if (param instanceof SqlNumericLiteral) {
				SqlNumericLiteral numericLiteral = ((SqlNumericLiteral) param);
				args[i] = numericLiteral.getValue();
			} else if (param instanceof SqlBinaryStringLiteral) {
				SqlBinaryStringLiteral binaryStringLiteral = ((SqlBinaryStringLiteral) param);
				args[i] = binaryStringLiteral.getValue();
			} else if (param instanceof SqlCharStringLiteral) {
				SqlCharStringLiteral charStringLiteral = ((SqlCharStringLiteral) param);
				args[i] = charStringLiteral.getNlsString().getValue();
			} else if (param instanceof SqlDateLiteral) {
				SqlDateLiteral dateLiteral = ((SqlDateLiteral) param);
				args[i] = dateLiteral.getValue();
			}  else if (param instanceof SqlTimestampLiteral) {
				SqlTimestampLiteral timestampLiteral = ((SqlTimestampLiteral) param);
				args[i] = timestampLiteral.getValue();
			} else if (param instanceof SqlTimeLiteral) {
				SqlTimeLiteral timeLiteral = ((SqlTimeLiteral) param);
				args[i] = timeLiteral.getValue();
			} else {
				//other type
				args[i] = null;
			}
		}

		if (registeredUnusableTableFunction.containsKey(tableFunctionName)) {
			DynamicReturnTypeTableFunction unusableTableFunction = registeredUnusableTableFunction.get(tableFunctionName);
			DynamicReturnTypeTableFunction usableTableFunction = unusableTableFunction.newInstance(args);
			String newFunctionName = unusableTableFunction.getNextInternalCatalogFunctionName();
			SqlIdentifier sqlIdentifier = function.getSqlIdentifier();
			sqlIdentifier.setNames(ImmutableList.of(newFunctionName), ImmutableList.of(sqlIdentifier.getComponentParserPosition(0)));
			env.registerFunction(newFunctionName, usableTableFunction);
		}
	}

	private static class Inner {
		static final TableFunctionInterceptor INSTANCE = new TableFunctionInterceptor();
	}
}
