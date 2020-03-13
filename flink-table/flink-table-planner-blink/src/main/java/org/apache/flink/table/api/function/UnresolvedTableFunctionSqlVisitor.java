package org.apache.flink.table.api.function;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.util.SqlVisitor;

/**
 * ast tree visitor.
 */
public class UnresolvedTableFunctionSqlVisitor implements SqlVisitor {

	private StreamTableEnvironment env;
	private TableFunctionInterceptor interceptor = TableFunctionInterceptor.getInstance();

	public UnresolvedTableFunctionSqlVisitor(StreamTableEnvironment env) {
		this.env = env;
	}

	@Override
	public Object visit(SqlCall call) {
		if (call == null) {
			return null;
		}
		//stream doesn't support order by, ignore, the following process will throw exception.
		if (call instanceof SqlSelect) {
			SqlSelect select = ((SqlSelect) call);
			//visit possible sub query
			//visit from
			SqlNode from = select.getFrom();
			visit(from);

			//visit where
			SqlNode where = select.getWhere();
			visit(where);

			//visit having
			SqlNode having = select.getHaving();
			visit(having);
		} else if (call instanceof SqlInsert) {
			SqlInsert insert = (SqlInsert) call;
			visit(insert.getSource());
		} else if (call instanceof SqlJoin) {
			SqlJoin join = (SqlJoin) call;

			//visit left
			visit(join.getLeft());

			//visit right
			visit(join.getRight());
		} else if (call instanceof SqlCase) {
			SqlCase sqlCase = (SqlCase) call;

			//visit case
			visit(sqlCase.getValueOperand());

			//visit when
			visit(sqlCase.getWhenOperands());

			//visit then
			visit(sqlCase.getThenOperands());

			//visit else
			visit(sqlCase.getElseOperand());
		} else if (call instanceof SqlBasicCall) {
			SqlBasicCall sqlBasicCall = ((SqlBasicCall) call);
			visit(sqlBasicCall.getOperator(), sqlBasicCall.operands);
		}
		return null;
	}

	public void visit(SqlOperator operator, SqlNode[] operands) {
		//only process function
		if (operator instanceof SqlUnresolvedFunction) {
			SqlUnresolvedFunction unresolvedFunction = ((SqlUnresolvedFunction) operator);
			interceptor.intercept(env, unresolvedFunction, operands);
		} else {
			if (operands != null) {
				for (SqlNode operand : operands) {
					visit(operand);
				}
			}
		}
	}

	private void visit(SqlNode node) {
		if (node instanceof SqlCall) {
			visit((SqlCall) node);
		}
	}

	// no use

	@Override
	public Object visit(SqlLiteral literal) {
		return null;
	}

	@Override
	public Object visit(SqlNodeList nodeList) {
		return null;
	}

	public Object visit(SqlIdentifier sqlIdentifier) {
		return null;
	}

	@Override
	public Object visit(SqlDataTypeSpec type) {
		return null;
	}

	@Override
	public Object visit(SqlDynamicParam param) {
		return null;
	}

	@Override
	public Object visit(SqlIntervalQualifier intervalQualifier) {
		return null;
	}

}
