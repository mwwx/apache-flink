package org.apache.flink.connector.jdbc.dialect;

import org.apache.flink.connector.jdbc.internal.converter.CommonRowConverter;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * CommonDialect.
 */
public class CommonDialect extends AbstractDialect {

	private final String driver;
	private boolean isQuoting = false;

	// Define MAX/MIN precision of TIMESTAMP type according to Linkoopdb docs:
	private static final int MAX_TIMESTAMP_PRECISION = 6;
	private static final int MIN_TIMESTAMP_PRECISION = 1;

	// Define MAX/MIN precision of DECIMAL type according to Linkoopdb docs:
	private static final int MAX_DECIMAL_PRECISION = 38;
	private static final int MIN_DECIMAL_PRECISION = 1;

	public CommonDialect(String driver) {
		Preconditions.checkNotNull(driver, "driver can't be null");
		this.driver = driver;
	}

	@Override
	public String dialectName() {
		return "Common";
	}

	@Override
	public boolean canHandle(String url) {
		return true;
	}

	@Override
	public JdbcRowConverter getRowConverter(RowType rowType) {
		return new CommonRowConverter(rowType);
	}

	@Override
	public Optional<String> defaultDriverName() {
		return Optional.of(driver);
	}

	@Override
	public boolean isQuoting() {
		return isQuoting;
	}

	@Override
	public void setQuoting(boolean quoting) {
		this.isQuoting = quoting;
	}

	public static CommonDialect newInstance(String driver) {
		return new CommonDialect(driver);
	}

	@Override
	public int maxDecimalPrecision() {
		return MAX_DECIMAL_PRECISION;
	}

	@Override
	public int minDecimalPrecision() {
		return MIN_DECIMAL_PRECISION;
	}

	@Override
	public int maxTimestampPrecision() {
		return MAX_TIMESTAMP_PRECISION;
	}

	@Override
	public int minTimestampPrecision() {
		return MIN_TIMESTAMP_PRECISION;
	}

	@Override
	public List<LogicalTypeRoot> unsupportedTypes() {
		return Arrays.asList(
			LogicalTypeRoot.BINARY,
			LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
			LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
			LogicalTypeRoot.INTERVAL_YEAR_MONTH,
			LogicalTypeRoot.INTERVAL_DAY_TIME,
			LogicalTypeRoot.ARRAY,
			LogicalTypeRoot.MULTISET,
			LogicalTypeRoot.MAP,
			LogicalTypeRoot.ROW,
			LogicalTypeRoot.DISTINCT_TYPE,
			LogicalTypeRoot.STRUCTURED_TYPE,
			LogicalTypeRoot.NULL,
			LogicalTypeRoot.RAW,
			LogicalTypeRoot.SYMBOL,
			LogicalTypeRoot.UNRESOLVED
		);
	}
}
