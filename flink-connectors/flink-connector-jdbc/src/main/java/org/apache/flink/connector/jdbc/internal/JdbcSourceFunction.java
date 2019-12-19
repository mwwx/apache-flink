package org.apache.flink.connector.jdbc.internal;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * rich source function for reading.
 */
public class JdbcSourceFunction extends RichSourceFunction<Row> implements CheckpointedFunction {

	private final JdbcInputFormat inputFormat;

	private final int arity;
	private final AtomicBoolean run = new AtomicBoolean(true);
	private boolean unFinishAble = false;

	private transient ListState<Long> posState;
	private static final String POSTATE_NAME = "posState";

	public JdbcSourceFunction(JdbcInputFormat inputFormat, int arity) {
		this.inputFormat = inputFormat;
		this.arity = arity;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		RuntimeContext ctx = getRuntimeContext();
		inputFormat.setRuntimeContext(ctx);
		inputFormat.openInputFormat();
		inputFormat.open(new GenericInputSplit(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks()));
		unFinishAble = !StringUtils.isEmpty(inputFormat.getIncreaseColumn());
	}

	@Override
	public void run(SourceContext<Row> ctx) throws Exception {
		Row row = new Row(arity);
		//reuse row
		if (unFinishAble) {
			while (run.get()) {
				row = inputFormat.nextRecord(row);
				if (row != null) {
					ctx.collect(row);
				} else {
					row = new Row(arity);
					Thread.sleep(100);
				}
			}
		} else {
			while (run.get()) {
				row = inputFormat.nextRecord(row);
				if (row != null) {
					ctx.collect(row);
				} else {
					row = new Row(arity);
				}
				if (inputFormat.reachedEnd()) {
					break;
				}
			}
		}

	}

	@Override
	public void cancel() {
		try {
			inputFormat.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			run.set(false);
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		posState.clear();
		posState.add(inputFormat.getCurrentPos());
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		OperatorStateStore stateStore = context.getOperatorStateStore();
		ListStateDescriptor<Long> posStateDescriptor = new ListStateDescriptor<>(POSTATE_NAME, Long.class);
		posState = stateStore.getListState(posStateDescriptor);
		if (context.isRestored()) {
			if (posState.get() != null) {
				Iterator<Long> iterator = posState.get().iterator();
				if (iterator.hasNext()) {
					inputFormat.setCurrentPos(iterator.next());
				}
			}
		}
	}
}
