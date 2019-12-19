package org.apache.flink.streaming.connectors.parquet;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * ParquetSourceUtil.
 */
public class ParquetSourceUtil {

	public static <OUT> DataStreamSource<OUT> createInput(
		StreamExecutionEnvironment environment,
		InputFormat<OUT, ?> inputFormat,
		TypeInformation<OUT> typeInfo,
		String path) {
		DataStreamSource<OUT> source;

		source = createLoopFileInput(
			environment,
			inputFormat,
			path,
			typeInfo,
			FileProcessingMode.PROCESS_CONTINUOUSLY,
			100);
		return source;
	}

	private static  <OUT> DataStreamSource<OUT> createLoopFileInput(
		StreamExecutionEnvironment environment,
		InputFormat<OUT, ?> inputFormat,
		String path,
		TypeInformation<OUT> typeInfo,
		FileProcessingMode monitoringMode,
		long interval) {

		@SuppressWarnings("unchecked")
		FileInputFormat<OUT> format = (FileInputFormat<OUT>) inputFormat;

		/*ContinuousFileMonitoringFunction<OUT> monitoringFunction = new ContinuousFileMonitoringFunction<>(
			format, monitoringMode, environment.getParallelism(), interval);

		ContinuousFileReaderOperatorFactory<OUT, ? extends TimestampedInputSplit> factory =
			new ContinuousFileReaderOperatorFactory<>(inputFormat);

		SingleOutputStreamOperator<OUT> source = environment
			.addSource(monitoringFunction, sourceName)
			.transform("Split Reader: " + sourceName, typeInfo, factory)
			.transform();

		return new DataStreamSource<>(source);*/
		return environment.readFile(format, path, monitoringMode, interval, typeInfo);
	}
}
