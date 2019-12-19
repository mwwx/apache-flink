package org.apache.flink.table.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * StreamITCase.
 */
public class StreamITCase {

	private static final List<String> testResults = new ArrayList<>();
	private static final List<String> retractedResults = new ArrayList<>();

	public static void clear() {
		testResults.clear();
		retractedResults.clear();
	}

	public static void compareWithList(List<String> expected) {
		Collections.sort(expected);
		Collections.sort(testResults);
	}

	/**
	 * StringSink.
	 * @param <T>
	 */
	public static final class StringSink<T> extends RichSinkFunction<T> {
		public void invoke(T value) {
			synchronized (testResults) {
				testResults.add(value.toString());
			}
		}
	}

	/**
	 * RetractMessagesSink.
	 */
	public static final class RetractMessagesSink extends RichSinkFunction<Tuple2<Boolean, Row>> {
		public void invoke(Tuple2<Boolean, Row> v) {
			synchronized (testResults) {
				if (v.f0) {
					testResults.add("+" + v.f1);
				} else {
					testResults.add("-" + v.f1);
				}
			}
		}
	}

	/**
	 * RetractingSink.
	 */
	public static final class RetractingSink extends RichSinkFunction<Tuple2<Boolean, Row>> {
		public void invoke(Tuple2<Boolean, Row> v) {
			synchronized (testResults) {
				String value = v.f1.toString();
				if (v.f0) {
					retractedResults.add(value);
				} else {
					int idx = retractedResults.indexOf(value);
					if (idx >= 0) {
						retractedResults.remove(idx);
					} else {
						throw new RuntimeException("Tried to retract a value that wasn't added first. " +
							"This is probably an incorrectly implemented test. " +
							"Try to set the parallelism of the sink to 1.");
					}
				}
			}
		}
	}
}
