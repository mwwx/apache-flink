/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.runtime.utils

import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CompletableFuture, ExecutorService, Executors}
import java.util.function.{Consumer, Supplier}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.table.api.{TableEnvironment, TableSchema}
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE
import org.apache.flink.table.descriptors.Schema.SCHEMA
import org.apache.flink.table.descriptors.{CustomConnectorDescriptor, DescriptorProperties, Schema}
import org.apache.flink.table.factories.TableSourceFactory
import org.apache.flink.table.functions.{AsyncTableFunction, FunctionContext, TableFunction}
import org.apache.flink.table.planner.runtime.utils.DimLookupableTableSource.{DimAsyncLookupFunction, DimInputSource, DimLookupFunction}
import org.apache.flink.table.planner.runtime.utils.InMemoryLookupableTableSource.RESOURCE_COUNTER
import org.apache.flink.table.sources._
import org.apache.flink.table.sources.lookup.LookupOptions
import org.apache.flink.table.sources.lookup.cache.CacheStrategy
import org.apache.flink.table.types.DataType
import org.apache.flink.table.utils.EncodingUtils
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

import scala.annotation.varargs
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * A [[LookupableTableSource]] which stores table in memory, this is mainly used for testing.
  */
class DimLookupableTableSource(
    schema: TableSchema,
    data: List[Row],
    asyncEnabled: Boolean,
    bounded: Boolean = false)
  extends LookupableTableSource[Row]
  with StreamTableSource[Row] {

  override def getLookupFunction(lookupKeys: Array[String]): TableFunction[Row] = {
    new DimLookupFunction(convertDataToMap(lookupKeys), RESOURCE_COUNTER)
  }

  override def getAsyncLookupFunction(lookupKeys: Array[String]): AsyncTableFunction[Row] = {
    new DimAsyncLookupFunction(convertDataToMap(lookupKeys), RESOURCE_COUNTER)
  }

  private def convertDataToMap(lookupKeys: Array[String]): Map[Row, List[Row]] = {
    val lookupFieldIndexes = lookupKeys.map(schema.getFieldNames.indexOf(_))
    val map = mutable.HashMap[Row, List[Row]]()
    data.foreach { row =>
      val key = Row.of(lookupFieldIndexes.map(row.getField): _*)
      val oldValue = map.get(key)
      if (oldValue.isDefined) {
        map.put(key, oldValue.get ++ List(row))
      } else {
        map.put(key, List(row))
      }
    }
    map.toMap
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    execEnv.addSource(new DimInputSource(data, RESOURCE_COUNTER))
      .returns(schema.toRowType)
      .name(explainSource())
  }

  override def isAsyncEnabled: Boolean = asyncEnabled

  override def getProducedDataType: DataType = schema.toRowDataType

  override def supportedCacheStrategies(): Array[CacheStrategy] = Array(CacheStrategy.ALL)

  override def getLookupOptions: LookupOptions = LookupOptions
    .builder().setDataType(TableDataType.STATIC).build()

  override def getTableSchema: TableSchema = schema


  override def isBounded: Boolean = bounded

}

class DimLookupableTableFactory extends TableSourceFactory[Row] {

  override def createTableSource(properties: util.Map[String, String]): TableSource[Row] = {
    val dp = new DescriptorProperties
    dp.putProperties(properties)
    val tableSchema = dp.getTableSchema(SCHEMA)

    val serializedData = dp.getString("data")
    val data = EncodingUtils.decodeStringToObject(serializedData, classOf[List[Row]])

    val asyncEnabled = dp.getOptionalBoolean("is-async").orElse(false)

    val bounded = dp.getOptionalBoolean("is-bounded").orElse(false)

    new DimLookupableTableSource(tableSchema, data, asyncEnabled, bounded)
  }

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, "DimLookupableTable")
    context
  }

  override def supportedProperties(): util.List[String] = {
    val supported = new util.ArrayList[String]()
    supported.add("*")
    supported
  }
}

object DimLookupableTableSource {

  val RESOURCE_COUNTER = new AtomicInteger()

  def createDimTable(
      tEnv: TableEnvironment,
      isAsync: Boolean,
      data: List[Row],
      schema: TableSchema,
      tableName: String,
      isBounded: Boolean = false): Unit = {
    tEnv.connect(
      new CustomConnectorDescriptor("DimLookupableTable", 1, false)
        .property("is-async", if (isAsync) "true" else "false")
        .property("is-bounded", if (isBounded) "true" else "false")
        .property("data", EncodingUtils.encodeObjectToString(data)))
      .withSchema(new Schema().schema(schema))
      .createTemporaryTable(tableName)
  }

  private class DimInputSource(
      data: List[Row],
      resourceCounter: AtomicInteger)
    extends RichSourceFunction[Row] {


    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      resourceCounter.incrementAndGet()
    }

    override def run(ctx: SourceFunction.SourceContext[Row]): Unit = {
      data.foreach(result => ctx.collect(result))
    }

    override def cancel(): Unit = {
      resourceCounter.decrementAndGet()
    }
  }

  /**
    * A lookup function which find matched rows with the given fields.
    */
  private class DimLookupFunction(
      data: Map[Row, List[Row]],
      resourceCounter: AtomicInteger)
    extends TableFunction[Row] {

    override def open(context: FunctionContext): Unit = {
      resourceCounter.incrementAndGet()
    }

    @varargs
    def eval(inputs: AnyRef*): Unit = {
      val key = Row.of(inputs: _*)
      Preconditions.checkArgument(!inputs.contains(null),
        s"Lookup key %s contains null value, which would not happen.", key)
      data.get(key) match {
        case Some(list) => list.foreach(result => collect(result))
        case None => // do nothing
      }
    }

    override def close(): Unit = {
      resourceCounter.decrementAndGet()
    }
  }

  /**
    * An async lookup function which find matched rows with the given fields.
    */
  @SerialVersionUID(1L)
  private class DimAsyncLookupFunction(
      data: Map[Row, List[Row]],
      resourceCounter: AtomicInteger,
      delayedReturn: Int = 0)
    extends AsyncTableFunction[Row] {

    @transient
    var executor: ExecutorService = _

    override def open(context: FunctionContext): Unit = {
      resourceCounter.incrementAndGet()
      executor = Executors.newSingleThreadExecutor()
    }

    @varargs
    def eval(resultFuture: CompletableFuture[util.Collection[Row]], inputs: AnyRef*): Unit = {
      val key = Row.of(inputs: _*)
      Preconditions.checkArgument(!inputs.contains(null),
        s"Lookup key %s contains null value, which would not happen.", key)
      CompletableFuture
        .supplyAsync(new CollectionSupplier(data, key), executor)
        .thenAccept(new CollectionConsumer(resultFuture))
    }

    override def close(): Unit = {
      resourceCounter.decrementAndGet()
      if (null != executor && !executor.isShutdown) {
        executor.shutdown()
      }
    }

    private class CollectionSupplier(data: Map[Row, List[Row]], key: Row)
        extends Supplier[util.Collection[Row]] {

      override def get(): util.Collection[Row] = {
        val list = data.get(key)
        if (list.isDefined && list.get.nonEmpty) {
          list.get.asJavaCollection
        } else {
          Collections.emptyList()
        }
      }
    }

    private class CollectionConsumer(resultFuture: CompletableFuture[util.Collection[Row]])
        extends Consumer[util.Collection[Row]] {

      override def accept(results: util.Collection[Row]): Unit = {
        resultFuture.complete(results)
      }
    }
  }

}
