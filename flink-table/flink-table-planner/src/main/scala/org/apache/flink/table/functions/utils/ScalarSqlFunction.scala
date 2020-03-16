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

package org.apache.flink.table.functions.utils

import com.google.common.primitives.Primitives
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.functions.utils.ScalarSqlFunction.createReturnTypeInference
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._

import scala.collection.JavaConverters._

/**
  * Calcite wrapper for user-defined scalar functions.
  *
  * @param name function name (used by SQL parser)
  * @param displayName name to be displayed in operator name
  * @param scalarFunction scalar function to be called
  * @param typeFactory type factory for converting Flink's between Calcite's types
  */
class ScalarSqlFunction(
    name: String,
    displayName: String,
    scalarFunction: ScalarFunction,
    typeFactory: FlinkTypeFactory)
  extends SqlFunction(
    new SqlIdentifier(name, SqlParserPos.ZERO),
    createReturnTypeInference(name, scalarFunction, typeFactory),
    createEvalOperandTypeInference(name, scalarFunction, typeFactory),
    createEvalOperandTypeChecker(name, scalarFunction),
    null,
    SqlFunctionCategory.USER_DEFINED_FUNCTION) {

  def getScalarFunction: ScalarFunction = scalarFunction

  override def isDeterministic: Boolean = scalarFunction.isDeterministic

  override def isDynamicFunction: Boolean = scalarFunction.isDynamicResultType

  override def toString: String = displayName
}

object ScalarSqlFunction {

  private[flink] def createReturnTypeInference(
      name: String,
      scalarFunction: ScalarFunction,
      typeFactory: FlinkTypeFactory)
    : SqlReturnTypeInference = {
    /**
      * Return type inference based on [[ScalarFunction]] given information.
      */
    new SqlReturnTypeInference {
      override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType = {
        val parameterTypes = opBinding
          .collectOperandTypes()
          .asScala
          .map { operandType =>
            if (operandType.getSqlTypeName == SqlTypeName.NULL) {
              null
            } else {
              FlinkTypeFactory.toTypeInfo(operandType)
            }
          }
        val foundSignature = getEvalMethodSignature(scalarFunction, parameterTypes)
        if (foundSignature.isEmpty) {
          throw new ValidationException(
            s"Given parameters of function '$name' do not match any signature. \n" +
              s"Actual: ${signatureToString(parameterTypes)} \n" +
              s"Expected: ${signaturesToString(scalarFunction, "eval")}")
        }
        val parameterCount = opBinding.getOperandCount
        val foundParameter: Array[AnyRef] = new Array[AnyRef](parameterCount)

        val foundSignatures: Array[Class[_]] = foundSignature.get
        if (scalarFunction.isDynamicResultType) {
          for (index <- 0 until parameterCount) {
            if (opBinding.isOperandLiteral(index, false)
              && !opBinding.isOperandNull(index, false)) {
              val value = opBinding.getOperandLiteralValue(
                index, primitiveTypeConverter(foundSignatures(index), name: String): Class[_])
              foundParameter(index) = value.asInstanceOf[AnyRef]
            } else {
              foundParameter(index) = null
            }
          }
        }

        val resultType = getResultTypeOfScalarFunction(scalarFunction,
          foundSignatures, foundParameter)
        typeFactory.createTypeFromTypeInfo(resultType, isNullable = true)
      }
    }
  }

  private[flink] def primitiveTypeConverter(typeClass: Class[_],
    name: String): Class[_] ={
    if (typeClass.isPrimitive) {
      Primitives.wrap(typeClass)
    } else if (typeClass == classOf[Int]
      || typeClass == classOf[String]
      || typeClass == classOf[Byte]
      || typeClass == classOf[Short]
      || typeClass == classOf[Long]
      || typeClass == classOf[Double]
      || typeClass == classOf[Float]) {
      typeClass
    } else {
      throw new ValidationException(
        s"Given parameters of function '$name' with dynamic result do not support data type. \n" +
          s"Actual: $typeClass")
    }
  }
}
