/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.glutenproject.backendsapi.glutendata

import io.glutenproject.backendsapi.IValidatorApi
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.utils.GlutenDataExpressionUtil
import io.glutenproject.vectorized.GlutenDataNativeExpressionEvaluator

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}

abstract class GlutenDataValidatorApi extends IValidatorApi {

  /**
   * Validate target expression within an input blacklist. Return false if target expression
   * (with the information of its args' types) matches any of the entry in the blacklist.
   */
  protected def doValidate(blacklist: Map[String, Set[String]], expr: Expression): Boolean = {
    // To handle cast(struct as string) AS col_name expression
    val key = if (expr.prettyName.toLowerCase().equals("alias")) {
      expr.asInstanceOf[Alias].child.prettyName.toLowerCase()
    } else expr.prettyName.toLowerCase()
    val value = blacklist.get(key)
    if (value.isEmpty) {
      return true
    }
    val inputTypeNames = value.get
    inputTypeNames.foreach { inputTypeName =>
      if (inputTypeName.equals(GlutenDataExpressionUtil.EMPTY_TYPE)) {
        return false
      } else {
        for (input <- expr.children) {
          if (inputTypeName.equals(input.dataType.typeName)) {
            return false
          }
        }
      }
    }
    true
  }

  override def doValidate(expr: Expression): Boolean = doValidate(Map(), expr)

  override def doValidate(plan: PlanNode): Boolean = {
    val validator = new GlutenDataNativeExpressionEvaluator()
    validator.doValidate(plan.toProtobuf.toByteArray)
  }
}
