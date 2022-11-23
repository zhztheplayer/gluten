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

package io.glutenproject.expression

import io.glutenproject.substrait.expression.ExpressionNode
import io.glutenproject.substrait.expression.ExpressionBuilder

import com.google.common.collect.Lists
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._

class RoundExpression(child: Expression, scale: Expression, original: Expression)
  extends Round(child: Expression, scale: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    val scaleNode = scale.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode] || !scaleNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.ROUND, Seq(child.dataType, scale.dataType),
      ConverterUtils.FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode],
      scaleNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class BRoundExpression(child: Expression, scale: Expression, original: Expression)
  extends BRound(child: Expression, scale: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    val scaleNode = scale.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode] || !scaleNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.BROUND, Seq(child.dataType, scale.dataType),
      ConverterUtils.FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode],
      scaleNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

object RoundOperatorTransformer {

  def create(child: Expression, scale: Expression, original: Expression): Expression =
    original match {
      case r: Round =>
        new RoundExpression(child, scale, original)
      case b: BRound =>
        new BRoundExpression(child, scale, original)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
}
