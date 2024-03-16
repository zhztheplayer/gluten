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

package io.glutenproject.cbo.dp

import io.glutenproject.cbo.{CanonicalNode, CboGroup, UnsafeMemoState}
import io.glutenproject.cbo.dp.DpZipperAlgo.Solution

// Dynamic programming algorithm to solve problem against a single CBO group that can be
// broken down to sub problems for sub groups.
trait DpGroupAlgoDef[T <: AnyRef, NodeOutput <: AnyRef, GroupOutput <: AnyRef] {
  def solveNode(
      node: CanonicalNode[T],
      childrenGroupsOutput: CboGroup[T] => Option[GroupOutput]): Option[NodeOutput]
  def solveGroup(
      group: CboGroup[T],
      nodesOutput: CanonicalNode[T] => Option[NodeOutput]): Option[GroupOutput]
}

object DpGroupAlgo {

  trait Adjustment[T <: AnyRef] extends DpZipperAlgo.Adjustment[CanonicalNode[T], CboGroup[T]]

  object Adjustment {
    private class None[T <: AnyRef] extends Adjustment[T] {
      override def beforeXSolved(
          panel: DpZipperAlgo.Adjustment.Panel[CanonicalNode[T], CboGroup[T]],
          x: CanonicalNode[T]): Unit = {}

      override def beforeYSolved(
          panel: DpZipperAlgo.Adjustment.Panel[CanonicalNode[T], CboGroup[T]],
          y: CboGroup[T]): Unit = {}
    }

    def none[T <: AnyRef](): Adjustment[T] = new None[T]()
  }

  def resolve[T <: AnyRef, NodeOutput <: AnyRef, GroupOutput <: AnyRef](
      memoState: UnsafeMemoState[T],
      groupAlgoDef: DpGroupAlgoDef[T, NodeOutput, GroupOutput],
      conf: DpZipperAlgo.Conf,
      adjustment: Adjustment[T],
      group: CboGroup[T]): Solution[CanonicalNode[T], CboGroup[T], NodeOutput, GroupOutput] = {
    DpZipperAlgo.resolve(new ZipperAlgoDefImpl(memoState, groupAlgoDef), conf, adjustment, group)
  }

  private class ZipperAlgoDefImpl[T <: AnyRef, NodeOutput <: AnyRef, GroupOutput <: AnyRef](
      memoState: UnsafeMemoState[T],
      groupAlgoDef: DpGroupAlgoDef[T, NodeOutput, GroupOutput])
    extends DpZipperAlgoDef[CanonicalNode[T], CboGroup[T], NodeOutput, GroupOutput] {
    override def idOfX(x: CanonicalNode[T]): Any = {
      x
    }

    override def idOfY(y: CboGroup[T]): Any = {
      y.id()
    }

    override def browseX(x: CanonicalNode[T]): Iterable[CboGroup[T]] = {
      val allGroups = memoState.allGroups()
      x.getChildrenGroups(allGroups).map(gn => allGroups(gn.groupId()))
    }

    override def browseY(y: CboGroup[T]): Iterable[CanonicalNode[T]] = {
      // TODO: Why set is way faster at here than regular iterable / seq / list / vector ?
      y.nodes().toSet
    }

    override def solveX(
        x: CanonicalNode[T],
        yOutput: CboGroup[T] => Option[GroupOutput]): Option[NodeOutput] = {
      groupAlgoDef.solveNode(x, yOutput)
    }

    override def solveY(
        y: CboGroup[T],
        xOutput: CanonicalNode[T] => Option[NodeOutput]): Option[GroupOutput] = {
      groupAlgoDef.solveGroup(y, xOutput)
    }
  }
}
