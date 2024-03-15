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

import io.glutenproject.cbo.{CanonicalNode, CboGroup, InGroupNode, MemoState, UnsafeMemoState}

// Dynamic programming algorithm to solve problem against a single CBO group that can be
// broken down to sub problems for sub groups.
//
// Cycle exclusion is also done internally so implementations don't have to
// deal with cycle issues by themselves.
trait DpGroupAlgoDef[T <: AnyRef, NodeOutput <: AnyRef, GroupOutput <: AnyRef] {
  def solveNode(
      node: CanonicalNode[T],
      childrenGroupsOutput: CboGroup[T] => Option[GroupOutput]): Option[NodeOutput]
  def solveGroup(
      group: CboGroup[T],
      nodesOutput: CanonicalNode[T] => Option[NodeOutput]): Option[GroupOutput]
}

object DpGroupAlgo {

  private class ZipperAlgoDefImpl[T <: AnyRef, GroupOutput <: AnyRef, NodeOutput <: AnyRef](
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
      // TODO: Why set is way faster than regular iterable / seq / list / vector ?
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
