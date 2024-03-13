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

import io.glutenproject.cbo.{CanonicalNode, CboGroup, InGroupNode, UnsafeMemoState}
import io.glutenproject.cbo.util.CycleDetector

import scala.collection.mutable

// Dynamic programming algorithm to solve problem against a single CBO group that can be
// broken down to sub problems for sub groups.
//
// Cycle exclusion is also done internally so implementations don't have to
// deal with cycle issues by themselves.
trait DpGroupAlgoDef[T <: AnyRef, GroupOutput <: AnyRef, NodeOutput <: AnyRef] {
  def solveNode(
      node: CanonicalNode[T],
      childrenGroupsOutput: Seq[Option[GroupOutput]]): Option[NodeOutput]
  def solveGroup(
      groupId: Int,
      nodesOutput: Map[CanonicalNode[T], Option[NodeOutput]]): Option[GroupOutput]
}

object DpGroupAlgo {
  def resolve[T <: AnyRef, GroupOutput <: AnyRef, NodeOutput <: AnyRef](
      memoState: UnsafeMemoState[T],
      algoDef: DpGroupAlgoDef[T, GroupOutput, NodeOutput],
      adjustment: Adjustment[T],
      groupId: Int): Solution[T, GroupOutput, NodeOutput] = {
    val algo = new DpGroupAlgoResolver(memoState, algoDef, adjustment)
    algo.resolve(groupId)
  }

  trait Adjustment[T <: AnyRef] {
    def onNodeSolved(node: CanonicalNode[T]): Unit
    def onGroupSolved(groupId: Int): Unit
    def onEmptyGroup(groupId: Int): Unit
  }

  object Adjustment {
    private class None[T <: AnyRef] extends Adjustment[T] {
      override def onNodeSolved(node: CanonicalNode[T]): Unit = {}
      override def onGroupSolved(groupId: Int): Unit = {}
      override def onEmptyGroup(groupId: Int): Unit = {}
    }
    def none[T <: AnyRef](): Adjustment[T] = new None()
  }

  private class DpGroupAlgoResolver[T <: AnyRef, GroupOutput <: AnyRef, NodeOutput <: AnyRef](
      memoState: UnsafeMemoState[T],
      algoDef: DpGroupAlgoDef[T, GroupOutput, NodeOutput],
      adjustment: Adjustment[T]) {
    private val allClusters = memoState.allClusters()
    private val allGroups = memoState.allGroups()

    def resolve(groupId: Int): Solution[T, GroupOutput, NodeOutput] = {
      val sBuilder = Solution.builder[T, GroupOutput, NodeOutput]()
      solveGroupRec(groupId, sBuilder, CycleDetector[Int]())
      sBuilder.build()
    }

    private def solveGroupRec(
        groupId: Int,
        sBuilder: Solution.Builder[T, GroupOutput, NodeOutput],
        cycleDetector: CycleDetector[Int]): Unit = {
      if (cycleDetector.contains(groupId)) {
        return
      }
      val newDetector = cycleDetector.append(groupId)
      if (sBuilder.isGroupSolved(groupId)) {
        // The same group was already solved by previous traversals before bumping into
        // this position.
        return
      }
      val group = allGroups(groupId)

      def loop(): Unit = {
        def getNodes(): Iterable[InGroupNode[T]] = {
          val nodes = group.inGroupNodes()
          if (nodes.nonEmpty) {
            return nodes
          }
          // The group is empty. Apply the adjustment.
          adjustment.onEmptyGroup(groupId)
          group.inGroupNodes()
        }

        var prevNodeCount = 0
        while (true) {
          val nodes = getNodes()
          if (nodes.size == prevNodeCount) {
            return
          }
          prevNodeCount = nodes.size
          nodes.foreach(node => solveNodeRec(node, sBuilder, newDetector))
        }
        throw new IllegalStateException("Unreachable code")
      }

      loop()

      if (sBuilder.isGroupSolved(groupId)) {
        // The same group was solved by the above recursive call on the child node.
        return
      }

      val finalNodes = group.inGroupNodes()

      val groupSolution = algoDef.solveGroup(
        groupId,
        finalNodes.map {
          inGroupNode =>
            val nodeSolution = if (sBuilder.isNodeSolved(inGroupNode)) {
              sBuilder.getNodeSolution(inGroupNode)
            } else {
              // Node was excluded by cycle.
              None
            }
            inGroupNode.can -> nodeSolution
        }.toMap
      )

      sBuilder.addGroupSolution(groupId, groupSolution)
      adjustment.onGroupSolved(groupId)
    }

    private def solveNodeRec(
        inGroupNode: InGroupNode[T],
        sBuilder: Solution.Builder[T, GroupOutput, NodeOutput],
        cycleDetector: CycleDetector[Int]): Unit = {
      if (sBuilder.isNodeSolved(inGroupNode)) {
        // The same mode was already solved by previous traversals before bumping into
        // this position.
        return
      }
      val can = inGroupNode.can
      if (can.isLeaf()) {
        sBuilder.addNodeSolution(inGroupNode, algoDef.solveNode(can, List.empty))
        adjustment.onNodeSolved(can)
        return
      }
      val childrenGroups = can.getChildrenGroups(allGroups).map(_.group(allGroups))
      assert(childrenGroups.nonEmpty)
      childrenGroups.foreach {
        childGroup =>
          val childGroupId = childGroup.id()
          solveGroupRec(childGroupId, sBuilder, cycleDetector)
      }
      val childrenGroupsOutput =
        childrenGroups.map(
          childGroup => {
            if (!sBuilder.isGroupSolved(childGroup.id())) {
              // Group was excluded by cycle, return without solving the node.
              // The subsequent solve-node calls from other groups may solve the same node
              // from other populating group.
              return
            }
            sBuilder.getGroupSolution(childGroup.id())
          })
      sBuilder.addNodeSolution(inGroupNode, algoDef.solveNode(can, childrenGroupsOutput))
      adjustment.onNodeSolved(can)
    }

  }

  trait Solution[T <: AnyRef, GroupOutput <: AnyRef, NodeOutput <: AnyRef] {
    def groups: Map[Int, GroupOutput]
    def nodes: Map[InGroupNode[T], NodeOutput]
  }

  private object Solution {
    private case class SolutionImpl[T <: AnyRef, GroupOutput <: AnyRef, NodeOutput <: AnyRef](
        override val groups: Map[Int, GroupOutput],
        override val nodes: Map[InGroupNode[T], NodeOutput])
      extends Solution[T, GroupOutput, NodeOutput]

    def builder[T <: AnyRef, GroupOutput <: AnyRef, NodeOutput <: AnyRef]()
        : Builder[T, GroupOutput, NodeOutput] = {
      Builder[T, GroupOutput, NodeOutput]()
    }

    class Builder[T <: AnyRef, GroupOutput <: AnyRef, NodeOutput <: AnyRef] private () {
      private val groups = mutable.Map[Int, Option[GroupOutput]]()
      private val nodes = mutable.Map[InGroupNode[T], Option[NodeOutput]]()

      def isGroupSolved(groupId: Int): Boolean = {
        groups.contains(groupId)
      }

      def isNodeSolved(node: InGroupNode[T]): Boolean = {
        nodes.contains(node)
      }

      def getGroupSolution(groupId: Int): Option[GroupOutput] = {
        groups(groupId)
      }

      def getNodeSolution(node: InGroupNode[T]): Option[NodeOutput] = {
        nodes(node)
      }

      def addGroupSolution(
          groupId: Int,
          groupOutput: Option[GroupOutput]): Builder[T, GroupOutput, NodeOutput] = {
        assert(!groups.contains(groupId))
        groups += groupId -> groupOutput
        this
      }

      def addNodeSolution(
          node: InGroupNode[T],
          nodeOutput: Option[NodeOutput]): Builder[T, GroupOutput, NodeOutput] = {
        assert(!nodes.contains(node))
        nodes += (node -> nodeOutput)
        this
      }

      def build(): Solution[T, GroupOutput, NodeOutput] = {
        SolutionImpl(groups.toMap.flattenValues(), nodes.toMap.flattenValues())
      }
    }

    private object Builder {
      def apply[T <: AnyRef, GroupOutput <: AnyRef, NodeOutput <: AnyRef]()
          : Builder[T, GroupOutput, NodeOutput] = {
        new Builder[T, GroupOutput, NodeOutput]()
      }
    }
  }

  implicit class OptionMapImplicits[K, V](map: Map[K, Option[V]]) {
    def flattenValues(): Map[K, V] = {
      map
        .filter {
          case (_, v) =>
            v.nonEmpty
        }
        .map {
          case (k, v) =>
            k -> v.get
        }
    }
  }

  implicit class GroupImplicits[T <: AnyRef](group: CboGroup[T]) {
    def inGroupNodes(): Iterable[InGroupNode[T]] = {
      // TODO: Why set is way faster than regular iterable / seq / list / vector ?
      group.nodes().map(n => InGroupNode(group.id(), n)).toSet
    }
  }
}
