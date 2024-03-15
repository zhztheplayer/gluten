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
package io.glutenproject.best

import io.glutenproject.cbo._
import io.glutenproject.cbo.Best.{BestNotFoundException, KnownCostPath}
import io.glutenproject.cbo.dp.{DpGroupAlgo, DpGroupAlgoDef}
import io.glutenproject.cbo.path.{CboPath, PathKeySet}

import scala.collection.mutable

trait BestFinder[T <: AnyRef] {
  def bestOf(groupId: Int): Best[T]
}

object BestFinder {
  def apply[T <: AnyRef](cbo: Cbo[T], memoState: MemoState[T]): BestFinder[T] = {
    unsafe(cbo, memoState, DpGroupAlgo.Adjustment.none())
  }

  def unsafe[T <: AnyRef](
      cbo: Cbo[T],
      memoState: UnsafeMemoState[T],
      adjustment: DpGroupAlgo.Adjustment[T]): BestFinder[T] = {
    new DpBestFinder[T](cbo, memoState, adjustment)
  }

  // The best path's each sub-path is considered optimal in its own group.
  private class DpBestFinder[T <: AnyRef](
      cbo: Cbo[T],
      memoState: UnsafeMemoState[T],
      adjustment: DpGroupAlgo.Adjustment[T])
    extends BestFinder[T] {
    import DpBestFinder._

    private val allGroups = memoState.allGroups()

    override def bestOf(groupId: Int): Best[T] = {
      val group = allGroups(groupId)
      val groupToCosts = fillBests(group)
      if (!groupToCosts.contains(group.id())) {
        throw BestNotFoundException(
          s"Best path not found. Memo state (Graphviz): \n${memoState.toSafe().formatGraphvizWithoutBest(groupId)}")

      }
      newBest(group, groupToCosts)
    }

    private def newBest(group: CboGroup[T], groupToCosts: Map[Int, KnownCostGroup[T]]): Best[T] = {
      val bestPath = groupToCosts(group.id()).best()
      val bestRoot = bestPath.cboPath.node()
      val winnerNodes = groupToCosts.map { case (id, g) => InGroupNode(id, g.bestNode) }.toSeq
      val bestNodeBuffer = mutable.ArrayBuffer[InGroupNode[T]]()
      bestNodeBuffer += InGroupNode(group.id(), bestRoot.self().asCanonical())
      def dfs(cursor: CboPath.PathNode[T]): Unit = {
        cursor
          .zipChildrenWithGroups(allGroups)
          .foreach {
            case (child, childGroup) =>
              bestNodeBuffer += InGroupNode(childGroup.id(), child.self().asCanonical())
              dfs(child)
          }
      }
      dfs(bestRoot)
      val costsMap = mutable.Map[InGroupNode[T], Cost]()
      groupToCosts.foreach {
        case (gid, g) =>
          g.nodeToCost.foreach {
            case (n, c) =>
              costsMap += (InGroupNode(gid, n) -> c.cost)
          }
      }
      Best(cbo, group.id(), bestNodeBuffer, winnerNodes, costsMap.get)
    }

    private def fillBests(group: CboGroup[T]): Map[Int, KnownCostGroup[T]] = {
      val algoDef = new AlgoDef(cbo, allGroups)
      val solution = DpGroupAlgo.resolve(memoState, algoDef, adjustment, group.id())
      solution.groups
    }
  }

  private object DpBestFinder {
    import DpGroupAlgo._

    private class AlgoDef[T <: AnyRef](cbo: Cbo[T], allGroups: Seq[CboGroup[T]])
      extends DpGroupAlgoDef[T, KnownCostGroup[T], KnownCostPath[T]] {
      private val costComparator = cbo.costModel.costComparator()

      override def solveNode(
          can: CanonicalNode[T],
          childrenGroupsOutput: Seq[Option[KnownCostGroup[T]]]): Option[KnownCostPath[T]] = {
        if (can.isLeaf()) {
          assert(childrenGroupsOutput.isEmpty)
          val path = CboPath.one(cbo, PathKeySet.trivial, allGroups, can)
          return Some(KnownCostPath(cbo, path))
        }
        assert(childrenGroupsOutput.size == can.childrenCount)
        val maybeBestChildrenPaths: Seq[Option[CboPath[T]]] = childrenGroupsOutput.map {
          maybeKcg => maybeKcg.map(kcg => kcg.best().cboPath)
        }
        if (maybeBestChildrenPaths.exists(_.isEmpty)) {
          return None
        }
        val bestChildrenPaths = maybeBestChildrenPaths.map(_.get)
        Some(KnownCostPath(cbo, path.CboPath(cbo, can, bestChildrenPaths).get))
      }

      override def solveGroup(
          groupId: Int,
          nodesOutput: Map[CanonicalNode[T], Option[KnownCostPath[T]]])
          : Option[KnownCostGroup[T]] = {
        val group = allGroups(groupId)
        assert(group.nodes().size == nodesOutput.size)
        if (nodesOutput.isEmpty) {
          return None
        }
        if (nodesOutput.values.forall(_.isEmpty)) {
          return None
        }
        val flatNodesOutput = nodesOutput.flattenValues()
        val bestPath: KnownCostPath[T] = flatNodesOutput.values.reduce {
          (left, right) =>
            Ordering
              .by((cp: KnownCostPath[T]) => cp.cost)(costComparator)
              .min(left, right)
        }
        Some(KnownCostGroup(flatNodesOutput, bestPath.cboPath.node().self().asCanonical()))
      }
    }
  }

  private case class KnownCostGroup[T <: AnyRef](
      nodeToCost: Map[CanonicalNode[T], KnownCostPath[T]],
      bestNode: CanonicalNode[T]) {
    def best(): KnownCostPath[T] = nodeToCost(bestNode)
  }
}
