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

import io.glutenproject.best.BestFinder.KnownCostGroup
import io.glutenproject.cbo._
import io.glutenproject.cbo.Best.{BestNotFoundException, KnownCostPath}
import io.glutenproject.cbo.dp.{DpGroupAlgo, DpGroupAlgoDef}
import io.glutenproject.cbo.path.{CboPath, PathKeySet}

import scala.collection.mutable

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
    val solution = DpGroupAlgo.resolve(memoState, algoDef, adjustment, group)
    val ySolutions: CboGroup[T] => Option[KnownCostGroup[T]] = solution.ySolutions
    val bests = allGroups.flatMap(group => ySolutions(group).map(kcg => group.id() -> kcg)).toMap
    bests
  }
}

private object DpBestFinder {

  private class AlgoDef[T <: AnyRef](cbo: Cbo[T], allGroups: Seq[CboGroup[T]])
    extends DpGroupAlgoDef[T, KnownCostPath[T], KnownCostGroup[T]] {
    private val costComparator = cbo.costModel.costComparator()

    override def solveNode(
        can: CanonicalNode[T],
        childrenGroupsOutput: CboGroup[T] => Option[KnownCostGroup[T]])
        : Option[KnownCostPath[T]] = {
      if (can.isLeaf()) {
        val path = CboPath.one(cbo, PathKeySet.trivial, allGroups, can)
        return Some(KnownCostPath(cbo, path))
      }
      val childrenGroups = can.getChildrenGroups(allGroups).map(gn => allGroups(gn.groupId()))
      val maybeBestChildrenPaths: Seq[Option[CboPath[T]]] = childrenGroups.map {
        childGroup => childrenGroupsOutput(childGroup).map(kcg => kcg.best().cboPath)
      }
      if (maybeBestChildrenPaths.exists(_.isEmpty)) {
        return None
      }
      val bestChildrenPaths = maybeBestChildrenPaths.map(_.get)
      Some(KnownCostPath(cbo, path.CboPath(cbo, can, bestChildrenPaths).get))
    }

    override def solveGroup(
        group: CboGroup[T],
        nodesOutput: CanonicalNode[T] => Option[KnownCostPath[T]]): Option[KnownCostGroup[T]] = {
      val nodes = group.nodes()
      val flatNodesOutput = nodes.flatMap(n => nodesOutput(n).map(kcp => n -> kcp)).toMap
      if (flatNodesOutput.isEmpty) {
        return None
      }
      val bestPath = flatNodesOutput.values.reduce {
        (left, right) =>
          Ordering
            .by((cp: KnownCostPath[T]) => cp.cost)(costComparator)
            .min(left, right)
      }
      Some(KnownCostGroup(flatNodesOutput, bestPath.cboPath.node().self().asCanonical()))
    }
  }
}
