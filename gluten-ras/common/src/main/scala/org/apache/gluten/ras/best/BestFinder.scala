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
package org.apache.gluten.ras.best

import org.apache.gluten.ras._
import org.apache.gluten.ras.Best.KnownCostPath
import org.apache.gluten.ras.dp.DpGroupAlgo
import org.apache.gluten.ras.memo.MemoState

import scala.collection.mutable

trait BestFinder[T <: AnyRef] {
  def bestOf(groupId: Int): Best[T]
}

object BestFinder {
  def apply[T <: AnyRef](ras: Ras[T], memoState: MemoState[T]): BestFinder[T] = {
    unsafe(ras, memoState, DpGroupAlgo.Adjustment.none())
  }

  def unsafe[T <: AnyRef](
      ras: Ras[T],
      memoState: MemoState[T],
      adjustment: DpGroupAlgo.Adjustment[T]): BestFinder[T] = {
    new GroupBasedBestFinder[T](ras, memoState, adjustment)
  }

  case class KnownCostGroup[T <: AnyRef](
      nodeToCost: java.util.IdentityHashMap[CanonicalNode[T], KnownCostPath[T]],
      bestNode: CanonicalNode[T]) {
    def best(): KnownCostPath[T] = {
      assert(nodeToCost.containsKey(bestNode))
      nodeToCost.get(bestNode)
    }
  }

  case class KnownCostCluster[T <: AnyRef](groupToCost: Map[Int, KnownCostGroup[T]])

  private[best] def newBest[T <: AnyRef](
      ras: Ras[T],
      allGroups: Seq[RasGroup[T]],
      group: RasGroup[T],
      groupToCosts: Map[Int, KnownCostGroup[T]]): Best[T] = {
    import scala.collection.JavaConverters._

    val bestPath = groupToCosts(group.id()).best()
    val bestRoot = bestPath.rasPath.node()
    val winnerNodes = groupToCosts.map { case (id, g) => InGroupNode(id, g.bestNode) }.toSeq
    val costsMap = mutable.Map[InGroupNode.HashKey, Cost]()
    groupToCosts.foreach {
      case (gid, g) =>
        val scala = new java.util.ArrayList(g.nodeToCost.entrySet()).asScala
        scala.map(entry => entry.getKey -> entry.getValue).foreach {
          case (n, c) =>
            costsMap += (InGroupNode(gid, n).toHashKey -> c.cost)
        }
    }
    Best(ras, group.id(), bestPath, winnerNodes, ign => costsMap.get(ign.toHashKey))
  }
}
