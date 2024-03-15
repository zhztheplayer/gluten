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
package io.glutenproject.cbo

import io.glutenproject.cbo.CboConfig.PlannerType
import io.glutenproject.cbo.dp.DpPlanner
import io.glutenproject.cbo.exaustive.ExhaustivePlanner
import io.glutenproject.cbo.path.{CboPath, PathKeySet}
import io.glutenproject.cbo.vis.GraphvizVisualizer

trait CboPlanner[T <: AnyRef] {
  def plan(): T
  def newState(): PlannerState[T]
}

object CboPlanner {
  def apply[T <: AnyRef](cbo: Cbo[T], plan: T): CboPlanner[T] = {
    apply(cbo, plan, cbo.propertySetFactory().get(plan))
  }

  def apply[T <: AnyRef](cbo: Cbo[T], plan: T, reqPropSet: PropertySet[T]): CboPlanner[T] = {
    cbo.config.plannerType match {
      case PlannerType.Exhaustive =>
        ExhaustivePlanner(cbo, plan, reqPropSet)
      case PlannerType.Dp =>
        DpPlanner(cbo, plan, reqPropSet)
    }
  }
}

trait Best[T <: AnyRef] {
  import Best._
  def rootGroupId(): Int
  def bestNodes(): Set[InGroupNode[T]]
  def winnerNodes(): Set[InGroupNode[T]]
  def costs(): InGroupNode[T] => Option[Cost]
  def path(allGroups: Int => CboGroup[T]): KnownCostPath[T]
}

object Best {
  def apply[T <: AnyRef](
      cbo: Cbo[T],
      rootGroupId: Int,
      bestNodes: Seq[InGroupNode[T]],
      winnerNodes: Seq[InGroupNode[T]],
      costs: InGroupNode[T] => Option[Cost]): Best[T] = {
    val bestNodeSet = bestNodes.toSet
    val winnerNodeSet = winnerNodes.toSet

    BestImpl(cbo, rootGroupId, bestNodeSet, winnerNodeSet, costs)
  }

  private case class BestImpl[T <: AnyRef](
      cbo: Cbo[T],
      override val rootGroupId: Int,
      override val bestNodes: Set[InGroupNode[T]],
      override val winnerNodes: Set[InGroupNode[T]],
      override val costs: InGroupNode[T] => Option[Cost])
    extends Best[T] {
    override def path(allGroups: Int => CboGroup[T]): KnownCostPath[T] = {
      val groupToBestNode: Map[Int, InGroupNode[T]] =
        bestNodes.map(ign => (ign.groupId, ign)).toMap

      def dfs(groupId: Int): CboPath[T] = {
        val can: CanonicalNode[T] = groupToBestNode(groupId).can
        if (can.isLeaf()) {
          return CboPath.one(cbo, PathKeySet.trivial, allGroups, can)
        }
        val children = can
          .getChildrenGroups(allGroups)
          .map(g => dfs(g.groupId()))
        CboPath(cbo, can, children).get
      }

      val rootCost = costs(groupToBestNode(rootGroupId))

      KnownCostPath(dfs(rootGroupId), rootCost.get)
    }
  }

  trait KnownCostPath[T <: AnyRef] {
    def cboPath: CboPath[T]
    def cost: Cost
  }

  object KnownCostPath {
    def apply[T <: AnyRef](cbo: Cbo[T], cboPath: CboPath[T]): KnownCostPath[T] = {
      KnownCostPathImpl(cboPath, cbo.costModel.costOf(cboPath.plan()))
    }

    def apply[T <: AnyRef](cboPath: CboPath[T], cost: Cost): KnownCostPath[T] = {
      KnownCostPathImpl(cboPath, cost)
    }

    private case class KnownCostPathImpl[T <: AnyRef](cboPath: CboPath[T], cost: Cost)
      extends KnownCostPath[T]
  }

  case class BestNotFoundException(message: String, cause: Exception)
    extends RuntimeException(message, cause)
  object BestNotFoundException {
    def apply(message: String): BestNotFoundException = {
      BestNotFoundException(message, null)
    }
    def apply(): BestNotFoundException = {
      BestNotFoundException(null, null)
    }
  }
}

trait PlannerState[T <: AnyRef] {
  def cbo(): Cbo[T]
  def memoState(): MemoState[T]
  def rootGroupId(): Int
  def best(): Best[T]
}

object PlannerState {
  def apply[T <: AnyRef](
      cbo: Cbo[T],
      memoState: MemoState[T],
      rootGroupId: Int,
      best: Best[T]): PlannerState[T] = {
    PlannerStateImpl(cbo, memoState, rootGroupId, best)
  }

  implicit class PlannerStateImplicits[T <: AnyRef](state: PlannerState[T]) {
    def formatGraphviz(): String = {
      formatGraphvizWithBest()
    }

    private def formatGraphvizWithBest(): String = {
      GraphvizVisualizer(state.cbo(), state.memoState(), state.best()).format()
    }
  }

  private case class PlannerStateImpl[T <: AnyRef] private (
      override val cbo: Cbo[T],
      override val memoState: MemoState[T],
      override val rootGroupId: Int,
      override val best: Best[T])
    extends PlannerState[T]
}
