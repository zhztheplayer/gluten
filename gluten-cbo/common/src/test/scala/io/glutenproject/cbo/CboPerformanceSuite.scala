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

import io.glutenproject.cbo.path.CboPath
import io.glutenproject.cbo.rule.{CboRule, Shape, Shapes}

import org.scalatest.funsuite.AnyFunSuite

class CboPerformanceSuite extends AnyFunSuite {
  import CboPerformanceSuite._

  test(s"Rule invocation count - depth 2, 1") {
    val l2l2 = new LeafToLeaf2()
    val u2u2 = new UnaryToUnary2()

    object Unary2Unary2ToUnary3 extends CboRule[TestNode] {
      var invocationCount: Int = 0
      var effectiveInvocationCount: Int = 0
      override def shift(node: TestNode): Iterable[TestNode] = {
        invocationCount += 1
        node match {
          case Unary2(cost1, Unary2(cost2, child)) =>
            effectiveInvocationCount += 1
            List(Unary3(cost1 + cost2 - 1, child))
          case other => List.empty
        }
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(2)
    }

    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(List(l2l2, u2u2, Unary2Unary2ToUnary3)))
    val plan = Unary(50, Unary2(50, Unary2(50, Unary2(50, Leaf(30)))))
    val planner = cbo.newPlanner(plan)
    val optimized = planner.plan()

    assert(Unary2Unary2ToUnary3.invocationCount == 14)
    assert(Unary2Unary2ToUnary3.effectiveInvocationCount == 3)
    assert(optimized == Unary3(98, Unary3(99, Leaf2(29))))
  }

  test("Rule invocation count - depth 2, 2") {
    val l2l2 = new LeafToLeaf2()
    val u2u2 = new UnaryToUnary2()
    val u2u22u3 = new Unary2Unary2ToUnary3()

    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(List(l2l2, u2u2, u2u22u3)))
    val plan = Unary(50, Unary2(50, Unary2(50, Unary2(50, Leaf(30)))))
    val planner = cbo.newPlanner(plan)
    val optimized = planner.plan()

    assert(l2l2.invocationCount == 10)
    assert(l2l2.effectiveInvocationCount == 1)
    assert(u2u2.invocationCount == 10)
    assert(u2u2.effectiveInvocationCount == 1)
    assert(u2u22u3.invocationCount == 14)
    assert(u2u22u3.effectiveInvocationCount == 3)
    assert(optimized == Unary3(98, Unary3(99, Leaf2(29))))
  }

  test("Plan manipulation count - depth 2") {
    val l2l2 = new LeafToLeaf2()
    val u2u2 = new UnaryToUnary2()
    val u2u22u3 = new Unary2Unary2ToUnary3()

    val planModel = new PlanModelWithStats(PlanModelImpl)

    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        planModel,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(List(l2l2, u2u2, u2u22u3)))
    val plan = Unary(50, Unary2(50, Unary2(50, Unary2(50, Leaf(30)))))
    val planner = cbo.newPlanner(plan)
    val optimized = planner.plan()
    assert(optimized == Unary3(98, Unary3(99, Leaf2(29))))

    assert(
      (
        planModel.childrenOfCount,
        planModel.withNewChildrenCount,
        planModel.hashCodeCount,
        planModel.equalsCount) == (583, 163, 165, 85)
    ) // TODO reduce this for performance

    val state = planner.newState()
    val allPaths = state.memoState().collectAllPaths(CboPath.INF_DEPTH).toSeq
    val distinctPathCount = allPaths.distinct.size
    val pathCount = allPaths.size
    assert(distinctPathCount == pathCount)
    assert(pathCount == 58)
  }

  test("Plan manipulation count - depth 5") {
    val rule = new CboRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case Unary(c1, Unary(c2, Unary(c3, Unary(c4, Unary(c5, child))))) =>
          List(Unary2(c1, Unary2(c2, Unary2(c3 - 6, Unary2(c4, Unary2(c5, child))))))
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(5)
    }

    val planModel = new PlanModelWithStats(PlanModelImpl)

    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        planModel,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(List(new UnaryToUnary2(), new LeafToLeaf2(), rule)))
    val plan = Unary(
      50,
      Unary(
        50,
        Unary(
          50,
          Unary(50, Unary(50, Unary(50, Unary(50, Unary(50, Unary(50, Unary(50, Leaf(30)))))))))))
    val planner = cbo.newPlanner(plan)
    val optimized = planner.plan()
    assert(
      optimized == Unary2(
        50,
        Unary2(
          50,
          Unary2(
            44,
            Unary2(
              50,
              Unary2(50, Unary2(50, Unary2(50, Unary2(44, Unary2(50, Unary2(50, Leaf2(29))))))))))))

    assert(
      (
        planModel.childrenOfCount,
        planModel.withNewChildrenCount,
        planModel.hashCodeCount,
        planModel.equalsCount) == (8927, 3060, 2306, 955)
    ) // TODO reduce this for performance

    val state = planner.newState()
    val allPaths = state.memoState().collectAllPaths(CboPath.INF_DEPTH).toSeq
    val distinctPathCount = allPaths.distinct.size
    val pathCount = allPaths.size
    assert(distinctPathCount == pathCount)
    assert(pathCount == 10865)
  }

  test("Cost evaluation count - base") {
    val costModel = new CostModelWithStats(CostModelImpl)

    val cbo =
      Cbo[TestNode](
        costModel,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(List(new UnaryToUnary2, new Unary2ToUnary3)))
    val plan = Unary(
      50,
      Unary(
        50,
        Unary(
          50,
          Unary(50, Unary(50, Unary(50, Unary(50, Unary(50, Unary(50, Unary(50, Leaf(30)))))))))))
    val planner = cbo.newPlanner(plan)
    val optimized = planner.plan()
    assert(
      optimized == Unary3(
        48,
        Unary3(
          48,
          Unary3(
            48,
            Unary3(
              48,
              Unary3(48, Unary3(48, Unary3(48, Unary3(48, Unary3(48, Unary3(48, Leaf(30))))))))))))
    assert(costModel.costOfCount == 32) // TODO reduce this for performance
    assert(costModel.costCompareCount == 20) // TODO reduce this for performance
  }

  test("Cost evaluation count - max cost") {
    val costModelPruned = new CostModel[TestNode] {

      override def costOf(node: TestNode): Cost = {
        node match {
          case ll: LeafLike =>
            CostModelImpl.costOf(ll)
          case ul: UnaryLike if ul.child.isInstanceOf[LeafLike] =>
            CostModelImpl.costOf(ul)
          case u @ Unary(_, Unary(_, _)) =>
            CostModelImpl.costOf(u)
          case u @ Unary2(_, Unary2(_, _)) =>
            CostModelImpl.costOf(u)
          case u @ Unary3(_, Unary3(_, _)) =>
            CostModelImpl.costOf(u)
          case _ =>
            // By returning a maximum cost, patterns other than the above accepted patterns
            // should be pruned.
            LongCost(Long.MaxValue)
        }
      }

      override def costComparator(): Ordering[Cost] = {
        CostModelImpl.costComparator()
      }

      override def makeInfCost(): Cost = CostModelImpl.makeInfCost()
    }

    val costModel = new CostModelWithStats(costModelPruned)

    val cbo =
      Cbo[TestNode](
        costModel,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(List(new UnaryToUnary2, new Unary2ToUnary3)))
    val plan = Unary(
      50,
      Unary(
        50,
        Unary(
          50,
          Unary(50, Unary(50, Unary(50, Unary(50, Unary(50, Unary(50, Unary(50, Leaf(30)))))))))))
    val planner = cbo.newPlanner(plan)
    val optimized = planner.plan()
    assert(
      optimized == Unary3(
        48,
        Unary3(
          48,
          Unary3(
            48,
            Unary3(
              48,
              Unary3(48, Unary3(48, Unary3(48, Unary3(48, Unary3(48, Unary3(48, Leaf(30))))))))))))
    assert(costModel.costOfCount == 32) // TODO reduce this for performance
    assert(costModel.costCompareCount == 20) // TODO reduce this for performance
  }
}

object CboPerformanceSuite extends CboSuiteBase {

  case class Unary(override val selfCost: Long, override val child: TestNode) extends UnaryLike {
    override def withNewChildren(child: TestNode): UnaryLike = copy(child = child)
  }

  case class Unary2(override val selfCost: Long, override val child: TestNode) extends UnaryLike {
    override def withNewChildren(child: TestNode): UnaryLike = copy(child = child)
  }

  case class Unary3(override val selfCost: Long, override val child: TestNode) extends UnaryLike {
    override def withNewChildren(child: TestNode): UnaryLike = copy(child = child)
  }

  case class Leaf(override val selfCost: Long) extends LeafLike {
    override def makeCopy(): LeafLike = copy()
  }

  case class Leaf2(override val selfCost: Long) extends LeafLike {
    override def makeCopy(): LeafLike = copy()
  }

  class LeafToLeaf2 extends CboRule[TestNode] {
    var invocationCount: Int = 0
    var effectiveInvocationCount: Int = 0

    override def shift(node: TestNode): Iterable[TestNode] = {
      invocationCount += 1
      node match {
        case Leaf(cost) =>
          effectiveInvocationCount += 1
          List(Leaf2(cost - 1))
        case other => List.empty
      }
    }

    override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
  }

  class UnaryToUnary2 extends CboRule[TestNode] {
    var invocationCount: Int = 0
    var effectiveInvocationCount: Int = 0

    override def shift(node: TestNode): Iterable[TestNode] = {
      invocationCount += 1
      node match {
        case Unary(cost, child) =>
          effectiveInvocationCount += 1
          List(Unary2(cost - 1, child))
        case other => List.empty
      }
    }

    override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
  }

  class Unary2ToUnary3 extends CboRule[TestNode] {
    var invocationCount: Int = 0
    var effectiveInvocationCount: Int = 0

    override def shift(node: TestNode): Iterable[TestNode] = {
      invocationCount += 1
      node match {
        case Unary2(cost, child) =>
          effectiveInvocationCount += 1
          List(Unary3(cost - 1, child))
        case other => List.empty
      }
    }

    override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
  }

  class Unary2Unary2ToUnary3 extends CboRule[TestNode] {
    var invocationCount: Int = 0
    var effectiveInvocationCount: Int = 0

    override def shift(node: TestNode): Iterable[TestNode] = {
      invocationCount += 1
      node match {
        case Unary2(cost1, Unary2(cost2, child)) =>
          effectiveInvocationCount += 1
          List(Unary3(cost1 + cost2 - 1, child))
        case other => List.empty
      }
    }

    override def shape(): Shape[TestNode] = Shapes.fixedHeight(2)
  }

  class PlanModelWithStats[T <: AnyRef](delegated: PlanModel[T]) extends PlanModel[T] {
    var childrenOfCount = 0
    var withNewChildrenCount = 0
    var hashCodeCount = 0
    var equalsCount = 0
    var newGroupLeafCount = 0
    var isGroupLeafCount = 0
    var getGroupIdCount = 0

    override def childrenOf(node: T): Seq[T] = {
      childrenOfCount += 1
      delegated.childrenOf(node)
    }
    override def withNewChildren(node: T, children: Seq[T]): T = {
      withNewChildrenCount += 1
      delegated.withNewChildren(node, children)
    }
    override def hashCode(node: T): Int = {
      hashCodeCount += 1
      delegated.hashCode(node)
    }
    override def equals(one: T, other: T): Boolean = {
      equalsCount += 1
      delegated.equals(one, other)
    }
    override def newGroupLeaf(groupId: Int, propSet: PropertySet[T]): T = {
      newGroupLeafCount += 1
      delegated.newGroupLeaf(groupId, propSet)
    }
    override def isGroupLeaf(node: T): Boolean = {
      isGroupLeafCount += 1
      delegated.isGroupLeaf(node)
    }
    override def getGroupId(node: T): Int = {
      getGroupIdCount += 1
      delegated.getGroupId(node)
    }
  }

  class CostModelWithStats[T <: AnyRef](delegated: CostModel[T]) extends CostModel[T] {
    var costOfCount = 0
    var costCompareCount = 0

    override def costOf(node: T): Cost = {
      costOfCount += 1
      delegated.costOf(node)
    }
    override def costComparator(): Ordering[Cost] = {
      new Ordering[Cost] {
        override def compare(x: Cost, y: Cost): Int = {
          costCompareCount += 1
          delegated.costComparator().compare(x, y)
        }
      }
    }

    override def makeInfCost(): Cost = delegated.makeInfCost()
  }
}
