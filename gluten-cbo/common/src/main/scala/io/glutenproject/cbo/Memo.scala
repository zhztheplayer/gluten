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

import io.glutenproject.cbo.CboCluster.{ImmutableCboCluster, MutableCboCluster}
import io.glutenproject.cbo.CboGroup.{ImmutableCboGroup, MutableCboGroup}
import io.glutenproject.cbo.util.CanonicalNodeMap
import io.glutenproject.cbo.vis.GraphvizVisualizer

import scala.collection.mutable

trait Closure[T <: AnyRef] {
  def defineEquiv(node: CanonicalNode[T], newNode: T): Unit
}

trait Memo[T <: AnyRef] extends Closure[T] {
  def memorize(node: T, reqPropSet: PropertySet[T]): CboGroup[T]
  def newState(): MemoState[T]
  def getGroupCount(): Int
  def getNodeCount(): Int
}

trait UnsafeMemo[T <: AnyRef] extends Memo[T] {
  def newUnsafeState(): UnsafeMemoState[T]
}

object Memo {
  def apply[T <: AnyRef](cbo: Cbo[T]): Memo[T] = {
    new CboMemo[T](cbo)
  }

  def unsafe[T <: AnyRef](cbo: Cbo[T]): UnsafeMemo[T] = {
    new CboMemo[T](cbo)
  }

  private class CboMemo[T <: AnyRef](val cbo: Cbo[T]) extends UnsafeMemo[T] {
    private val allClusters: mutable.ArrayBuffer[MutableCboCluster[T]] = mutable.ArrayBuffer()
    private val allGroups: mutable.ArrayBuffer[MutableCboGroup[T]] = mutable.ArrayBuffer()

    private val cache: NodeToClusterMap[T] = new NodeToClusterMap(cbo)

    // Mutable members.
    private var nodeCount: Int = 0

    private def newGroup(
        cluster: MutableCboCluster[T],
        propertySet: PropertySet[T]): MutableCboGroup[T] = {
      val newGroupId = allGroups.size
      val out = cluster.newGroup(newGroupId, propertySet)
      allGroups += out
      assert(out eq allGroups(newGroupId))
      out
    }

    private def getOrCreateGroup(
        cluster: MutableCboCluster[T],
        propertySet: PropertySet[T]): MutableCboGroup[T] = {
      if (cluster.containsPropertySet(propertySet)) {
        return cluster.getGroup(propertySet)
      }
      newGroup(cluster, propertySet)
    }

    private def newCluster(): MutableCboCluster[T] = {
      val newClusterId = allClusters.size
      val out = new MutableCboCluster(cbo, newClusterId)
      allClusters += out
      assert(out eq allClusters(newClusterId))
      out
    }

    // Node is supposed to be canonical.
    private def addToCluster(cluster: MutableCboCluster[T], node: CanonicalNode[T]): Unit = {
      assert(!cache.contains(node))
      cache.put(node, cluster)
      cluster.add(node)
      nodeCount += 1
    }

    // Replace node's children with node groups. When a group doesn't exist, create it.
    private def canonizeUnsafe(node: T, depth: Int): T = {
      assert(depth >= 1)
      if (depth > 1) {
        return cbo.withNewChildren(
          node,
          cbo.planModel.childrenOf(node).map(child => canonizeUnsafe(child, depth - 1)))
      }
      assert(depth == 1)
      val childrenGroups: Seq[CboGroup[T]] = cbo.planModel
        .childrenOf(node)
        .zip(cbo.propertySetFactory().childrenRequirements(node))
        .map {
          case (child, reqPropSet) =>
            memorize(child, reqPropSet)
        }
      val newNode =
        cbo.withNewChildren(node, childrenGroups.map(group => group.self()))
      newNode
    }

    private def canonize(node: T): CanonicalNode[T] = {
      CanonicalNode(cbo, canonizeUnsafe(node, 1))
    }

    private def memorize0(n: T): MutableCboCluster[T] = {
      if (cbo.planModel.isGroupLeaf(n)) {
        val plainGroup = allGroups(cbo.planModel.getGroupId(n))
        return allClusters(plainGroup.clusterId())
      }

      val node = canonize(n)

      if (cache.contains(node)) {
        cache.get(node)
      } else {
        // Node not yet added to cluster.
        val nc = newCluster()
        addToCluster(nc, node)
        nc
      }
    }

    override def memorize(node: T, reqPropSet: PropertySet[T]): CboGroup[T] = {
      val cluster = memorize0(node)
      getOrCreateGroup(cluster, reqPropSet)
    }

    override def defineEquiv(node: CanonicalNode[T], newNode: T): Unit = {
      val newNodeCanonical = canonize(newNode)
      if (cache.contains(newNodeCanonical)) {
        return
      }
      assert(cache.contains(node))
      val cluster = cache.get(node)
      addToCluster(cluster, newNodeCanonical)
    }

    override def getGroupCount(): Int = {
      allGroups.size
    }

    override def getNodeCount(): Int = nodeCount

    override def newState(): MemoState[T] = {
      MemoState(cbo, allClusters, allGroups)
    }

    override def newUnsafeState(): UnsafeMemoState[T] = {
      MemoState.unsafe(cbo, allClusters, allGroups)
    }
  }

  private class NodeToClusterMap[T <: AnyRef](cbo: Cbo[T])
    extends CanonicalNodeMap[T, MutableCboCluster[T]](cbo)
}

// "Unsafe" indicates the members are not guaranteed immutable
// and may be modified when caller operates on memo again.
trait UnsafeMemoState[T <: AnyRef] {
  def cbo(): Cbo[T]
  def allClusters(): Seq[CboCluster[T]]
  def allGroups(): Seq[CboGroup[T]]
  def toSafe(): MemoState[T]
}

trait MemoState[T <: AnyRef] extends UnsafeMemoState[T]

object MemoState {
  def apply[T <: AnyRef](
      cbo: Cbo[T],
      allClusters: Seq[CboCluster[T]],
      allGroups: Seq[CboGroup[T]]): MemoState[T] = {
    new UnsafeMemoStateImpl(cbo, allClusters, allGroups).toSafe()
  }

  def unsafe[T <: AnyRef](
      cbo: Cbo[T],
      allClusters: Seq[MutableCboCluster[T]],
      allGroups: Seq[MutableCboGroup[T]]): UnsafeMemoState[T] = {
    new UnsafeMemoStateImpl(cbo, allClusters, allGroups)
  }

  private class UnsafeMemoStateImpl[T <: AnyRef](
      override val cbo: Cbo[T],
      override val allClusters: Seq[CboCluster[T]],
      override val allGroups: Seq[CboGroup[T]])
    extends UnsafeMemoState[T] {
    import UnsafeMemoStateImpl._
    override def toSafe(): MemoState[T] = {
      SafeVariant(cbo, allClusters, allGroups)
    }
  }

  private object UnsafeMemoStateImpl {
    private case class SafeVariant[T <: AnyRef] private (
        override val cbo: Cbo[T],
        override val allClusters: Seq[ImmutableCboCluster[T]],
        override val allGroups: Seq[ImmutableCboGroup[T]])
      extends MemoState[T] {

      override def toSafe(): MemoState[T] = this
    }

    private object SafeVariant {
      def apply[T <: AnyRef](
          cbo: Cbo[T],
          allClusters: Seq[CboCluster[T]],
          allGroups: Seq[CboGroup[T]]): MemoState[T] = {
        val immutableClusters = allClusters.map(cluster => ImmutableCboCluster(cluster)).toVector
        val immutableGroups = allGroups.map(group => ImmutableCboGroup(group)).toVector
        new SafeVariant[T](cbo, immutableClusters, immutableGroups)
      }
    }
  }

  implicit class MemoStateImplicits[T <: AnyRef](state: MemoState[T]) {
    def formatGraphvizWithBest(best: Best[T]): String = {
      GraphvizVisualizer(state.cbo(), state, best).format()
    }

    def formatGraphvizWithoutBest(rootGroupId: Int): String = {
      GraphvizVisualizer(state.cbo(), state, rootGroupId).format()
    }
  }
}
