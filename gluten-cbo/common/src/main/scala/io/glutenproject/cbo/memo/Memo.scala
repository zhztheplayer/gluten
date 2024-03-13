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
package io.glutenproject.cbo.memo

import io.glutenproject.cbo._
import io.glutenproject.cbo.CboCluster.ImmutableCboCluster
import io.glutenproject.cbo.util.CanonicalNodeMap
import io.glutenproject.cbo.vis.GraphvizVisualizer

trait Closure[T <: AnyRef] {
  def defineEquiv(node: CanonicalNode[T], newNode: T): Unit
}

trait Memo[T <: AnyRef] extends Closure[T] {
  def memorize(node: T, reqPropSet: PropertySet[T]): CboGroup[T]
  def newState(): MemoState[T]
  def doExhaustively(func: => Unit): Unit
}

trait UnsafeMemo[T <: AnyRef] extends Memo[T] {
  def table(): MemoTable[T]
}

object Memo {
  def apply[T <: AnyRef](cbo: Cbo[T]): Memo[T] = {
    new CboMemo[T](cbo)
  }

  def unsafe[T <: AnyRef](cbo: Cbo[T]): UnsafeMemo[T] = {
    new CboMemo[T](cbo)
  }

  private class CboMemo[T <: AnyRef](val cbo: Cbo[T]) extends UnsafeMemo[T] {
    private val memoTable: MemoTable.Writable[T] = MemoTable.create(cbo)

    private val cache: NodeToClusterMap[T] = new NodeToClusterMap(cbo)

    private def newCluster(): CboClusterKey = {
      memoTable.newCluster()
    }

    private def addToCluster(clusterKey: CboClusterKey, can: CanonicalNode[T]): Unit = {
      assert(!cache.contains(can))
      cache.put(can, clusterKey)
      memoTable.addToCluster(clusterKey, can)
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

    private def memorize0(n: T): CboClusterKey = {
      if (cbo.planModel.isGroupLeaf(n)) {
        val plainGroup = memoTable.allGroups()(cbo.planModel.getGroupId(n))
        return plainGroup.clusterKey()
      }

      val node = canonize(n)

      if (cache.contains(node)) {
        cache.get(node)
      } else {
        // Node not yet added to cluster.
        val clusterKey = newCluster()
        addToCluster(clusterKey, node)
        clusterKey
      }
    }

    override def memorize(node: T, reqPropSet: PropertySet[T]): CboGroup[T] = {
      val clusterKey = memorize0(node)
      val prevGroupCount = memoTable.allGroups().size
      val out = memoTable.groupOf(clusterKey, reqPropSet)
      val newGroupCount = memoTable.allGroups().size
      assert(newGroupCount >= prevGroupCount)
      out
    }

    override def defineEquiv(node: CanonicalNode[T], newNode: T): Unit = {
      assert(cache.contains(node))
      val preparedCluster = cache.get(node)

      val newNodeCanonical = canonize(newNode)
      if (cache.contains(newNodeCanonical)) {
        val newNodeCluster = cache.get(newNodeCanonical)
        if (newNodeCluster == preparedCluster) {
          return
        }
        // The new node already memorized to memo, but in the different cluster
        // with the input node. Merge the two clusters.
        //
        // TODO: Traversal up the tree to do more merges.
        memoTable.mergeClusters(newNodeCluster, preparedCluster)
        // Since new node already memorized, we don't have to add it to either of the clusters
        // anymore.
        return
      }
      addToCluster(preparedCluster, newNodeCanonical)
    }

    override def newState(): MemoState[T] = {
      memoTable.newState()
    }

    override def table(): MemoTable[T] = memoTable

    override def doExhaustively(func: => Unit): Unit = {
      memoTable.doExhaustively(func)
    }
  }

  private class NodeToClusterMap[T <: AnyRef](cbo: Cbo[T])
    extends CanonicalNodeMap[T, CboClusterKey](cbo)
}

trait MemoStore[T <: AnyRef] {
  def getCluster(key: CboClusterKey): CboCluster[T]
  def getGroup(id: Int): CboGroup[T]
}

object MemoStore {
  implicit class MemoStoreImplicits[T <: AnyRef](store: MemoStore[T]) {
    def asGroupSupplier(): Int => CboGroup[T] = {
      store.getGroup
    }
  }
}

trait MemoState[T <: AnyRef] extends MemoStore[T] {
  def cbo(): Cbo[T]
  def clusterLookup(): Map[CboClusterKey, CboCluster[T]]
  def allClusters(): Iterable[CboCluster[T]]
  def allGroups(): Seq[CboGroup[T]]
}

object MemoState {
  def apply[T <: AnyRef](
      cbo: Cbo[T],
      clusterLookup: Map[CboClusterKey, ImmutableCboCluster[T]],
      allGroups: Seq[CboGroup[T]]): MemoState[T] = {
    MemoStateImpl(cbo, clusterLookup, allGroups)
  }

  private case class MemoStateImpl[T <: AnyRef](
      override val cbo: Cbo[T],
      override val clusterLookup: Map[CboClusterKey, ImmutableCboCluster[T]],
      override val allGroups: Seq[CboGroup[T]])
    extends MemoState[T] {
    private val allClustersCopy = clusterLookup.values

    override def getCluster(key: CboClusterKey): CboCluster[T] = clusterLookup(key)
    override def getGroup(id: Int): CboGroup[T] = allGroups(id)
    override def allClusters(): Iterable[CboCluster[T]] = allClustersCopy
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
