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

import io.glutenproject.cbo.{CanonicalNode, CboCluster, UnsafeMemoState}
import io.glutenproject.cbo.util.CycleDetector

import scala.collection.mutable

// Dynamic programming algorithm to solve problem against a single CBO cluster that can be
// broken down to sub problems for sub clusters.
//
// Cycle exclusion is also done internally so implementations don't have to
// deal with cycle issues by themselves.
trait DpClusterAlgoDef[T <: AnyRef, ClusterOutput <: AnyRef, NodeOutput <: AnyRef] {
  def solveNode(
      node: CanonicalNode[T],
      childrenClustersOutput: Seq[Option[ClusterOutput]]): Option[NodeOutput]
  def solveCluster(
      clusterId: Int,
      nodesOutput: Map[CanonicalNode[T], Option[NodeOutput]]): Option[ClusterOutput]
}

object DpClusterAlgo {
  def resolve[T <: AnyRef, ClusterOutput <: AnyRef, NodeOutput <: AnyRef](
      memoState: UnsafeMemoState[T],
      algoDef: DpClusterAlgoDef[T, ClusterOutput, NodeOutput],
      adjustment: Adjustment[T],
      clusterId: Int): Solution[T, ClusterOutput, NodeOutput] = {
    val algo = new DpClusterAlgoResolver(memoState, algoDef, adjustment)
    algo.resolve(clusterId)
  }

  trait Adjustment[T <: AnyRef] {
    def afterNodeSolved(node: CanonicalNode[T]): Unit
    def beforeClusterSolved(clusterId: Int): Unit
  }

  object Adjustment {
    private class None[T <: AnyRef] extends Adjustment[T] {
      override def afterNodeSolved(node: CanonicalNode[T]): Unit = {}
      override def beforeClusterSolved(clusterId: Int): Unit = {}
    }
    def none[T <: AnyRef](): Adjustment[T] = new None()
  }

  private class DpClusterAlgoResolver[T <: AnyRef, ClusterOutput <: AnyRef, NodeOutput <: AnyRef](
      memoState: UnsafeMemoState[T],
      algoDef: DpClusterAlgoDef[T, ClusterOutput, NodeOutput],
      adjustment: Adjustment[T]) {
    private val allClusters = memoState.allClusters()
    private val allGroups = memoState.allGroups()

    def resolve(clusterId: Int): Solution[T, ClusterOutput, NodeOutput] = {
      val sBuilder = Solution.builder[T, ClusterOutput, NodeOutput]()
      solveClusterRec(clusterId, sBuilder, CycleDetector[Int]())
      sBuilder.build()
    }

    private def solveClusterRec(
        clusterId: Int,
        sBuilder: Solution.Builder[T, ClusterOutput, NodeOutput],
        cycleDetector: CycleDetector[Int]): Unit = {
      if (cycleDetector.contains(clusterId)) {
        return
      }
      val newDetector = cycleDetector.append(clusterId)
      if (sBuilder.isClusterSolved(clusterId)) {
        // The same cluster was already solved by previous traversals before bumping into
        // this position.
        return
      }
      val cluster = allClusters(clusterId)

      def loop(): Unit = {
        var prevNodeCount = 0

        def getNodes(): Iterable[CanonicalNode[T]] = {
          val nodes = cluster.inClusterNodes()
          val nodeCount = nodes.size
          if (nodeCount >= prevNodeCount) {
            return nodes
          }
          assert(nodes.size == prevNodeCount)
          // We have no more nodes to add.
          // The cluster is going to be solved, try applying adjustment
          // to see if algo caller likes to add some nodes.
          adjustment.beforeClusterSolved(clusterId)
          cluster.inClusterNodes()
        }
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

      if (sBuilder.isClusterSolved(clusterId)) {
        // The same cluster was solved by the above recursive call on the child node.
        return
      }

      val finalNodes = cluster.inClusterNodes()

      val clusterSolution = algoDef.solveCluster(
        clusterId,
        finalNodes.map {
          can =>
            val nodeSolution = if (sBuilder.isNodeSolved(can)) {
              sBuilder.getNodeSolution(can)
            } else {
              // Node was excluded by cycle.
              None
            }
            can -> nodeSolution
        }.toMap
      )

      sBuilder.addClusterSolution(clusterId, clusterSolution)
      adjustment.beforeClusterSolved(clusterId)
    }

    private def solveNodeRec(
        can: CanonicalNode[T],
        sBuilder: Solution.Builder[T, ClusterOutput, NodeOutput],
        cycleDetector: CycleDetector[Int]): Unit = {
      if (sBuilder.isNodeSolved(can)) {
        // The same mode was already solved by previous traversals before bumping into
        // this position.
        return
      }
      if (can.isLeaf()) {
        sBuilder.addNodeSolution(can, algoDef.solveNode(can, List.empty))
        adjustment.afterNodeSolved(can)
        return
      }
      val childrenClusters =
        can.getChildrenGroups(allGroups).map(gn => allClusters(allGroups(gn.groupId()).clusterId()))
      assert(childrenClusters.nonEmpty)
      childrenClusters.foreach {
        childCluster =>
          val childClusterId = childCluster.id()
          solveClusterRec(childClusterId, sBuilder, cycleDetector)
      }
      val childrenClustersOutput =
        childrenClusters.map(
          childCluster => {
            if (!sBuilder.isClusterSolved(childCluster.id())) {
              // Cluster was excluded by cycle, return without solving the node.
              // The subsequent solve-node calls from other clusters may solve the same node
              // from other populating cluster.
              return
            }
            sBuilder.getClusterSolution(childCluster.id())
          })
      sBuilder.addNodeSolution(can, algoDef.solveNode(can, childrenClustersOutput))
      adjustment.afterNodeSolved(can)
    }

  }

  trait Solution[T <: AnyRef, ClusterOutput <: AnyRef, NodeOutput <: AnyRef] {
    def clusters: Map[Int, ClusterOutput]
    def nodes: Map[CanonicalNode[T], NodeOutput]
  }

  private object Solution {
    private case class SolutionImpl[T <: AnyRef, ClusterOutput <: AnyRef, NodeOutput <: AnyRef](
        override val clusters: Map[Int, ClusterOutput],
        override val nodes: Map[CanonicalNode[T], NodeOutput])
      extends Solution[T, ClusterOutput, NodeOutput]

    def builder[T <: AnyRef, ClusterOutput <: AnyRef, NodeOutput <: AnyRef]()
        : Builder[T, ClusterOutput, NodeOutput] = {
      Builder[T, ClusterOutput, NodeOutput]()
    }

    class Builder[T <: AnyRef, ClusterOutput <: AnyRef, NodeOutput <: AnyRef] private () {
      private val clusters = mutable.Map[Int, Option[ClusterOutput]]()
      private val nodes = mutable.Map[CanonicalNode[T], Option[NodeOutput]]()

      def isClusterSolved(clusterId: Int): Boolean = {
        clusters.contains(clusterId)
      }

      def isNodeSolved(node: CanonicalNode[T]): Boolean = {
        nodes.contains(node)
      }

      def getClusterSolution(clusterId: Int): Option[ClusterOutput] = {
        clusters(clusterId)
      }

      def getNodeSolution(node: CanonicalNode[T]): Option[NodeOutput] = {
        nodes(node)
      }

      def addClusterSolution(
          clusterId: Int,
          ClusterOutput: Option[ClusterOutput]): Builder[T, ClusterOutput, NodeOutput] = {
        assert(!clusters.contains(clusterId))
        clusters += clusterId -> ClusterOutput
        this
      }

      def addNodeSolution(
          node: CanonicalNode[T],
          nodeOutput: Option[NodeOutput]): Builder[T, ClusterOutput, NodeOutput] = {
        assert(!nodes.contains(node))
        nodes += (node -> nodeOutput)
        this
      }

      def build(): Solution[T, ClusterOutput, NodeOutput] = {
        SolutionImpl(clusters.toMap.flattenValues(), nodes.toMap.flattenValues())
      }
    }

    private object Builder {
      def apply[T <: AnyRef, ClusterOutput <: AnyRef, NodeOutput <: AnyRef]()
          : Builder[T, ClusterOutput, NodeOutput] = {
        new Builder[T, ClusterOutput, NodeOutput]()
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

  implicit class ClusterImplicits[T <: AnyRef](cluster: CboCluster[T]) {
    def inClusterNodes(): Iterable[CanonicalNode[T]] = {
      // TODO: Why set is way faster than regular iterable / seq / list / vector ?
      cluster.nodes().toSet
    }
  }
}
