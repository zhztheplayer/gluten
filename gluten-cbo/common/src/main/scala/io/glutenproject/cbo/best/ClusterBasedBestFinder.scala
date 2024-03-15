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

package io.glutenproject.cbo.best

import io.glutenproject.cbo.{Best, CanonicalNode, Cbo, CboCluster, CboGroup, UnsafeMemoState}
import io.glutenproject.cbo.Best.{BestNotFoundException, KnownCostPath}
import io.glutenproject.cbo.best.BestFinder.{KnownCostCluster, KnownCostGroup}
import io.glutenproject.cbo.dp.{DpClusterAlgo, DpClusterAlgoDef}

private class ClusterBasedBestFinder[T <: AnyRef](
    cbo: Cbo[T],
    memoState: UnsafeMemoState[T],
    adjustment: DpClusterAlgo.Adjustment[T])
  extends BestFinder[T] {
  import ClusterBasedBestFinder._

  private val allClusters = memoState.allClusters()
  private val allGroups = memoState.allGroups()

  override def bestOf(groupId: Int): Best[T] = {
    val group = allGroups(groupId)
    val cluster = allClusters(group.clusterId())
    val groupToCosts = fillBests(cluster)
    if (!groupToCosts.contains(groupId)) {
      throw BestNotFoundException(
        s"Best path not found. Memo state (Graphviz): \n${memoState.toSafe().formatGraphvizWithoutBest(groupId)}")
    }
    BestFinder.newBest(cbo, allGroups, group, groupToCosts)
  }

  private def fillBests(cluster: CboCluster[T]): Map[Int, KnownCostGroup[T]] = {
    val algoDef = new AlgoDef(cbo, memoState)
    val solution = DpClusterAlgo.resolve(memoState, algoDef, adjustment, cluster)
    val ySolutions: CboCluster[T] => Option[KnownCostCluster[T]] = solution.ySolutions
    val bests = allGroups
      .flatMap(
        group =>
          ySolutions(allClusters(group.clusterId()))
            .map(kcc => kcc.groupToCost(group.id()))
            .map(kcg => group.id() -> kcg))
      .toMap
    bests
  }
}

private object ClusterBasedBestFinder {
  private class AlgoDef[T <: AnyRef](cbo: Cbo[T], memoState: UnsafeMemoState[T])
    extends DpClusterAlgoDef[T, KnownCostPath[T], KnownCostCluster[T]] {

    private val allClusters = memoState.allClusters()
    private val allGroups = memoState.allGroups()

    override def solveNode(
        node: CanonicalNode[T],
        childrenClustersOutput: CboCluster[T] => Option[KnownCostCluster[T]])
        : Option[KnownCostPath[T]] = ???

    override def solveCluster(
        cluster: CboCluster[T],
        nodesOutput: CanonicalNode[T] => Option[KnownCostPath[T]]): Option[KnownCostCluster[T]] =
      ???
  }
}
