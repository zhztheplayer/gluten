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

import io.glutenproject.cbo.{CboCluster, InClusterNode, UnsafeMemoState}
import io.glutenproject.cbo.dp.DpZipperAlgo.Solution

// Dynamic programming algorithm to solve problem against a single CBO cluster that can be
// broken down to sub problems for sub clusters.
//
// FIXME: Code is so similar with DpGroupAlgo.
trait DpClusterAlgoDef[T <: AnyRef, NodeOutput <: AnyRef, ClusterOutput <: AnyRef] {
  def solveNode(
      node: InClusterNode[T],
      childrenClustersOutput: CboCluster[T] => Option[ClusterOutput]): Option[NodeOutput]
  def solveCluster(
      group: CboCluster[T],
      nodesOutput: InClusterNode[T] => Option[NodeOutput]): Option[ClusterOutput]
}

object DpClusterAlgo {

  trait Adjustment[T <: AnyRef] extends DpZipperAlgo.Adjustment[InClusterNode[T], CboCluster[T]]

  object Adjustment {
    private class None[T <: AnyRef] extends Adjustment[T] {
      override def beforeXSolved(
          panel: DpZipperAlgo.Adjustment.Panel[InClusterNode[T], CboCluster[T]],
          x: InClusterNode[T]): Unit = {}

      override def beforeYSolved(
          panel: DpZipperAlgo.Adjustment.Panel[InClusterNode[T], CboCluster[T]],
          y: CboCluster[T]): Unit = {}
    }

    def none[T <: AnyRef](): Adjustment[T] = new None[T]()
  }

  def resolve[T <: AnyRef, NodeOutput <: AnyRef, ClusterOutput <: AnyRef](
      memoState: UnsafeMemoState[T],
      groupAlgoDef: DpClusterAlgoDef[T, NodeOutput, ClusterOutput],
      conf: DpZipperAlgo.Conf,
      adjustment: Adjustment[T],
      cluster: CboCluster[T])
      : Solution[InClusterNode[T], CboCluster[T], NodeOutput, ClusterOutput] = {
    DpZipperAlgo.resolve(new ZipperAlgoDefImpl(memoState, groupAlgoDef), conf, adjustment, cluster)
  }

  private class ZipperAlgoDefImpl[T <: AnyRef, NodeOutput <: AnyRef, ClusterOutput <: AnyRef](
      memoState: UnsafeMemoState[T],
      clusterAlgoDef: DpClusterAlgoDef[T, NodeOutput, ClusterOutput])
    extends DpZipperAlgoDef[InClusterNode[T], CboCluster[T], NodeOutput, ClusterOutput] {
    override def idOfX(x: InClusterNode[T]): Any = {
      x
    }

    override def idOfY(y: CboCluster[T]): Any = {
      y.id()
    }

    override def browseX(x: InClusterNode[T]): Iterable[CboCluster[T]] = {
      val allGroups = memoState.allGroups()
      val allClusters = memoState.allClusters()
      x.can
        .getChildrenGroups(allGroups)
        .map(gn => allGroups(gn.groupId()).clusterId())
        .map(cid => allClusters(cid))
    }

    override def browseY(y: CboCluster[T]): Iterable[InClusterNode[T]] = {
      // TODO: Why set is way faster at here than regular iterable / seq / list / vector ?
      y.nodes().map(n => InClusterNode(y.id(), n)).toSet
    }

    override def solveX(
        x: InClusterNode[T],
        yOutput: CboCluster[T] => Option[ClusterOutput]): Option[NodeOutput] = {
      clusterAlgoDef.solveNode(x, yOutput)
    }

    override def solveY(
        y: CboCluster[T],
        xOutput: InClusterNode[T] => Option[NodeOutput]): Option[ClusterOutput] = {
      clusterAlgoDef.solveCluster(y, xOutput)
    }
  }
}
