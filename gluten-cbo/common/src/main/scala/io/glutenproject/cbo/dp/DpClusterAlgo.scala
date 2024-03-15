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
import io.glutenproject.cbo.dp.DpZipperAlgo.Solution

// Dynamic programming algorithm to solve problem against a single CBO cluster that can be
// broken down to sub problems for sub clusters.
//
// FIXME: Code is so similar with DpGroupAlgo.
trait DpClusterAlgoDef[T <: AnyRef, NodeOutput <: AnyRef, ClusterOutput <: AnyRef] {
  def solveNode(
      node: CanonicalNode[T],
      childrenClustersOutput: CboCluster[T] => Option[ClusterOutput]): Option[NodeOutput]
  def solveCluster(
      group: CboCluster[T],
      nodesOutput: CanonicalNode[T] => Option[NodeOutput]): Option[ClusterOutput]
}

object DpClusterAlgo {

  trait Adjustment[T <: AnyRef] extends DpZipperAlgo.Adjustment[CanonicalNode[T], CboCluster[T]]

  object Adjustment {
    private class None[T <: AnyRef] extends Adjustment[T] {
      override def beforeXSolved(x: CanonicalNode[T]): Unit = {}
      override def beforeYSolved(y: CboCluster[T]): Unit = {}
    }

    def none[T <: AnyRef](): Adjustment[T] = new None[T]()
  }

  def resolve[T <: AnyRef, NodeOutput <: AnyRef, ClusterOutput <: AnyRef](
      memoState: UnsafeMemoState[T],
      groupAlgoDef: DpClusterAlgoDef[T, NodeOutput, ClusterOutput],
      adjustment: Adjustment[T],
      cluster: CboCluster[T])
      : Solution[CanonicalNode[T], CboCluster[T], NodeOutput, ClusterOutput] = {
    DpZipperAlgo.resolve(new ZipperAlgoDefImpl(memoState, groupAlgoDef), adjustment, cluster)
  }

  private class ZipperAlgoDefImpl[T <: AnyRef, NodeOutput <: AnyRef, ClusterOutput <: AnyRef](
      memoState: UnsafeMemoState[T],
      clusterAlgoDef: DpClusterAlgoDef[T, NodeOutput, ClusterOutput])
    extends DpZipperAlgoDef[CanonicalNode[T], CboCluster[T], NodeOutput, ClusterOutput] {
    override def idOfX(x: CanonicalNode[T]): Any = {
      x
    }

    override def idOfY(y: CboCluster[T]): Any = {
      y.id()
    }

    override def browseX(x: CanonicalNode[T]): Iterable[CboCluster[T]] = {
      val allGroups = memoState.allGroups()
      val allClusters = memoState.allClusters()
      x.getChildrenGroups(allGroups)
        .map(gn => allGroups(gn.groupId()).clusterId())
        .map(cid => allClusters(cid))
    }

    override def browseY(y: CboCluster[T]): Iterable[CanonicalNode[T]] = {
      // TODO: Why set is way faster at here than regular iterable / seq / list / vector ?
      y.nodes().toSet
    }

    override def solveX(
        x: CanonicalNode[T],
        yOutput: CboCluster[T] => Option[ClusterOutput]): Option[NodeOutput] = {
      clusterAlgoDef.solveNode(x, yOutput)
    }

    override def solveY(
        y: CboCluster[T],
        xOutput: CanonicalNode[T] => Option[NodeOutput]): Option[ClusterOutput] = {
      clusterAlgoDef.solveCluster(y, xOutput)
    }

    override def xExistRestriction(): Boolean = {
      true
    }

    override def yExistRestriction(): Boolean = {
      true
    }

    override def excludeCyclesOnX(): Boolean = {
      false
    }

    override def excludeCyclesOnY(): Boolean = {
      // Do cycle exclusion on Groups.
      true
    }
  }
}
