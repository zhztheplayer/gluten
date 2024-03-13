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

import scala.collection.mutable

trait CboCluster[T <: AnyRef] {
  def id(): Int
  def groups(): Iterable[CboGroup[T]]
  def nodes(): Iterable[CanonicalNode[T]]
}

trait CboGroup[T <: AnyRef] {
  def id(): Int
  def clusterId(): Int
  def propSet(): PropertySet[T]
  def self(): T
  def nodes(): Iterable[CanonicalNode[T]]
}

object CboCluster {
  import CboGroup._
  // Node cluster.
  class MutableCboCluster[T <: AnyRef](val cbo: Cbo[T], override val id: Int)
    extends CboCluster[T] {
    private val buffer: mutable.ArrayBuffer[CanonicalNode[T]] =
      mutable.ArrayBuffer()
    private val groupMap: mutable.Map[PropertySet[T], MutableCboGroup[T]] =
      mutable.Map[PropertySet[T], MutableCboGroup[T]]()

    def add(t: CanonicalNode[T]): Unit = {
      buffer += t
      val propSet = t.propSet()
      groupMap
        .foreach {
          case (groupPropSet, group) =>
            if (propSet.satisfies(groupPropSet)) {
              group.add(t)
            }
        }
    }

    def containsPropertySet(propertySet: PropertySet[T]): Boolean = {
      groupMap.contains(propertySet)
    }

    def getGroup(propertySet: PropertySet[T]): MutableCboGroup[T] = {
      assert(groupMap.contains(propertySet))
      groupMap(propertySet)
    }

    def newGroup(id: Int, propertySet: PropertySet[T]): MutableCboGroup[T] = {
      assert(!groupMap.contains(propertySet))
      val out = new MutableCboGroup(this, id, propertySet)
      groupMap += (propertySet -> out)
      buffer.foreach {
        n =>
          if (n.propSet().satisfies(propertySet)) {
            out.add(n)
          }
      }
      out
    }

    override def groups(): Iterable[CboGroup[T]] = groupMap.values

    override def nodes(): Seq[CanonicalNode[T]] = buffer
  }

  case class ImmutableCboCluster[T <: AnyRef] private (
      override val id: Int,
      override val groups: Iterable[CboGroup[T]],
      override val nodes: Iterable[CanonicalNode[T]])
    extends CboCluster[T]

  object ImmutableCboCluster {
    def apply[T <: AnyRef](cluster: CboCluster[T]): ImmutableCboCluster[T] = {
      ImmutableCboCluster(
        cluster.id(),
        cluster.groups().map(ImmutableCboGroup(_)).toVector,
        cluster.nodes().toVector)
    }
  }
}

object CboGroup {
  import CboCluster._
  class MutableCboGroup[T <: AnyRef](
      cluster: MutableCboCluster[T],
      override val id: Int,
      override val propSet: PropertySet[T])
    extends CboGroup[T] {
    private val cbo: Cbo[T] = cluster.cbo
    private val groupLeaf: T = cbo.planModel.newGroupLeaf(id, propSet)
    private val buffer: mutable.ArrayBuffer[CanonicalNode[T]] = mutable.ArrayBuffer()

    private[cbo] def add(node: CanonicalNode[T]): Unit = {
      buffer += node
    }

    override def nodes(): Seq[CanonicalNode[T]] = buffer

    override def toString(): String = s"MutableCboGroup([$id][cluster-${cluster.id}], " +
      s"$buffer, $propSet)"

    override def self(): T = groupLeaf

    override def clusterId(): Int = cluster.id
  }

  case class ImmutableCboGroup[T <: AnyRef] private (
      override val id: Int,
      override val clusterId: Int,
      override val propSet: PropertySet[T],
      override val self: T,
      override val nodes: Iterable[CanonicalNode[T]])
    extends CboGroup[T]

  object ImmutableCboGroup {
    def apply[T <: AnyRef](group: CboGroup[T]): ImmutableCboGroup[T] = {
      ImmutableCboGroup(
        group.id(),
        group.clusterId(),
        group.propSet(),
        group.self(),
        group.nodes().toVector)
    }
  }
}
