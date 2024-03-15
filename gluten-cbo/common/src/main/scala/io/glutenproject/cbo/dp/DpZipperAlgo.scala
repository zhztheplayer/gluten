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

import io.glutenproject.cbo.util.CycleDetector

import scala.collection.mutable

// Dynamic programming algorithm to solve problem that can be broken down to
// sub-problems on 2 individual different element types.
//
// The elements types here are X, Y. Programming starts from Y, respectively
// traverses down to X, Y, X..., util reaching to a leaf.
//
// Cycle exclusion is also done internally so implementations don't have to
// deal with cycle issues by themselves.
trait DpZipperAlgoDef[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef] {
  // Requires all X children outputs to exist otherwise would neither call Y's #resolve with
  // these outputs nor register Y's output
  def xExistRestriction(): Boolean
  // Requires all Y children outputs to exist otherwise would neither call X's #resolve with
  // these outputs nor register X's output
  def yExistRestriction(): Boolean

  def excludeCyclesOnX(): Boolean
  def excludeCyclesOnY(): Boolean

  def idOfX(x: X): Any
  def idOfY(y: Y): Any

  def browseX(x: X): Iterable[Y]
  def browseY(y: Y): Iterable[X]

  def solveX(x: X, yOutput: Y => Option[YOutput]): Option[XOutput]
  def solveY(y: Y, xOutput: X => Option[XOutput]): Option[YOutput]
}

object DpZipperAlgo {
  def resolve[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
      algoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput],
      adjustment: Adjustment[X, Y],
      root: Y): Solution[X, Y, XOutput, YOutput] = {
    val algo = new DpZipperAlgoResolver(algoDef, adjustment)
    algo.resolve(root)
  }

  trait Adjustment[X <: AnyRef, Y <: AnyRef] {
    def beforeXSolved(x: X): Unit
    def beforeYSolved(y: Y): Unit
  }

  object Adjustment {
    private class None[X <: AnyRef, Y <: AnyRef] extends Adjustment[X, Y] {
      override def beforeXSolved(x: X): Unit = {}
      override def beforeYSolved(y: Y): Unit = {}
    }
    def none[X <: AnyRef, Y <: AnyRef](): Adjustment[X, Y] = new None()
  }

  private class DpZipperAlgoResolver[
      X <: AnyRef,
      Y <: AnyRef,
      XOutput <: AnyRef,
      YOutput <: AnyRef](
      algoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput],
      adjustment: Adjustment[X, Y]) {

    def resolve(root: Y): Solution[X, Y, XOutput, YOutput] = {
      val sBuilder = Solution.builder[X, Y, XOutput, YOutput](algoDef)
      val xCycleDetector = if (algoDef.excludeCyclesOnX()) {
        CycleDetector[X](Ordering.by(x => System.identityHashCode(algoDef.idOfX(x))))
      } else {
        CycleDetector.noop[X]()
      }
      val yCycleDetector = if (algoDef.excludeCyclesOnY()) {
        CycleDetector[Y](Ordering.by(y => System.identityHashCode(algoDef.idOfY(y))))
      } else {
        CycleDetector.noop[Y]()
      }
      solveYRec(root, sBuilder, xCycleDetector, yCycleDetector)
      sBuilder.build()
    }

    private def solveYRec(
        y: Y,
        sBuilder: Solution.Builder[X, Y, XOutput, YOutput],
        xCycleDetector: CycleDetector[X],
        yCycleDetector: CycleDetector[Y]): Unit = {
      if (yCycleDetector.contains(y)) {
        return
      }
      val newYCycleDetector = yCycleDetector.append(y)
      if (sBuilder.isYResolved(y)) {
        // The same Y was already solved by previous traversals before bumping into
        // this position.
        return
      }

      def loop(): Unit = {
        var prevXCount = 0

        def getXs(): Iterable[X] = {
          val xs = algoDef.browseY(y)
          val xCount = xs.size
          if (xCount > prevXCount) {
            return xs
          }
          assert(xCount == prevXCount)
          // We have no more Xs to add.
          // The Y is going to be solved, try applying adjustment
          // to see if algo caller likes to add some nodes.
          adjustment.beforeYSolved(y)
          algoDef.browseY(y)
        }

        while (true) {
          val xs = getXs()
          if (xs.size == prevXCount) {
            return
          }
          prevXCount = xs.size
          xs.foreach(x => solveXRec(x, sBuilder, xCycleDetector, newYCycleDetector))
        }
        throw new IllegalStateException("Unreachable code")
      }

      loop()

      if (sBuilder.isYResolved(y)) {
        // The same Y was solved by the above recursive call on the child X.
        return
      }

      val finalXs = algoDef.browseY(y)

      val xSolutions = finalXs.map {
        x =>
          val xSolution = if (sBuilder.isXResolved(x)) {
            sBuilder.getXSolution(x)
          } else {
            // X does not exist.
            if (algoDef.xExistRestriction()) {
              return
            }
            None
          }
          Solution.SolutionKey(algoDef.idOfX(x), x) -> xSolution
      }.toMap

      val ySolution =
        algoDef.solveY(y, x => xSolutions.get(Solution.SolutionKey(algoDef.idOfX(x), x)).flatten)

      sBuilder.addYSolution(y, ySolution)
    }

    private def solveXRec(
        x: X,
        sBuilder: Solution.Builder[X, Y, XOutput, YOutput],
        xCycleDetector: CycleDetector[X],
        yCycleDetector: CycleDetector[Y]): Unit = {
      if (xCycleDetector.contains(x)) {
        return
      }
      val newXCycleDetector = xCycleDetector.append(x)
      if (sBuilder.isXResolved(x)) {
        // The same X was already solved by previous traversals before bumping into
        // this position.
        return
      }

      def loop(): Unit = {
        var prevYCount = 0

        def getYs(): Iterable[Y] = {
          val ys = algoDef.browseX(x)
          val yCount = ys.size
          if (yCount > prevYCount) {
            return ys
          }
          assert(yCount == prevYCount)
          // We have no more Ys to add.
          // The Y is going to be solved, try applying adjustment
          // to see if algo caller likes to add some nodes.
          adjustment.beforeXSolved(x)
          algoDef.browseX(x)
        }

        while (true) {
          val ys = getYs()
          if (ys.size == prevYCount) {
            return
          }
          prevYCount = ys.size
          ys.foreach(y => solveYRec(y, sBuilder, newXCycleDetector, yCycleDetector))
        }
        throw new IllegalStateException("Unreachable code")
      }

      loop()

      if (sBuilder.isXResolved(x)) {
        // The same X was solved by the above recursive call on the child Y.
        return
      }

      val finalYs = algoDef.browseX(x)
      val ySolutions = finalYs.map {
        y =>
          val ySolution = if (sBuilder.isYResolved(y)) {
            sBuilder.getYSolution(y)
          } else {
            // Y does not exist.
            if (algoDef.yExistRestriction()) {
              return
            }
            None
          }
          Solution.SolutionKey(algoDef.idOfY(y), y) -> ySolution
      }.toMap

      val xSolution =
        algoDef.solveX(x, y => ySolutions.get(Solution.SolutionKey(algoDef.idOfY(y), y)).flatten)

      sBuilder.addXSolution(x, xSolution)
    }

  }
  trait Solution[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef] {
    def xSolutions: X => Option[XOutput]
    def ySolutions: Y => Option[YOutput]
  }

  private object Solution {
    private case class SolutionImpl[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
        override val xSolutions: X => Option[XOutput],
        override val ySolutions: Y => Option[YOutput])
      extends Solution[X, Y, XOutput, YOutput]

    def builder[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
        argoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput]): Builder[X, Y, XOutput, YOutput] = {
      Builder[X, Y, XOutput, YOutput](argoDef)
    }

    class Builder[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef] private (
        argoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput]) {
      private val xSolutions = mutable.Map[SolutionKey[X], Option[XOutput]]()
      private val ySolutions = mutable.Map[SolutionKey[Y], Option[YOutput]]()

      private def keyOfX(x: X): SolutionKey[X] = {
        val xid = argoDef.idOfX(x)
        SolutionKey(xid, x)
      }

      private def keyOfY(y: Y): SolutionKey[Y] = {
        val yid = argoDef.idOfY(y)
        SolutionKey(yid, y)
      }

      def isXResolved(x: X): Boolean = {
        val xKey = keyOfX(x)
        xSolutions.contains(xKey)
      }

      def isYResolved(y: Y): Boolean = {
        val yKey = keyOfY(y)
        ySolutions.contains(yKey)
      }

      def getXSolution(x: X): Option[XOutput] = {
        val xKey = keyOfX(x)
        assert(xSolutions.contains(xKey))
        xSolutions(xKey)
      }

      def getYSolution(y: Y): Option[YOutput] = {
        val yKey = keyOfY(y)
        assert(ySolutions.contains(yKey))
        ySolutions(yKey)
      }

      def addXSolution(x: X, xSolution: Option[XOutput]): Unit = {
        val xKey = keyOfX(x)
        assert(!xSolutions.contains(xKey))
        xSolutions += xKey -> xSolution
      }

      def addYSolution(y: Y, ySolution: Option[YOutput]): Unit = {
        val yKey = keyOfY(y)
        assert(!ySolutions.contains(yKey))
        ySolutions += yKey -> ySolution
      }

      def build(): Solution[X, Y, XOutput, YOutput] = {
        val xSolutionsImmutable = xSolutions.toMap
        val ySolutionsImmutable = ySolutions.toMap
        SolutionImpl(
          x => xSolutionsImmutable.get(keyOfX(x)).flatten,
          y => ySolutionsImmutable.get(keyOfY(y)).flatten
        )
      }
    }

    class SolutionKey[T <: AnyRef] private (private val id: Any, val ele: T) {
      override def hashCode(): Int = id.hashCode()
      override def equals(obj: Any): Boolean = {
        obj match {
          case other: SolutionKey[T] => id == other.id
          case _ => false
        }
      }
    }

    object SolutionKey {
      def apply[T <: AnyRef](id: Any, ele: T): SolutionKey[T] = {
        new SolutionKey[T](id, ele)
      }
    }

    private object Builder {
      def apply[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
          argoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput]): Builder[X, Y, XOutput, YOutput] = {
        new Builder[X, Y, XOutput, YOutput](argoDef)
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
  }
}
