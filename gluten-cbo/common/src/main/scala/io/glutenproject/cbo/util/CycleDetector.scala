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
package io.glutenproject.cbo.util

trait CycleDetector[T <: Any] {
  def append(obj: T): CycleDetector[T]
  def contains(obj: T): Boolean
}

object CycleDetector {
  def apply[T <: Any](): CycleDetector[T] = {
    new LinkedCycleDetector[T](null.asInstanceOf[T], null)
  }

  // Immutable, append-only linked list for detecting cycle during path finding.
  // Do not use this on case classes. The code compares elements through their
  // 'equals' method which is slow on case classes (or other user defined equals code).
  private case class LinkedCycleDetector[T <: Any](obj: T, last: LinkedCycleDetector[T])
    extends CycleDetector[T] {

    override def append(obj: T): CycleDetector[T] = {
      LinkedCycleDetector(obj, this)
    }

    override def contains(obj: T): Boolean = {
      // Backtrack the linked list to find cycle.
      assert(obj != null)
      var cursor = this
      while (cursor.obj != null) {
        if (obj == cursor.obj) {
          return true
        }
        cursor = cursor.last
      }
      false
    }
  }
}
