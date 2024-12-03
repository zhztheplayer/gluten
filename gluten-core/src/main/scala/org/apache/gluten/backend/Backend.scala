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
package org.apache.gluten.backend

import java.util.ServiceLoader
import scala.collection.JavaConverters

trait Backend extends Component {
}

object Backend {
  private val backend: Backend = {
    val discoveredBackends =
      JavaConverters.iterableAsScalaIterable(ServiceLoader.load(classOf[Backend])).toList
    discoveredBackends match {
      case Nil =>
        throw new IllegalStateException("Backend implementation not discovered from JVM classpath")
      case head :: Nil =>
        head
      case backends =>
        val backendNames = backends.map(_.name())
        throw new IllegalStateException(
          s"More than one Backend implementation discovered from JVM classpath: $backendNames")
    }
  }

  def get(): Backend = {
    backend
  }
}
