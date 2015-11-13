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

// scalastyle:off println
package org.apache.spark.examples


import org.apache.spark._
import org.apache.spark.rdd.ShuffledRDD2

object TestPartition {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()
    val map_partition_list = List(10, 100, 1000)
    val rdd = sc.parallelize(1 to 1000).map(x => (x, x))
    val reducer_partition_list = List(1,5,10)
    for (num_mappers <- map_partition_list) {
      val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(num_mappers))
      for (num_reducers <- reducer_partition_list) {
        if (num_reducers <= num_mappers) {
          val skip = num_mappers / num_reducers
          val arr = (0 to (num_mappers - 1) by skip).toArray

          var totalTime : Long = 0
          var samples : Long = 0
          for (i <- 1 to 100) {
            var startTime: Long = 0
            if (i >= 90) {
              startTime = System.nanoTime()
            }
            val rdd2 = new org.apache.spark.rdd.ShuffledRDD2(dep, arr)
            rdd2.glom.collect
            if (i >= 90) {
              val estimatedTime = System.nanoTime() - startTime
              samples += 1
              totalTime += estimatedTime
            }
          }

          println("mappers: ", num_mappers, "     reducers: ", num_reducers, "     avg time: ", totalTime.toFloat / samples)
        }
      }
    }

  }
}
