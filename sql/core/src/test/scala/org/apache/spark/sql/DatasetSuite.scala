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

package org.apache.spark.sql

import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.functions._

class DatasetSuite extends QueryTest with SQLTestUtils {
  import org.apache.spark.sql.TestData._

  lazy val ctx = org.apache.spark.sql.test.TestSQLContext
  import ctx.implicits._

  def sqlContext: SQLContext = ctx

  test("basic operations") {
    val ds = new Dataset[TestData](testData)
    println("ds:")
    ds.toDF.show()

    val filtered = ds.filter(_.key > 95)
    println("filtered:")
    filtered.toDF.show()

    val mapped1 = ds.map(x => TestData2(x.key, x.key + 1))
    println("mapped1:")
    mapped1.toDF.show()

    val mapped2 = ds.map(x => x.key)
    println("mapped2:")
    mapped2.toDF.show()

    val grouped1 = ds.groupBy(_.value).agg[Int](sum("key"))
    println("grouped1:")
    grouped1.toDF.show()

    val grouped2 = ds.groupBy(_.value).agg[Int, Int](sum("key"), max("key"))
    println("grouped2:")
    grouped2.toDF.show()

    val grouped3 = ds.groupBy(x => TestData2(x.key, x.key + 1)).agg[Int](sum("key"))
    println("grouped3:")
    grouped3.toDF.show()

    val grouped4 = ds.groupBy(_.value).agg(sum((_: TestData).key))
    println("grouped4:")
    grouped4.toDF.show()

    val grouped5 = ds.groupBy(_.value).sum(_.key)
    println("grouped5:")
    grouped5.toDF.show()

    val as1 = ds.groupBy(_.key).sum(_.key).as[TestData2]
    println("as1:")
    as1.toDF.show()
  }
}
