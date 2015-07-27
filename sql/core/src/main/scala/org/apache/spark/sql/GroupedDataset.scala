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

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.functions._

trait TypedAggregate[T, R] {
  def resultEncoder: Encoder[R]
}

case class TypedSum[T, R: Encoder](func: T => R) extends TypedAggregate[T, R] {
  override def resultEncoder: Encoder[R] = implicitly[Encoder[R]]
}

@Experimental
class GroupedDataset[K: Encoder, V: Encoder] private[sql](data: GroupedData) {
  def agg[T1: Encoder](e1: Column): Dataset[(K, T1)] = {
    new Dataset[(K, T1)](data.agg(e1.as("_2")))
  }

  def agg[T1: Encoder, T2: Encoder](e1: Column, e2: Column): Dataset[(K, T1, T2)] = {
    new Dataset[(K, T1, T2)](data.agg(e1.as("_2"), e2.as("_3")))
  }

  private def typedAggregateToColumn[R](agg: TypedAggregate[V, R]): Column = {
    val vEnc = implicitly[Encoder[V]]  // to prevent closures capturing this
    implicit def rTag = agg.resultEncoder.typeTag
    val rowStruct = struct(data.dataFrame.columns.map(Column(_)): _*)

    agg match {
      case TypedSum(func) =>
        val f = functions.udf((r: Row) => func(vEnc.fromRow(r)))
        functions.sum(f(rowStruct))

      case other => ???
    }
  }

  def agg[T1](e1: TypedAggregate[V, T1]): Dataset[(K, T1)] = {
    implicit val t1Enc = e1.resultEncoder
    agg[T1](typedAggregateToColumn(e1))
  }

  def agg[T1, T2](e1: TypedAggregate[V, T1], e2: TypedAggregate[V, T2]): Dataset[(K, T1, T2)] = {
    implicit val t1Enc = e1.resultEncoder
    implicit val t2Enc = e2.resultEncoder
    agg[T1, T2](typedAggregateToColumn(e1), typedAggregateToColumn(e2))
  }

  def sum[T1: Encoder](f1: V => T1): Dataset[(K, T1)] = {
    agg[T1](TypedSum(f1))
  }

  def sum[T1: Encoder, T2: Encoder](f1: V => T1, f2: V => T2): Dataset[(K, T1, T2)] = {
    agg[T1, T2](TypedSum(f1), TypedSum(f2))
  }
}
