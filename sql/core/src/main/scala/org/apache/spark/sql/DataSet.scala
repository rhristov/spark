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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
 * A typed dataset stored efficiently as a DataFrame.
 *
 * TODO: we should support things other than Product
 */
@Experimental
class Dataset[T] private[sql](dataFrame: DataFrame)(implicit encoder: Encoder[T]) {
  private val sqlContext = dataFrame.sqlContext
  import sqlContext.implicits._

  private implicit val classTag = encoder.classTag

  def rdd: RDD[T] = dataFrame.rdd.map(encoder.fromRow)

  /** Return an expression that represents all of the data frame's columns as a struct */
  private lazy val rowStruct: Column = {
    struct(dataFrame.columns.map(Column(_)): _*)
  }

  def filter(func: T => Boolean): Dataset[T] = {
    val enc = encoder  // to prevent closures capturing this
    val f = functions.udf((r: Row) => func(enc.fromRow(r)))
    new Dataset[T](dataFrame.filter(f(rowStruct)))(encoder)
  }

  def map[U](func: T => U)(implicit uEncoder: Encoder[U]): Dataset[U] = {
    val enc = encoder  // to prevent closures capturing this
    implicit def uTag = uEncoder.typeTag
    val f = functions.udf((r: Row) => func(enc.fromRow(r)))
    val outputStructs = dataFrame.select(f(rowStruct).as("_1"))
    val fieldExprs: Seq[String] = if (f.dataType.isInstanceOf[StructType]) {
      uEncoder.schema.fieldNames.map(c => "_1." + c)
    } else {
      Seq("_1")
    }
    val newRows = outputStructs.select(fieldExprs.map(Column(_)): _*)
    new Dataset[U](newRows)(uEncoder)
  }

  def groupBy[K](func: T => K)(implicit kEncoder: Encoder[K]): GroupedDataset[K, T] = {
    val enc = encoder  // to prevent closures capturing this
    implicit def kTag = kEncoder.typeTag
    val f = functions.udf((r: Row) => func(enc.fromRow(r)))
    val grouped = dataFrame.groupBy(f(rowStruct).as("_1"))
    new GroupedDataset[K, T](grouped)
  }

  def as[U: Encoder]: Dataset[U] = {
    val uEncoder = implicitly[Encoder[U]]
    val myTypes = encoder.schema.fields.map(_.dataType)
    val uTypes = uEncoder.schema.fields.map(_.dataType)
    if (!myTypes.sameElements(uTypes)) {
      throw new AnalysisException(s"Mismatched data types: ${encoder.schema} vs ${uEncoder.schema}")
    }
    val mapping = encoder.schema.fields.zip(uEncoder.schema.fields).map {
      p => Column(p._1.name).as(p._2.name)
    }
    new Dataset[U](dataFrame.select(mapping: _*))
  }

  def toDF: DataFrame = dataFrame
}

