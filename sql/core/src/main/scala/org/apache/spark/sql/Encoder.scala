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

import java.lang.reflect.Constructor

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.{ScalaReflectionLock, CatalystTypeConverters, ScalaReflection}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
 * Captures how to encode Java objects as Spark SQL rows.
 */
@Experimental
trait Encoder[T] extends Serializable {
  def schema: StructType

  def typeTag: TypeTag[T]

  def classTag: ClassTag[T]

  def fromRow(row: Row): T

  def toRow(value: T): Row  // TODO: should probably take a MutableRow as input and write into it
}

@Experimental
class ProductEncoder[T <: Product](@transient implicit val typeTag: TypeTag[T]) extends Encoder[T] {
  override val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

  private val converter = CatalystTypeConverters.createToCatalystConverter(schema)

  override val classTag = ScalaReflectionLock.synchronized {
    val cls = typeTag.mirror.runtimeClass(typeTag.tpe.erasure.typeSymbol.asClass)
    ClassTag[T](cls)
  }

  @transient private lazy val constructor = {
    // TODO: deal with Spark class loaders, etc?
    val cls = classTag.runtimeClass
    val cons = cls.getConstructors.filter(_.getParameterTypes.size == schema.fields.length).head
    cons.asInstanceOf[Constructor[T]]
  }

  override def fromRow(row: Row): T = {
    constructor.newInstance(row.toSeq.map(_.asInstanceOf[AnyRef]): _*)
  }

  override def toRow(value: T): Row = {
    converter.apply(value).asInstanceOf[Row]
  }
}


@Experimental
class PrimitiveEncoder[T](@transient implicit val typeTag: TypeTag[T]) extends Encoder[T] {
  private val dataType =  ScalaReflection.schemaFor[T].dataType
  override val schema = StructType(Seq(StructField("_1", dataType)))

  private val rowConverter = CatalystTypeConverters.createToCatalystConverter(dataType)
  private val scalaConverter = CatalystTypeConverters.createToScalaConverter(dataType)

  override val classTag = ScalaReflectionLock.synchronized {
    val cls = typeTag.mirror.runtimeClass(typeTag.tpe.erasure.typeSymbol.asClass)
    ClassTag[T](cls)
  }

  override def fromRow(row: Row): T = {
    scalaConverter(row(0)).asInstanceOf[T]
  }

  override def toRow(value: T): Row = {
    Row(rowConverter.apply(value))
  }
}

@Experimental
object Encoder {
  private implicit def encoderTypeTag[T: Encoder]: TypeTag[T] = implicitly[Encoder[T]].typeTag

  implicit def productEncoder[T <: Product : TypeTag] = new ProductEncoder[T]

  implicit def tuple2Encoder[T1: Encoder, T2: Encoder] = new ProductEncoder[(T1, T2)]

  implicit def tuple3Encoder[T1: Encoder, T2: Encoder, T3: Encoder] = {
    new ProductEncoder[(T1, T2, T3)]
  }

  implicit val intEncoder = new PrimitiveEncoder[Int]
  implicit val stringEncoder = new PrimitiveEncoder[String]
}
