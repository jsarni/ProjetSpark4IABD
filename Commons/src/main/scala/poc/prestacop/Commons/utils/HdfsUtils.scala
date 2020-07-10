package poc.prestacop.Commons.utils

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{DataType, StructType}

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import org.apache.spark.sql.functions._

import scala.collection.breakOut

object HdfsUtils {

    def readFromHdfs(path: String, format: String, options: Option[Map[String, String]] = None)(implicit sparkSession: SparkSession): Try[DataFrame] = {
        Try{
            options match {
                case None =>
                    sparkSession.read.format(format).load(path)
                case Some(ops) =>
                    sparkSession.read.format(format).options(ops).load(path)
            }
        }
    }

    def writeToHdfs(dataFrame: DataFrame, path: String, format: String, mode: SaveMode = SaveMode.Append, options: Option[Map[String, String]] = None)(implicit sparkSession: SparkSession): Try[Unit] = {
        Try{
            options match {
                case None =>
                    dataFrame.write.format(format).mode(mode).save(path)
                case Some(ops) =>
                    dataFrame.write.format(format).options(ops).mode(mode).save(path)
            }
        }
    }

    def transformTo[T: TypeTag](dataFrame: DataFrame)(implicit ct: ClassTag[T]): DataFrame = {

        val oldDfFieldsNames: List[String] = dataFrame.columns.map(_.toLowerCase()).toList
        val caseClassFields: List[String] = ct.runtimeClass.getDeclaredFields.map(_.getName.toLowerCase).toList

        val caseClassSchema: StructType = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
        val dataTypes: Map[String, DataType] = caseClassSchema.fields.map(field => field.name -> field.dataType).toMap

        val columnsList: List[Column] = oldDfFieldsNames.zip(caseClassFields).map(f => {col(f._1).as(f._2)})

        val renamedDF: DataFrame = dataFrame.select(columnsList:_ *)

        val newDfFieldsNames: List[String] = renamedDF.columns.map(_.toLowerCase()).toList
        renamedDF.select(
            caseClassFields.map { field =>
                if (newDfFieldsNames.contains(field)) {
                    renamedDF(field).cast(dataTypes(field))
                } else {
                    lit(null).as(field).cast(dataTypes(field))
                }
            }: _*)

    }

}
