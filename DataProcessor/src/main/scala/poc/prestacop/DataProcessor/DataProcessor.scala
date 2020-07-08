package poc.prestacop.DataProcessor

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import poc.prestacop.Commons.{AppConfig, schema}
import poc.prestacop.Commons.utils.HdfsUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import poc.prestacop.Commons.schema.DroneViolationMessage

import scala.util.Try
class DataProcessor(dataFrame: DataFrame)(implicit sparkSession: SparkSession) {

    import sparkSession.implicits._
    import DataProcessor._

    /**
     * Class to manage the following 4 analysis which are :
     *      - First Analysis : Do violations happen more often during Winter than during Summer ?
     *      - Second Analysis : Which are the 5 most common violation ?
     *      - Third Analysis : Do violations happen more often during night (from 8pm to 8am) ?
     *      - Fourth Analysis : What are the most common violations for each season ?
     *
     * The following analysis are run on a dataset of 1.7 million lines
     */


    val preparedDF: DataFrame =
        transformTo[DroneViolationMessage](dataFrame)
          .persist(StorageLevel.MEMORY_AND_DISK)
          .repartition(SPARK_DEFAULT_PARTITIONS)

    def run(): Unit = {
        for {
            _ <- manageFirstAnalysis
            _ = preparedDF.unpersist()
        } yield ()
    }

    def manageFirstAnalysis: Try[Unit] = {
        for {
            winterDF <- winterViolationsDF
            summerDF <- summerViolationsDF
            unionDF = winterDF.union(summerDF)
            aggregatedDF <- aggregationForFirstAnalysis(unionDF)
            repartitionnedDF = aggregatedDF.repartition(1)
            _ = writeToHdfs(repartitionnedDF, FIRST_ANALYSIS_TARGET_PATH, FIRST_ANALYSIS_TARGET_FORMAT, SaveMode.Overwrite)
        } yield ()
    }

    def aggregationForFirstAnalysis(df: DataFrame): Try[DataFrame] = {
        Try {
            df
              .groupBy("season")
              .agg(count("violation_code").alias("number_of_violations"))
        }
    }

    lazy val winterViolationsDF: Try[DataFrame] = {
        Try{
            preparedDF
              .where(
                  (date_format('sending_date, "MMdd") >= "1221" && date_format('sending_date, "MMdd") <= "1231") ||
                    (date_format('sending_date, "MMdd") >= "0101" && date_format('sending_date, "MMdd") < "0321")
            ).withColumn("season", lit("winter"))
        }
    }

    lazy val summerViolationsDF: Try[DataFrame] = {
        Try{
            preparedDF.where(
                date_format('sending_date, "MMdd") >= "0621" && date_format('sending_date, "MMdd") < "0921"
            ).withColumn("season", lit("summer"))
        }
    }


}

object DataProcessor extends AppConfig{
    def apply(dataFrame: DataFrame)(implicit sparkSession: SparkSession): DataProcessor = new DataProcessor(dataFrame)(sparkSession)

    val SPARK_DEFAULT_PARTITIONS: Int = conf.getInt("spark.default_partitions")


    val FIRST_ANALYSIS_TARGET_PATH: String = conf.getString("hdfs.target.first_analysis_path")
    val SECOND_ANALYSIS_TARGET_PATH: String = conf.getString("hdfs.target.second_analysis_path")
    val THIRD_ANALYSIS_TARGET_PATH: String = conf.getString("hdfs.target.third_analysis_path")
    val FOURTH_ANALYSIS_TARGET_PATH: String = conf.getString("hdfs.target.fourth_analysis_path")


    val FIRST_ANALYSIS_TARGET_FORMAT: String = conf.getString("hdfs.target.first_analysis_format")
    val SECOND_ANALYSIS_TARGET_FORMAT: String = conf.getString("hdfs.target.second_analysis_format")
    val THIRD_ANALYSIS_TARGET_FORMAT: String = conf.getString("hdfs.target.third_analysis_format")
    val FOURTH_ANALYSIS_TARGET_FORMAT: String = conf.getString("hdfs.target.fourth_analysis_format")
}
