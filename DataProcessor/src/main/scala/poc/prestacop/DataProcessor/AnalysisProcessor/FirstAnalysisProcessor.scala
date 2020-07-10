package poc.prestacop.DataProcessor.AnalysisProcessor

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{count, date_format, lit, sum}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import poc.prestacop.Commons.AppConfig
import poc.prestacop.Commons.utils.HdfsUtils.writeToHdfs

import scala.util.Try

class FirstAnalysisProcessor(dataFrame: DataFrame)(implicit sparkSession: SparkSession) {

    import FirstAnalysisProcessor._
    import sparkSession.implicits._

    def run: Try[Unit] = {
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
            val window: WindowSpec = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

            df
              .groupBy("season")
              .agg(count("violation_code").alias("number_of_violations"))
              .withColumn("total", sum('number_of_violations).over(window))
              .withColumn("percent", ('number_of_violations * 100) / 'total)
              .drop('total)
        }
    }

    lazy val winterViolationsDF: Try[DataFrame] = {
        Try{
            dataFrame
              .where(
                  (date_format('sending_date, "MMdd") >= "1221" && date_format('sending_date, "MMdd") <= "1231") ||
                    (date_format('sending_date, "MMdd") >= "0101" && date_format('sending_date, "MMdd") < "0321")
              ).withColumn("season", lit("winter"))
        }
    }

    lazy val summerViolationsDF: Try[DataFrame] = {
        Try{
            dataFrame.where(
                date_format('sending_date, "MMdd") >= "0621" && date_format('sending_date, "MMdd") < "0921"
            ).withColumn("season", lit("summer"))
        }
    }

}

object FirstAnalysisProcessor extends AppConfig {
    def apply(dataFrame: DataFrame)(implicit sparkSession: SparkSession): FirstAnalysisProcessor = new FirstAnalysisProcessor(dataFrame)(sparkSession)

    val FIRST_ANALYSIS_TARGET_PATH: String = conf.getString("hdfs.target.first_analysis_path")
    val FIRST_ANALYSIS_TARGET_FORMAT: String = conf.getString("hdfs.target.first_analysis_format")

}
