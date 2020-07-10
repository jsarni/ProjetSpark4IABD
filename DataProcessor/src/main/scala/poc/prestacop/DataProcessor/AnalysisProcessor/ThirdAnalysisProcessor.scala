package poc.prestacop.DataProcessor.AnalysisProcessor

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import poc.prestacop.Commons.AppConfig
import poc.prestacop.Commons.utils.HdfsUtils.writeToHdfs

import scala.util.Try

class ThirdAnalysisProcessor(dataFrame: DataFrame)(implicit sparkSession: SparkSession) {

    import ThirdAnalysisProcessor._
    import sparkSession.implicits._

    def run: Try[Unit] = {
        for {
            nightDF <- nightViolationsDF
            dayDF <- dayViolationsDF
            unionDF = nightDF.union(dayDF)
            aggregatedDF <- aggregationForThirdAnalysis(unionDF)
            repartitionnedDF = aggregatedDF.repartition(1)
            _ = writeToHdfs(repartitionnedDF, THIRD_ANALYSIS_TARGET_PATH, THIRD_ANALYSIS_TARGET_FORMAT, SaveMode.Overwrite)
        } yield ()
    }

    def aggregationForThirdAnalysis(df: DataFrame): Try[DataFrame] = {
        Try {
            val window: WindowSpec = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

            df
              .groupBy("period")
              .agg(count("violation_code").alias("number_of_violations"))
              .withColumn("total", sum('number_of_violations).over(window))
              .withColumn("percent", ('number_of_violations * 100) / 'total)
              .drop('total)
        }
    }

    lazy val nightViolationsDF: Try[DataFrame] = {
        Try{

            dataFrame
              .where(
                  (date_format('sending_date, "HHmm") >= "2000" && date_format('sending_date, "HHmm") <= "2359") ||
                    (date_format('sending_date, "HHmm") >= "0000" && date_format('sending_date, "HHmm") < "0800")
              ).withColumn("period", lit("night"))
        }
    }

    lazy val dayViolationsDF: Try[DataFrame] = {
        Try{

            dataFrame.where(
                date_format('sending_date, "HHmm") >= "0800" && date_format('sending_date, "HHmm") < "2000"
            ).withColumn("period", lit("day"))
        }
    }
}

object ThirdAnalysisProcessor extends AppConfig {
    def apply(dataFrame: DataFrame)(implicit sparkSession: SparkSession): ThirdAnalysisProcessor = new ThirdAnalysisProcessor(dataFrame)(sparkSession)

    val THIRD_ANALYSIS_TARGET_PATH: String = conf.getString("hdfs.target.third_analysis_path")
    val THIRD_ANALYSIS_TARGET_FORMAT: String = conf.getString("hdfs.target.third_analysis_format")

}

