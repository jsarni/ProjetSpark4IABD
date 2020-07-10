package poc.prestacop.DataProcessor.AnalysisProcessor

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import poc.prestacop.Commons.AppConfig
import poc.prestacop.Commons.utils.HdfsUtils.writeToHdfs

import scala.util.Try

class FourthAnalysisProcessor(dataFrame: DataFrame)(implicit sparkSession: SparkSession) {

    import FourthAnalysisProcessor._
    import sparkSession.implicits._

    def run: Try[Unit] = {
        for {
            winterDF <- winterViolationsDF
            springDF <- springViolationsDF
            summerDF <- summerViolationsDF
            autumnDF <- autumnViolationsDF
            aggWinterDF <- aggregateSeasonViolations(winterDF)
            aggSprintDF <- aggregateSeasonViolations(springDF)
            aggSummerDF <- aggregateSeasonViolations(summerDF)
            aggAutumnDF <- aggregateSeasonViolations(autumnDF)
            finalDF = aggWinterDF.union(aggSprintDF).union(aggSummerDF).union(aggAutumnDF)
            repartitionnedDF = finalDF.repartition(1)
            _ = writeToHdfs(repartitionnedDF, FOURTH_ANALYSIS_TARGET_PATH, FOURTH_ANALYSIS_TARGET_FORMAT, SaveMode.Overwrite)
        } yield ()
    }

    def aggregateSeasonViolations(seasonDF: DataFrame): Try[DataFrame] = {
        Try{
            val window: WindowSpec = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

            seasonDF.groupBy("season", "violation_code")
              .agg(count("violation_code").alias("number_of_violations"))
              .withColumn("total", sum('number_of_violations).over(window))
              .withColumn("percent", ('number_of_violations * 100) / 'total)
              .drop('total)
              .sort(desc("number_of_violations"))
              .limit(1)
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

    lazy val springViolationsDF: Try[DataFrame] = {
        Try{
            dataFrame.where(
                date_format('sending_date, "MMdd") >= "0321" && date_format('sending_date, "MMdd") < "0621"
            ).withColumn("season", lit("spring"))
        }
    }

    lazy val summerViolationsDF: Try[DataFrame] = {
        Try{
            dataFrame.where(
                date_format('sending_date, "MMdd") >= "0621" && date_format('sending_date, "MMdd") < "0921"
            ).withColumn("season", lit("summer"))
        }
    }

    lazy val autumnViolationsDF: Try[DataFrame] = {
        Try{
            dataFrame.where(
                date_format('sending_date, "MMdd") >= "0921" && date_format('sending_date, "MMdd") < "1221"
            ).withColumn("season", lit("autumn"))
        }
    }
}

object FourthAnalysisProcessor extends AppConfig {
    def apply(dataFrame: DataFrame)(implicit sparkSession: SparkSession): FourthAnalysisProcessor = new FourthAnalysisProcessor(dataFrame)(sparkSession)

    val FOURTH_ANALYSIS_TARGET_PATH: String = conf.getString("hdfs.target.fourth_analysis_path")
    val FOURTH_ANALYSIS_TARGET_FORMAT: String = conf.getString("hdfs.target.fourth_analysis_format")

}

