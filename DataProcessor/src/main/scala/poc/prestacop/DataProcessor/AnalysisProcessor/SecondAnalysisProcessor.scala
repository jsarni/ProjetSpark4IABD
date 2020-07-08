package poc.prestacop.DataProcessor.AnalysisProcessor

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{count, desc, sum}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import poc.prestacop.Commons.AppConfig
import poc.prestacop.Commons.utils.HdfsUtils.writeToHdfs

import scala.util.Try

class SecondAnalysisProcessor(dataFrame: DataFrame)(implicit sparkSession: SparkSession) {

    import SecondAnalysisProcessor._
    import sparkSession.implicits._

    def run: Try[Unit] = {
        for {
            violationsDF <- topFiveViolationsDF
            repartitionnedDF = violationsDF.repartition(1)
            _ = writeToHdfs(repartitionnedDF, SECOND_ANALYSIS_TARGET_PATH, SECOND_ANALYSIS_TARGET_FORMAT, SaveMode.Overwrite)
        } yield ()
    }

    lazy val topFiveViolationsDF: Try[DataFrame] = {
        Try{
            val window: WindowSpec = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

            dataFrame.groupBy("violation_code")
              .agg(count("violation_code").alias("number_of_violations"))
              .withColumn("total", sum('number_of_violations).over(window))
              .withColumn("percent", ('number_of_violations * 100) / 'total)
              .sort(desc("number_of_violations"))
              .limit(5)
        }
    }
}

object SecondAnalysisProcessor extends AppConfig {
    def apply(dataFrame: DataFrame)(implicit sparkSession: SparkSession): SecondAnalysisProcessor = new SecondAnalysisProcessor(dataFrame)(sparkSession)

    val SECOND_ANALYSIS_TARGET_PATH: String = conf.getString("hdfs.target.second_analysis_path")
    val SECOND_ANALYSIS_TARGET_FORMAT: String = conf.getString("hdfs.target.second_analysis_format")

}

