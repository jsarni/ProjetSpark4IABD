package poc.prestacop.DataProcessor

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import poc.prestacop.Commons.AppConfig
import poc.prestacop.Commons.utils.HdfsUtils._
import org.apache.spark.storage.StorageLevel
import poc.prestacop.Commons.schema.DroneViolationMessage
import poc.prestacop.DataProcessor.AnalysisProcessor.{FirstAnalysisProcessor, FourthAnalysisProcessor, SecondAnalysisProcessor, ThirdAnalysisProcessor}

class DataProcessor(dataFrame: DataFrame)(implicit sparkSession: SparkSession) {

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
          .where(col("sending_date").isNotNull)
          .persist(StorageLevel.MEMORY_AND_DISK)
          .repartition(SPARK_DEFAULT_PARTITIONS)

    def run(): Unit = {
        for {
            _ <- FirstAnalysisProcessor(preparedDF).run
            _ <- SecondAnalysisProcessor(preparedDF).run
            _ <- ThirdAnalysisProcessor(preparedDF).run
            _ <- FourthAnalysisProcessor(preparedDF).run
            _ = preparedDF.unpersist()
        } yield ()
    }

}

object DataProcessor extends AppConfig{
    def apply(dataFrame: DataFrame)(implicit sparkSession: SparkSession): DataProcessor = new DataProcessor(dataFrame)(sparkSession)

    val SPARK_DEFAULT_PARTITIONS: Int = conf.getInt("spark.default_partitions")



    val THIRD_ANALYSIS_TARGET_PATH: String = conf.getString("hdfs.target.third_analysis_path")
    val FOURTH_ANALYSIS_TARGET_PATH: String = conf.getString("hdfs.target.fourth_analysis_path")

    val THIRD_ANALYSIS_TARGET_FORMAT: String = conf.getString("hdfs.target.third_analysis_format")
    val FOURTH_ANALYSIS_TARGET_FORMAT: String = conf.getString("hdfs.target.fourth_analysis_format")
}
