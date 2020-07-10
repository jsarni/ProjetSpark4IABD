package poc.prestacop.DataProcessor

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import poc.prestacop.Commons.AppConfig
import poc.prestacop.Commons.schema.DroneViolationMessage
import poc.prestacop.Commons.utils.HdfsUtils.readFromHdfs

import scala.util.{Failure, Success, Try}

object ProcessDataMain extends AppConfig {

    def main(args: Array[String]): Unit = {

        val SPARKSESSION_APPNAME: String = conf.getString("spark.appname")
        val SPARKSESSION_MASTER: String = conf.getString("spark.master")
        val HDFS_FILE_PATH: String = conf.getString("hdfs.source_file.path")
        val HDFS_FILE_FORMAT: String = conf.getString("hdfs.source_file.format")

        implicit val sparkSession: SparkSession =
            SparkSession.builder()
              .appName(SPARKSESSION_APPNAME)
              .master(SPARKSESSION_MASTER)
              .getOrCreate()



        val srcDF: Try[DataFrame] = readFromHdfs(HDFS_FILE_PATH, HDFS_FILE_FORMAT)

        srcDF match {
            case Success(df) =>
                DataProcessor(df).run()
            case Failure(exception) =>
                println(s"ERROR - Could load data $HDFS_FILE_PATH from lake: ${exception.getMessage} ")
        }
        sparkSession.stop()
    }
}
