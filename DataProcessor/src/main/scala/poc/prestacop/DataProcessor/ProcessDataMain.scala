package poc.prestacop.DataProcessor

import org.apache.spark.sql.SparkSession
import poc.prestacop

object ProcessDataMain extends AppConfig {

    def main(args: Array[String]): Unit = {

        val SPARKSESSION_APPNAME: String = conf.getString("historic_data.spark.appname")
        val SPARKSESSION_MASTER: String = conf.getString("historic_data.spark.master")

        val sparkSession: SparkSession =
            SparkSession.builder()
              .appName(SPARKSESSION_APPNAME)
              .master(SPARKSESSION_MASTER)
              .getOrCreate()

        sparkSession.stop()
    }
}
