package poc.prestacop.HistoricDataSaver

import org.apache.spark.sql.SparkSession
import poc.prestacop.AppConfig

object HistoricDataSaverMain extends AppConfig{

    val SPARKSESSION_APPNAME: String = conf.getString("historic_data.spark.appname")
    val SPARKSESSION_MASTER: String = conf.getString("historic_data.spark.master")
    def main(args: Array[String]): Unit = {

        val sparkSession: SparkSession =
            SparkSession
              .builder()
              .master(SPARKSESSION_MASTER)
              .appName(SPARKSESSION_APPNAME)
              .getOrCreate()

        HistoricDataSaver(sparkSession).run()
    }
}
