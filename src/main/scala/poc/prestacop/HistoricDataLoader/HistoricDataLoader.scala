package poc.prestacop.HistoricDataLoader

import java.io.{BufferedReader, File, FileReader, FileWriter}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source._
import poc.prestacop.AppConfig

import scala.util.{Failure, Success, Try}

class HistoricDataLoader(producer: KafkaProducer[String, String]){

    import HistoricDataLoader._

    def run(): Unit =  {
        for {
            filesToProcess <- getAllFiles
            _ = createCheckpointDirectory()
            _ = processFiles(filesToProcess)
        } yield ()
    }

    private[this] val getAllFiles: Try[Seq[String]] = {
        val rootDir: File = new File(HISTORIC_DATA_ROOT_PATH)
        Try(
        rootDir
          .listFiles
          .filter(_.isFile)
          .map(file => file.getName)
          .filter(_.endsWith(".csv"))
          .toSeq
        ) match {
            case Success(filesSeq) =>
                Success(filesSeq)
            case Failure(exception) =>
                println("ERROR - Please check the specified path of the directory containing the files to export")
                Failure(exception)
        }
    }

    private[this] def getFileRootName(file: String): String = {
        file.split("\\.").head
    }

    private[this] def isAlreadyProcessed(file: String): Boolean = {
        val fileCheckpointPath: String = getCheckpointFileName(file)

        Try(fromFile(fileCheckpointPath)) match {
            case Success(sourceFile) =>
                sourceFile.mkString.toInt == PROCESSED_FILE_TAG
            case Failure(_) =>
                false
        }
    }

    private[this] def getCheckpointFileName(file: String): String = {
        val fileRoot: String = getFileRootName(file)
        s"$CHECKPOINT_FILE_PATH_PREFIX$fileRoot"
    }
    private[this] def loadCheckpoint(file: String): Int = {
        val fileCheckpointPath: String = getCheckpointFileName(file)
        Try(fromFile(fileCheckpointPath)) match {
            case Success(sourceFile) =>
                sourceFile.mkString.toInt
            case Failure(_) =>
                0
        }
    }
    private[this] def updateCheckpoint(file: String, checkpoint: Int): Unit = {
        val checkpointFile: String = getCheckpointFileName(file)
        val writer: FileWriter = new FileWriter(checkpointFile, false)
        writer.write(checkpoint.toString)
        writer.close()
    }

    private[this] def createFileReader(file: String): Try[BufferedReader] = {
        val filePath: String = s"$HISTORIC_DATA_ROOT_PATH/$file"
        Try(
            new BufferedReader(new FileReader(filePath))
        )
    }

    private[this] def createCheckpointDirectory(): Unit = {
        val dir: File = new File(PROCESS_CHECKPOINT_FILE_ROOT_PATH)
        if(!dir.exists()){
            dir.mkdir()
        }
    }

    @scala.annotation.tailrec
    private[this] def readAndSendFile(file: String, reader: BufferedReader, startingCheckpoint: Int, currentCheckpoint: Int): Unit = {
        if(currentCheckpoint >= startingCheckpoint) {
            val filePart: String = reader.readLine()
            if(filePart != null) {

                producer.send(new ProducerRecord(KAFKA_TOPIC, KAFKA_KEY, filePart))

                updateCheckpoint(file, currentCheckpoint)
                readAndSendFile(file, reader, startingCheckpoint, currentCheckpoint + 1)
            } else {
                reader.close()
                updateCheckpoint(file, PROCESSED_FILE_TAG)
                println(s"The file $file has been successfully exported.")
            }
        } else {
            readAndSendFile(file, reader, startingCheckpoint, currentCheckpoint + 1)
        }
    }

    private[this] def processFile(fileToProcess: String): Unit = {
        if (!isAlreadyProcessed(fileToProcess)) {
            println(s"reading file $fileToProcess")
            val checkpoint: Int = loadCheckpoint(fileToProcess)
            createFileReader(fileToProcess) match {
                case Success(bufferedReader) =>
                    readAndSendFile(fileToProcess, bufferedReader, checkpoint, 0)
                case Failure(exception) =>
                    println(s"Couldn't read file $fileToProcess. " + exception)
            }
        }
    }

    private[this] def processFiles(files: Seq[String]): Unit = {
        files.foreach{
            file =>
                processFile(file)
        }
    }

}

object HistoricDataLoader extends AppConfig {
    def apply(producer: KafkaProducer[String, String]):HistoricDataLoader = new HistoricDataLoader(producer)

    private val PROCESSED_FILE_TAG: Int = -1

    private val HISTORIC_DATA_ROOT_PATH: String = conf.getString("historic_data.raw_files.files_root_path")
    private val PROCESS_CHECKPOINT_FILE_ROOT_PATH: String = conf.getString("historic_data.raw_files.checkpoint_root_path")

    private val CHECKPOINT_PREFIX: String = "CHECKPOINT_"
    private val CHECKPOINT_FILE_PATH_PREFIX: String = s"$PROCESS_CHECKPOINT_FILE_ROOT_PATH/$CHECKPOINT_PREFIX"

    private val KAFKA_TOPIC: String = conf.getString("historic_data.kafka.kafka_topic")
    private val KAFKA_KEY: String = conf.getString("historic_data.kafka.kafka_key")
}
