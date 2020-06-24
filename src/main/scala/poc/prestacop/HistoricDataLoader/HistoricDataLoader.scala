package poc.prestacop.HistoricDataLoader


import poc.prestacop.AppConfig
import java.io.{BufferedReader, File, FileNotFoundException, FileReader, FileWriter}

import scala.io.Source._
import scala.util.{Failure, Success, Try}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

class HistoricDataLoader(producer: KafkaProducer[String, String]){

    import HistoricDataLoader._

    def run(): Unit =  {
        for {
            filesToProcess <- getAllFiles
            _ = createCheckpointDirectory()
            _ = createMarkpointDirectory()
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
        val fileMarkpointFile: String = getMarkpointFileName(file)

        if(pathExists(fileMarkpointFile)) {
            val markpointFile: BufferedSource = fromFile(fileMarkpointFile)
            Try(markpointFile.mkString.toInt) match {
                case Success(markpoint) =>
                    markpointFile.close()
                    markpoint == PROCESSED_FILE_TAG
                case Failure(_) =>
                    markpointFile.close()
                    false
            }
        } else {
            false
        }
    }

    private[this] def getCheckpointFileName(file: String): String = {
        val fileRoot: String = getFileRootName(file)
        s"$CHECKPOINT_FILE_PATH_PREFIX$fileRoot"
    }

    private[this] def getMarkpointFileName(file: String): String = {
        val fileRoot: String = getFileRootName(file)
        s"$MARKPOINT_FILE_PATH_PREFIX$fileRoot"
    }

    private[this] def pathExists(path: String): Boolean = {
        val file: File = new File(path)
        file.exists()
    }

    private[this] def loadCheckpoint(file: String): Int = {
        val fileCheckpointPath: String = getCheckpointFileName(file)

        if (pathExists(fileCheckpointPath)) {
            val checkpointFile: BufferedSource = fromFile(fileCheckpointPath)
            Try(checkpointFile.mkString.toInt) match {
                case Success(checkpoint) =>
                    checkpointFile.close()
                    println(s"SUCCESS - Starting file export from Checkpoint $checkpoint")
                    checkpoint
                case Failure(_) =>
                    checkpointFile.close()
                    println("INFO - Problem with checkpoint file content... Restarting file processing from the begining ")
                    0
            }
        } else {
            println("INFO - No checkpoint found for this file... Starting file processing from the begining")
            0
        }
    }

    private[this] def loadMarkpoint(file: String): Int = {
        val fileMarkpointPath: String = getMarkpointFileName(file)

        if (pathExists(fileMarkpointPath)) {
            val markpointFile: BufferedSource = fromFile(fileMarkpointPath)
            Try(markpointFile.mkString.toInt) match {
                case Success(markpoint) =>
                    markpointFile.close()
                    println(s"SUCCESS - Starting file export from Markpoint $markpoint")
                    markpoint
                case Failure(_) =>
                    markpointFile.close()
                    println("INFO - Problem with Markpoint file content... Trying to start file processing from last checkpoint")
                    MARKPOINT_LOAD_ERROR_TAG
            }
        } else {
            println("INFO - No Markpoint found for this file... Starting file processing from the last checkpoint")
            MARKPOINT_LOAD_ERROR_TAG
        }
    }

    @throws(classOf[FileNotFoundException])
    private[this] def updateCheckpoint(file: String, checkpoint: Int): Unit = {
        if(pathExists(PROCESS_CHECKPOINT_FILE_ROOT_PATH)) {
            val checkpointFile: String = getCheckpointFileName(file)
            val writer: FileWriter = new FileWriter(checkpointFile, false)
            writer.write(checkpoint.toString)
            writer.close()
        } else {
            println("ERROR - No Checkpoints directory found")
            throw new FileNotFoundException("No Checkpoints directory found")
        }
    }

    @throws(classOf[FileNotFoundException])
    private[this] def updateMarkpoint(file: String, markpoint: Int): Unit = {
        if(pathExists(PROCESS_MARKPOINT_FILE_ROOT_PATH)) {
            val markpointFile: String = getMarkpointFileName(file)
            val writer: FileWriter = new FileWriter(markpointFile, false)
            writer.write(markpoint.toString)
            writer.close()
        } else {
            println("ERROR - No Markpoints directory found")
            throw new FileNotFoundException("No Markpoints directory found")
        }
    }

    private[this] def createFileReader(file: String): Try[BufferedReader] = {
        val filePath: String = s"$HISTORIC_DATA_ROOT_PATH/$file"
        Try(
            new BufferedReader(new FileReader(filePath))
        )
    }

    private[this] def createCheckpointDirectory(): Unit = {
        if(!pathExists(PROCESS_CHECKPOINT_FILE_ROOT_PATH)){
            val dir: File = new File(PROCESS_CHECKPOINT_FILE_ROOT_PATH)
            dir.mkdir()
            if(pathExists(PROCESS_CHECKPOINT_FILE_ROOT_PATH)) {
                println("SUCCESS - Checkpoints directory was successfully created")
            } else {
                println("ERROR - Checkpoints directory couldn't be created")
            }
        } else {
            println("INFO - Checkpoints directory do already exist")
        }
    }

    private[this] def createMarkpointDirectory(): Unit = {
        if(!pathExists(PROCESS_MARKPOINT_FILE_ROOT_PATH)){
            val dir: File = new File(PROCESS_MARKPOINT_FILE_ROOT_PATH)
            dir.mkdir()
            if(pathExists(PROCESS_MARKPOINT_FILE_ROOT_PATH)) {
                println("SUCCESS - Markpoints directory was successfully created")
            } else {
                println("ERROR - Markpoints directory couldn't be created")
            }
        } else {
            println("INFO - Markpoints directory do already exist")
        }
    }

    @scala.annotation.tailrec
    private[this] def readAndSendFile(file: String,
                                      reader: BufferedReader,
                                      startingMarkpoint: Int,
                                      currentMarkpoint: Int): Unit = {

        if(currentMarkpoint >= startingMarkpoint) {
            val filePart: String = reader.readLine()
            if(filePart != null) {

                producer.send(new ProducerRecord(KAFKA_TOPIC, KAFKA_KEY, filePart))

                updateMarkpoint(file, currentMarkpoint)
                if ((currentMarkpoint % NB_CHECKPOINT_TO_PRINT_INFO) == 0 && (currentMarkpoint > 0)){
                    updateCheckpoint(file, currentMarkpoint)
                    println(s"-------> Number of processed Lines : ${currentMarkpoint / 1000}K")
                }
                readAndSendFile(file, reader, startingMarkpoint, currentMarkpoint + 1)
            } else {
                reader.close()
                updateCheckpoint(file, PROCESSED_FILE_TAG)
                println(s"********************** The file '$file' has been successfully exported **********************")
            }
        } else {
            readAndSendFile(file, reader, startingMarkpoint, currentMarkpoint + 1)
        }
    }

    private[this] def processFile(fileToProcess: String): Unit = {
        if (!isAlreadyProcessed(fileToProcess)) {
            println(s"================ Started Processing file '$fileToProcess' ================")

            val markpoint: Int = loadMarkpoint(fileToProcess)

            val startingPoint: Int =
                if (markpoint == MARKPOINT_LOAD_ERROR_TAG){
                    loadCheckpoint(fileToProcess)
                } else {
                    markpoint
                }

            createFileReader(fileToProcess) match {
                case Success(bufferedReader) =>
                    println("INFO - Started exporting the file")
                    readAndSendFile(fileToProcess, bufferedReader, startingPoint, 0)
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
    private val MARKPOINT_LOAD_ERROR_TAG: Int = -2
    private val NB_CHECKPOINT_TO_PRINT_INFO: Int = 100000

    private val HISTORIC_DATA_ROOT_PATH: String = conf.getString("historic_data.raw_files.files_root_path")
    private val PROCESS_CHECKPOINT_FILE_ROOT_PATH: String = conf.getString("historic_data.raw_files.checkpoint_root_path")
    private val PROCESS_MARKPOINT_FILE_ROOT_PATH: String = conf.getString("historic_data.raw_files.markpoint_root_path")

    private val CHECKPOINT_PREFIX: String = "CHECKPOINT_"
    private val MARKPOINT_PREFIX: String = "MARKPOINT_"
    private val CHECKPOINT_FILE_PATH_PREFIX: String = s"$PROCESS_CHECKPOINT_FILE_ROOT_PATH/$CHECKPOINT_PREFIX"
    private val MARKPOINT_FILE_PATH_PREFIX: String = s"$PROCESS_MARKPOINT_FILE_ROOT_PATH/$MARKPOINT_PREFIX"

    private val KAFKA_TOPIC: String = conf.getString("historic_data.kafka.kafka_topic")
    private val KAFKA_KEY: String = conf.getString("historic_data.kafka.kafka_key")
}
