package example

import com.spotify.scio._
import example.objects.{NumberAuto, NumberManual, NumberMessage}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.options.{Description, PipelineOptions, PipelineOptionsFactory, StreamingOptions, ValueProvider}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.language.implicitConversions

/*
sbt "runMain example.SourceMerge
*/

class SourceMerge(@transient val sc: ScioContext) extends Serializable {
  def processSources(autoTopic: String, manualTopic: String, numberInfo: String): Unit = {
    val autoInput = sc.customInput("auto", PubsubIO.readMessages().fromSubscription(autoTopic))
      .map(NumberAuto(_))

    val manualInput = sc.customInput("manual", PubsubIO.readMessages().fromSubscription(manualTopic))
      .map(NumberManual(_))
  }
}

object SourceMerge {
  final val logger = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val projectName = "playground-bart"
    val bucketPath = "gs://playground-bart"

    val subscriptions = Map(
      "auto" -> s"projects/$projectName/subscriptions/sensor-auto",
      "manual" -> s"projects/$projectName/subscriptions/sensor-manual"
    )

    val numberInfo = s"$projectName:playground.numbers"

    val (sc, args) = ContextAndArgs(cmdlineArgs ++ Array(
      s"--stagingLocation=$bucketPath/staging",
      s"--tempLocation=$bucketPath/temp/"))

    sc.options.setJobName("example-etl")
    sc.optionsAs[DataflowPipelineOptions].setProject("playground-bart")
    sc.optionsAs[DataflowPipelineOptions].setRegion("europe-west4")
    sc.optionsAs[DataflowPipelineOptions].setExperiments(List("shuffle_mode=service").asJava)
    sc.optionsAs[DataflowPipelineOptions].setNumWorkers(1)
    sc.optionsAs[DataflowPipelineOptions].setStreaming(true)

    new SourceMerge(sc).processSources(
      autoTopic = subscriptions("auto"),
      manualTopic = subscriptions("manual"),
      numberInfo = numberInfo)

    val result = sc.close().waitUntilFinish()
//    logger.info(result)
  }
}
