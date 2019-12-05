package example

import com.google.protobuf.Message
import com.spotify.scio._
import example.message.NumberBuffer

//import org.joda.time.Duration
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.slf4j.LoggerFactory


/*
sbt "runMain example.SourceMerge"
*/

class SourceMerge(@transient val sc: ScioContext) extends Serializable {
  def processSources(autoTopic: String, manualTopic: String, numberInfo: String): Unit = {
    val autoInput = sc.customInput("auto",
      PubsubIO
        .readProtos(classOf[NumberBuffer].asSubclass(classOf[Message]))
        .fromSubscription(autoTopic))

    autoInput.map(x => x)
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
    /*    sc.optionsAs[DataflowPipelineOptions].setProject("playground-bart")
        sc.optionsAs[DataflowPipelineOptions].setRegion("europe-west4")
        sc.optionsAs[DataflowPipelineOptions].setExperiments(List("shuffle_mode=service").asJava)
        sc.optionsAs[DataflowPipelineOptions].setNumWorkers(1)
        sc.optionsAs[DataflowPipelineOptions].setStreaming(true)*/

    new SourceMerge(sc).processSources(
      autoTopic = subscriptions("auto"),
      manualTopic = subscriptions("manual"),
      numberInfo = numberInfo)

    val result = sc.close().waitUntilFinish()
    //    logger.info(result)
  }
}
