package example

import com.spotify.scio._
import com.spotify.scio.values.{SCollection, WindowOptions}
import example.proto.message.NumberBuffer
import example.objects.NumberInfo
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.joda.time.Duration
import org.slf4j.LoggerFactory
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner.LATEST

//import com.spotify.scio.bigquery.types.BigQueryType

//@BigQueryType.toTable
//case class NumberResult(number_avg: Float, number_type: String)

/*
sbt "runMain example.SourceMerge"
*/

class SourceMerge(@transient val sc: ScioContext) extends Serializable {
  def processSources(autoTopic: String, manualTopic: String, numberInfo: String): Unit = {
    val autoInput: SCollection[NumberBuffer] = sc.pubsubSubscription[NumberBuffer](autoTopic)
    val manualInput: SCollection[NumberBuffer] = sc.pubsubSubscription[NumberBuffer](manualTopic)

//    @BigQueryType.fromTable(numberInfo)
//    class NumberInfoRow

    autoInput
      .union(manualInput)
      .map(NumberInfo(_))
      .timestampBy(_.timestamp.get)
      .withFixedWindows(Duration.standardSeconds(10), options = WindowOptions(timestampCombiner = LATEST))
      .map(_.number)
      .sum
      .withWindow[IntervalWindow]
      .map {
        case (score, window) => (window.end(), score)
      }
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
    val numberResults = s"$projectName:playground.results"

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

    sc.close().waitUntilFinish()
    //    val result = sc.close().waitUntilFinish()
    //    logger.info(result)
  }
}
