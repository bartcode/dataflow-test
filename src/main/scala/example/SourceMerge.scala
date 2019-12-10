package example

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.WindowOptions
import example.objects.NumberInfo
import example.proto.message.NumberBuffer
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner.LATEST
import org.apache.beam.sdk.transforms.windowing.{AfterPane, IntervalWindow, Repeatedly}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.{Duration, Instant}

import scala.collection.JavaConverters._


class SourceMerge(@transient val sc: ScioContext) extends Serializable {

  import example.SourceMerge.{NumberInfoRow, NumberResults}

  def processSources(autoSubscription: String, manualSubscription: String,
                     numberInfo: String, outputTopic: String): Unit = {
    val autoInput = sc.pubsubSubscription[Array[Byte]](autoSubscription)
    val manualInput = sc.pubsubSubscription[Array[Byte]](manualSubscription)

    val numberInfoTable = sc.typedBigQuery[NumberInfoRow]()
      .map(x => (x.lower_bound.get, x.upper_bound.get, x.number_type.get))
      .asListSideInput

    val intermediateSums = autoInput
      .union(manualInput)
      .transform("Parse objects")(_
        .map(x => NumberInfo(NumberBuffer.parseFrom(x)))
        .timestampBy(_.timestamp.get, allowedTimestampSkew = Duration.standardSeconds(5))
      )
      .transform("Apply window functions")(
        _.withFixedWindows(Duration.standardSeconds(5),
          options = WindowOptions(
            timestampCombiner = LATEST,
            accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
            trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
            allowedLateness = Duration.standardSeconds(5)))
          .map(_.number)
          .withWindow[IntervalWindow]
          .map { case (score, window) => (window.end(), score.get) }
      )

    val sumCounts = intermediateSums
      .transform("Determine counted values")(
        _.countByKey)

    intermediateSums
      .transform("Sum values")(
        _.sumByKey)
      .transform("Extract values")(
        _.map(x => (x._1, x._2))) // Number length, (timestamp, sum)
      .withSideInputs(numberInfoTable)
      .map {
        case (score, side) =>
          val numberInfoMap = side(numberInfoTable)
          (
            score,
            numberInfoMap
              .filter {
                case (lowerBound: Long, upperBound: Long, _: String) => (lowerBound <= score._2) && (upperBound >= score._2)
              }
              .map(x => x._3)
              .head
          )
      }
      .toSCollection
      .transform("Combine with counts")(
        _.map(x => (x._1._1, (x._1._2, x._2)))
          .hashLeftJoin(sumCounts))
      .transform("Tuple values")(
        _.map(x => (x._1, x._2._1._1, x._2._1._2.toString, x._2._2.getOrElse(1.toLong))))
      .transform("Convert into TableRow")(
        _.map(x =>
          TableRow(Map(
            "update_time" -> Timestamp(x._1),
            "number_sum" -> x._2,
            "number_count" -> x._4,
            "number_type" -> x._3,
            "version" -> "run-1")
            .map(kv => (kv._1, kv._2))
            .toList: _*)))
      .saveAsCustomOutput(
        "Save aggregate to BQ",
        BigQueryIO
          .writeTableRows()
          .to("playground-bart:playground.results")
          .withSchema(BigQueryType[NumberResults].schema)
          .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(WriteDisposition.WRITE_APPEND))

  }
}

object SourceMerge {
  val projectName = "playground-bart"
  val bucketPath = "gs://playground-bart"

  def main(cmdlineArgs: Array[String]): Unit = {
    val subscriptions = Map(
      "auto" -> s"projects/$projectName/subscriptions/sensor-auto",
      "manual" -> s"projects/$projectName/subscriptions/sensor-manual",
    )

    val numberInfo = s"$projectName:playground.numbers"
    //    val numberResults = s"$projectName:playground.results"

    val (sc, args) = ContextAndArgs(cmdlineArgs ++ Array(
      s"--stagingLocation=$bucketPath/staging",
      s"--tempLocation=$bucketPath/temp/"))

    sc.options.setJobName("example-etl")
    sc.optionsAs[DataflowPipelineOptions].setProject("playground-bart")
    sc.optionsAs[DataflowPipelineOptions].setRegion("europe-west4")
    sc.optionsAs[DataflowPipelineOptions].setExperiments(List("shuffle_mode=service").asJava)
    sc.optionsAs[DataflowPipelineOptions].setNumWorkers(1)
    sc.optionsAs[DataflowPipelineOptions].setStreaming(true)

    val sm = new SourceMerge(sc)

    sm.processSources(
      autoSubscription = subscriptions("auto"),
      manualSubscription = subscriptions("manual"),
      numberInfo = numberInfo,
      outputTopic = s"projects/$projectName/topics/outputs")

    sc.close().waitUntilFinish()
  }

  @BigQueryType.toTable
  case class NumberResults(update_time: Instant, number_sum: Int, number_count: Int, number_type: String, version: String)

  @BigQueryType.fromTable("playground-bart:playground.numbers")
  class NumberInfoRow

}
