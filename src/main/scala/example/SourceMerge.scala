package example

import java.util.NoSuchElementException

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.{SCollection, SideInput, WindowOptions}
import com.typesafe.config.{Config, ConfigFactory}
import example.proto.message.NumberBuffer
import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner
import org.apache.beam.sdk.transforms.windowing.{AfterPane, IntervalWindow, Repeatedly}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.{DateTime, Duration, Instant}

import scala.collection.JavaConverters._


/**
 * Process Pub/Sub source and BigQuery table.
 *
 * @param sc                : ScioContext.
 * @param inputSubscription : Path to subscription.
 * @param numberInfoTable   : BigQuery table reference.
 * @param resultInfoTable   : Table to write results to.
 */
class SourceMerge(@transient val sc: ScioContext, inputSubscription: String,
                  numberInfoTable: String, resultInfoTable: String,
                  windowSeconds: Int, allowedLateness: Int) extends Serializable {

  import example.SourceMerge.NumberResults

  /**
   * Process data.
   */
  def processSources(): Unit = {
    val windowedInput = getWindowedStream

    val numberCount = windowedInput
          .transform("Determine counted values")(_.count)
          .asListSideInput // Sometimes and SCollection can contain more than a single item. Use a list to be sure.

    val aggregatedValues: SCollection[(Int, String)] = windowedInput
      .aggregateValues(numberInfoTable)

    aggregatedValues
      .withSideInputs(numberCount)
      .map { case ((numberSum, numberType), side) => (numberSum, numberType, side(numberCount).max) }
      .toSCollection
      .transformToTableRow
      .saveAsCustomOutput("Save aggregate to BQ",
        BigQueryIO
          .writeTableRows()
          .to(resultInfoTable)
          .withSchema(BigQueryType[NumberResults].schema)
          .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(WriteDisposition.WRITE_APPEND))
  }

  implicit class scSideInputProcessor(pipeline: SCollection[Int]) {
    /**
     * Add side input and aggregate values.
     *
     * @param numberInfoTable : number info table to use.
     * @return SCollection with time window and summed value.
     */
    def aggregateValues(numberInfoTable: String): SCollection[(Int, String)] = {
      val numberInfoSideInput = getNumberInfoSideInput(numberInfoTable)

      pipeline
        .transform("Sum values")(_.sum)
        .withSideInputs(numberInfoSideInput) // Since the right side is small, a side input can be used.
        .map {
          case (sum, side) =>
            val numberInfoMap = side(numberInfoSideInput)
            try {
              (sum,
                numberInfoMap.filter {
                  case (lowerBound: Long, upperBound: Long, _: String) =>
                    (lowerBound <= sum) && (upperBound >= sum)
                }
                  .map(x => x._3) // The number type: "hundreds", "thousands", etc.
                  .head)
            } catch {
              case _: NoSuchElementException => (sum, "unknown")
            }
        }
        .toSCollection
    }
  }

  implicit class scJoinProcessor(pipeline: SCollection[(Int, String, Long)]) {
    /**
     * Transform joined data into TableRow.
     *
     * @return SCollection of TableRows
     */
    def transformToTableRow: SCollection[TableRow] = {
      pipeline
        .withWindow[IntervalWindow]
        .map { case (sumDetails, window) => (window.end(), sumDetails) }
        .transform("Convert into TableRow")(
          _.map {
            case (timestamp, (numberSum, numberType, numberCount)) => TableRow(Map(
              "update_time" -> Timestamp(timestamp),
              "number_sum" -> numberSum,
              "number_count" -> numberCount,
              "number_type" -> numberType,
              "version" -> "run-2")
              .map(kv => (kv._1, kv._2))
              .toList: _*)
          })
    }
  }

  /**
   * Windowed stream with sum and window.
   *
   * @return Windowed SCollection with time window and sum.
   */
  def getWindowedStream: SCollection[Int] = getInputStream
    .transform("Timestamp")(
      _.timestampBy(x =>
        new DateTime(x.timestamp).toInstant, allowedTimestampSkew = Duration.standardSeconds(windowSeconds)))
    .transform("Apply window functions")(
      _.withFixedWindows(Duration.standardSeconds(windowSeconds),
        options = WindowOptions(
          timestampCombiner = TimestampCombiner.LATEST,
          accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
          trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
          allowedLateness = Duration.standardSeconds(allowedLateness)))
        .map(_.number))

  /**
   * Load parsed stream of messages from Google Cloud Pub/Sub.
   *
   * @return SCollection of parsed messages.
   */
  def getInputStream: SCollection[NumberBuffer] = sc
    .pubsubSubscription[Array[Byte]](inputSubscription)
    .transform("Parse objects")(
      _.map(NumberBuffer.parseFrom))

  /**
   * Load typed BigQuery table which contains information about the numbers.
   *
   * @param tableSpec : Table name to load.
   * @return Side input
   */
  def getNumberInfoSideInput(tableSpec: String): SideInput[Seq[(Long, Long, String)]] = sc
    .customInput(s"Read number info",
      BigQueryIO
        .readTableRows()
        .from(tableSpec))
    .map(x => (x.getLong("lower_bound"), x.getLong("upper_bound"), x.getString("number_type")))
    .asListSideInput
}

object SourceMerge {
  val config: Config = ConfigFactory.load()
  val projectId: String = config.getString("pipeline.project_id")
  val bucketPath: String = config.getString("pipeline.bucket")
  val inputSubscription: String = config.getString("pipeline.subscription_input")
  val region: String = config.getString("pipeline.region")
  val numberInfo: String = config.getString("pipeline.number_info_table")
  val resultInfoTable: String = config.getString("pipeline.result_table")
  val windowSeconds: Int = config.getInt("pipeline.window_seconds")
  val allowedLateness: Int = config.getInt("pipeline.allowed_lateness_seconds")

  /**
   * Main method
   *
   * @param cmdlineArgs : Command-line arguments
   */
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, _) = ContextAndArgs(cmdlineArgs ++ Array(
      s"--stagingLocation=$bucketPath/staging",
      s"--tempLocation=$bucketPath/temp/"))

    sc.options.setJobName("example-etl-" + (DateTime.now().toString("YYYY-MM-dd-HHmmss")))
    sc.optionsAs[DataflowPipelineOptions].setProject(projectId)
    sc.optionsAs[DataflowPipelineOptions].setRegion(region)
    sc.optionsAs[DataflowPipelineOptions].setWorkerMachineType("n1-standard-2")
    sc.optionsAs[DataflowPipelineOptions].setExperiments(List("flexRSGoal=OPTIMIZED").asJava)
    sc.optionsAs[DataflowPipelineOptions].setNumWorkers(1)
    sc.optionsAs[DataflowPipelineOptions].setStreaming(true)

    sc.options.setRunner(classOf[DataflowRunner])

    val sm = new SourceMerge(sc, inputSubscription, numberInfo, resultInfoTable,
      windowSeconds = windowSeconds, allowedLateness = allowedLateness)

    sm.processSources()

    sc.close().waitUntilFinish()
  }

  @BigQueryType.toTable
  case class NumberResults(update_time: Instant, number_sum: Int,
                           number_count: Int, number_type: String, version: String)

}
