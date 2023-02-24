import akka.Done
import java.io.File
import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Framing, Sink, Source}
import akka.util.ByteString

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

object HumiditySensorStatistics {

  case class HumidityData(sum: Double, count: Int, min: Double, max: Double) {
    def avg: Option[Double] = if (count > 0) Some(sum / count) else None
  }

  case class SensorStats(min: Option[Double], avg: Option[Double], max: Option[Double])

  def main(args: Array[String]): Unit = {
    val directoryPath = args(0)

    implicit val system: ActorSystem = ActorSystem("HumiditySensorStatistics")
    val sensors = mutable.Map[String, HumidityData]()
    var failedMeasurements = 0

    println(s"Looking for CSV files in directory: $directoryPath")

    val fileSource = Source.fromIterator(() => new File(directoryPath)// accessing the .csv files
      .listFiles((_, name) => name.endsWith(".csv")).iterator)

    val files = new File(directoryPath).listFiles
    val numFiles = files.length

    val countFiles: Future[Int] = fileSource
      .runWith(Sink.fold(0)((count, _) => count + 1))

    val measurementSource = {
      fileSource.flatMapConcat(f => FileIO.fromPath(f.toPath)) //reading in the contents of the file using FileIO.fromPath.
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true))
        // apply a Framing stage that splits the incoming ByteString elements into frames separated by newlines (\n).
        .drop(1) // skip header lines
        .map(_.utf8String)// ByteString elements to strings
        .map(line => {
          val fields = line.split(",")// splits it into fields using a comma as the delimiter
          (fields(0), fields(1)) // returns a tuple of the first two fields. (ID and humidity)
        })
      //The resulting stream of tuples is used to update the sensors mutable map.
    }

    //This creates a sink, which is a place where data will end up after being processed by the stream.
    val sink = Sink.foreach[(String, String)](data => {//foreach function that takes a tuple of two strings as input.
      val sensorId = data._1
      val humidity = data._2.toDoubleOption //checks if the humidity value is a valid double using the toDoubleOption method.

        humidity match {
        case Some(h) =>
          sensors.put(sensorId, sensors.getOrElse(sensorId, HumidityData(0.0, 0, Int.MaxValue, 0.0)) match {
            case HumidityData(sum, count, min, max) =>
              HumidityData(sum + h, count + 1, Math.min(h, min), Math.max(h, max))
          })//it updates the sensors map with the new humidity data for the corresponding sensor ID.

          //println(s"Added humidity reading $humidity for sensor $sensorId.
                  //Current data: ${sensors(sensorId)}, keys: ${sensors.keys}")

        case None =>// If the humidity value is not valid, increments the failedMeasurements counter.
        failedMeasurements += 1
      }
    })

    //val x: NotUsed = sink.runWith(measurementSource)
    val sensorStats: Future[Done] = measurementSource.runWith(sink)//need sink MAT value that's why we kept in on right
    sensorStats.onComplete(_ => {
      val numSensorsProcessed = sensors.size
      val numMeasurementsProcessed = sensors.values.map(_.count).sum
      val numFailedMeasurements = failedMeasurements

      println(s"Num of processed files: $numFiles")
      println(s"Num of processed sensors: $numSensorsProcessed")
      println(s"Num of processed measurements: $numMeasurementsProcessed")
      println(s"Num of failed measurements: $numFailedMeasurements")

      val statsBySensor = sensors.map {
        case (sensorId, humidityData) =>
          val stats = SensorStats(
            min = Some(humidityData.min),
            avg = humidityData.avg,
            max = Some(humidityData.max)
          )
          println(stats, "STATS")
          (sensorId, stats)
      }

      println("Sensors with highest avg humidity:")
      println("sensor-id,min,avg,max")
      statsBySensor.toList.sortBy(_._2.avg).reverse.foreach {
        case (sensorId, stats) =>
          println(s"$sensorId,${stats.min.getOrElse("X")},${stats.avg.getOrElse("X")},${stats.max.getOrElse("X")}")
      }

      system.terminate()
    })
    Await.ready(sensorStats, 2000.millis)

  }
}
