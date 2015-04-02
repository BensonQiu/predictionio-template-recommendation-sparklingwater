package org.template.recommendation

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.storage.Storage

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.spark.h2o._

import grizzled.slf4j.Logger

case class DataSourceParams(appId: Int) extends Params
 
class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {
    val eventsDb = Storage.getPEvents()
    val eventsRDD: RDD[Event] = eventsDb.find(
      appId = dsp.appId,
      entityType = Some("electric_load"),
      eventNames = Some(List("predict_energy")))(sc)

    val electricalLoadRDD: RDD[ElectricalLoad] = eventsRDD.map { event =>
      val electricalLoad: ElectricalLoad = 
        event.event match {
          case "predict_energy" => 
            val circuitId = event.properties.get[Int]("circuitId")
            val timeArray: Seq[Int] = 
              event.properties.get[Seq[String]]("timeArray").map { time => time.toInt }
            val energyArray: Seq[Double] = 
              event.properties.get[Seq[String]]("energyArray").map { energy => energy.toDouble }

            ElectricalLoad(event.properties.get[Int]("circuitId"),
                           time = timeArray.head,
                           energy = energyArray.head
                          )
          case _ => throw new Exception(s"Unexpected event ${event} is read.")
        }
        electricalLoad
    }.cache()

    new TrainingData(electricalLoadRDD)
  }
}

case class ElectricalLoad(
  circuitId: Int,
  time: Int,
  energy: Double
)

class TrainingData(
  val electricalLoads: RDD[ElectricalLoad]
) extends Serializable /* {
  override def toString = {
    s"electricalLoads: [${electricalLoads.count()}] (${electricalLoads.take(2).toList}...)"
  }
} */
