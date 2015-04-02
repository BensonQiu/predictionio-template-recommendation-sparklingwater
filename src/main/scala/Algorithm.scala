package org.template.recommendation

import io.prediction.controller.P2LAlgorithm
import io.prediction.controller.Params
import io.prediction.data.storage.BiMap

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.spark.h2o._
import org.apache.spark.sql.{SQLContext, SchemaRDD}
import hex.deeplearning._
import hex.deeplearning.DeepLearningModel.DeepLearningParameters

import grizzled.slf4j.Logger

case class AlgorithmParams(
  rank: Int,
  numIterations: Int,
  lambda: Double,
  seed: Option[Long]) extends Params

class Algorithm(val ap: AlgorithmParams)
  extends P2LAlgorithm[PreparedData, Model, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(data: PreparedData): Model = {
    val electricalLoads : RDD[ElectricalLoad] = data.electricalLoads

    val h2oContext = new H2OContext(electricalLoads.context).start()
    import h2oContext._

    val sqlContext = new SQLContext(electricalLoads.context)
    import sqlContext._
    electricalLoads.registerTempTable("electricalLoads")
    val result: SchemaRDD = sql("SELECT * FROM electricalLoads")

    val dlParams: DeepLearningParameters = new DeepLearningParameters()
    dlParams._train = result('time, 'energy)
    dlParams._response_column = 'energy
    val dl: DeepLearning = new DeepLearning(dlParams)
    val dlModel: DeepLearningModel = dl.trainModel.get
     
    new Model(count = result.count.toInt
    /*
              h2oContext = h2oContext,
              result = result,
                dlModel = dlModel
                */
             )
  }

  def predict(model: Model, query: Query): PredictedResult = {
    /*
    import model.h2oContext._

    val result = model.result
    val dlModel = model.dlModel
    val predictionH2OFrame = dlModel.score(result)('predict)
    val predictionsFromModel = 
      toRDD[DoubleHolder](predictionH2OFrame).
      map ( _.result.getOrElse(Double.NaN) ).collect

    new PredictedResult(energy = model.count)
    */

    new PredictedResult(energy = -1)

  }
}

class Model (
  val count: Int
  /*
  val h2oContext: H2OContext,
  val result: SchemaRDD,
  val dlModel: DeepLearningModel
  */
) extends Serializable
