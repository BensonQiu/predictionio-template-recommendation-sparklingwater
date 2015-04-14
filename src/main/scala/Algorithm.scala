package org.template.recommendation

import io.prediction.controller.P2LAlgorithm
import io.prediction.controller.Params
import io.prediction.controller.IPersistentModel

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.spark.h2o._
import org.apache.spark.sql.{SQLContext, SchemaRDD}
import hex.deeplearning._
import hex.deeplearning.DeepLearningModel.DeepLearningParameters

import grizzled.slf4j.Logger

case class AlgorithmParams(
  epochs: Int
) extends Params

class Algorithm(val ap: AlgorithmParams)
  extends P2LAlgorithm[PreparedData, Model, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): Model = {
    val electricalLoads : RDD[ElectricalLoad] = data.electricalLoads

    val h2oContext = new H2OContext(sc).start()
    import h2oContext._

    val result = createDataFrame(data.electricalLoads)

    val dlParams: DeepLearningParameters = new DeepLearningParameters()
    dlParams._train = result('circuitId, 'time, 'energy)
    dlParams._response_column = 'energy
    dlParams._epochs = ap.epochs

    val dl: DeepLearning = new DeepLearning(dlParams)
    val dlModel: DeepLearningModel = dl.trainModel.get

    new Model(h2oContext = h2oContext,
              dlModel = dlModel,
              sc = sc
             )
  }

  def predict(model: Model, query: Query): PredictedResult = {
    import model.h2oContext._
  
    val inputQuery = Seq(Input(query.circuit_id,query.time.toInt))
    val inputDF = createDataFrame(model.sc.parallelize(inputQuery))
    val predictionH2OFrame = model.dlModel.score(inputDF)('predict)
    val predictionsFromModel =
      toRDD[DoubleHolder](predictionH2OFrame).
      map ( _.result.getOrElse(Double.NaN) ).collect
    
    new PredictedResult(energy = predictionsFromModel(0))
  }
}

case class Input(circuitId: Int, time: Int)

class Model (
  val h2oContext: H2OContext,
  val dlModel: DeepLearningModel,
  val sc: SparkContext
) extends IPersistentModel[Params] with Serializable {
  def save(id: String, params: Params, sc: SparkContext): Boolean = {
    false
  }
}
