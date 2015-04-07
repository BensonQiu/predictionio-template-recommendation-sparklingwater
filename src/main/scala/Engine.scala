package org.template.recommendation

import io.prediction.controller.IEngineFactory
import io.prediction.controller.Engine

case class Query(
  circuit_id: Int,
  time: String
) extends Serializable

case class PredictedResult(
  energy: Double
) extends Serializable

object RecommendationEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("als" -> classOf[Algorithm]),
      classOf[Serving])
  }
}
