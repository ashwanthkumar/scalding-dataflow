package in.ashwanthkumar.scaldingdataflow

import java.io.Serializable

import com.google.cloud.dataflow.sdk.transforms.PTransform

trait TransformEvaluator[PT <: PTransform[_, _]] extends Serializable {
  def evaluate(transform: PT, ctx: SContext): SContext
}
