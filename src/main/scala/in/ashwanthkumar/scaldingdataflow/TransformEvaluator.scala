package in.ashwanthkumar.scaldingdataflow

import java.io.Serializable

import cascading.pipe.Pipe
import com.google.cloud.dataflow.sdk.transforms.PTransform
import com.twitter.scalding.{FieldConversions, RichPipe}

import scala.language.implicitConversions

trait TransformEvaluator[PT <: PTransform[_, _]] extends Serializable with FieldConversions {
  def evaluate(transform: PT, ctx: SContext): SContext

  implicit def pipeToRichPipe(pipe: Pipe): RichPipe = RichPipe(pipe)
}
