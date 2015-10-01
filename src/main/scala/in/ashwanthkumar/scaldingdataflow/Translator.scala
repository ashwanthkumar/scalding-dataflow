package in.ashwanthkumar.scaldingdataflow

import com.google.cloud.dataflow.sdk.io.TextIO.Read.{Bound => RBound}
import com.google.cloud.dataflow.sdk.io.TextIO.Write.{Bound => WBound}
import com.google.cloud.dataflow.sdk.transforms.PTransform
import com.twitter.scalding._

object Translator {

  def readText[T]() = new TransformEvaluator[RBound[T]] {
    override def evaluate(transform: RBound[T], ctx: SContext) = {
      ctx.apply(TextLine(transform.getFilepattern).read(ctx.flowDef, ctx.mode).name(transform.getName).project('line))
    }
  }

  def writeText[T]() = new TransformEvaluator[WBound[T]] {
    override def evaluate(transform: WBound[T], ctx: SContext) = {
      ctx.apply(ctx.pipe.name(transform.getName).write(TextLine(transform.getFilenamePrefix))(ctx.flowDef, ctx.mode))
    }
  }

  private val EVALUATORS: Map[Class[_ <: PTransform[_, _]], TransformEvaluator[_]] = Map(
    classOf[RBound[_]] -> readText(),
    classOf[WBound[_]] -> writeText()
  )

  def has[PT <: PTransform[_, _]](clazz: Class[PT]) = EVALUATORS.contains(clazz)
  def get[PT <: PTransform[_, _]](clazz: Class[PT]) = EVALUATORS(clazz)
}
