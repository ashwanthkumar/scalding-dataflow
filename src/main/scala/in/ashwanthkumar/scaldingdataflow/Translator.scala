package in.ashwanthkumar.scaldingdataflow

import com.google.cloud.dataflow.sdk.io.TextIO.Read.{Bound => RBound}
import com.google.cloud.dataflow.sdk.io.TextIO.Write.{Bound => WBound}
import com.google.cloud.dataflow.sdk.transforms.{DoFn, PTransform, ParDo}
import com.twitter.scalding._
import scala.collection.JavaConverters._

object Translator {

  def readText[T]() = new TransformEvaluator[RBound[T]] {
    override def evaluate(transform: RBound[T], ctx: SContext) = {
      ctx.apply(
        TextLine(transform.getFilepattern)
          .read(ctx.flowDef, ctx.mode)
          .name(transform.getName)
          .project('line)
          .rename('line -> 'record)
      )
    }
  }

  def writeText[T]() = new TransformEvaluator[WBound[T]] {
    override def evaluate(transform: WBound[T], ctx: SContext) = {
      ctx.apply(
        ctx.pipe
          .name(transform.getName)
          .write(TextLine(transform.getFilenamePrefix))(ctx.flowDef, ctx.mode)
      )
    }
  }

  def flatMap[I, O]() = new TransformEvaluator[ParDo.Bound[I, O]] {
    override def evaluate(transform: ParDo.Bound[I, O], ctx: SContext): SContext = ctx.apply(ctx.pipe.flatMapTo('record -> 'record) {
      input: I => {
        val context  = new ProcessContext[I, O](transform.getFn, input)
        transform.getFn.startBundle(context.asInstanceOf[DoFn[I, O]#Context])
        transform.getFn.processElement(context.asInstanceOf[DoFn[I, O]#ProcessContext])
        transform.getFn.finishBundle(context.asInstanceOf[DoFn[I, O]#Context])
        context.getOutput.asScala
      }
    })
  }

  private val EVALUATORS: Map[Class[_ <: PTransform[_, _]], TransformEvaluator[_]] = Map(
    classOf[RBound[_]] -> readText(),
    classOf[WBound[_]] -> writeText(),
    classOf[ParDo.Bound[_, _]] -> flatMap()
  )

  def has[PT <: PTransform[_, _]](clazz: Class[PT]) = EVALUATORS.contains(clazz)
  def get[PT <: PTransform[_, _]](clazz: Class[PT]) = EVALUATORS(clazz)
}
