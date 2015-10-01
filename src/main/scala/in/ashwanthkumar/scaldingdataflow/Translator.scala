package in.ashwanthkumar.scaldingdataflow

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.google.cloud.dataflow.sdk.io.TextIO.Read.{Bound => RBound}
import com.google.cloud.dataflow.sdk.io.TextIO.Write.{Bound => WBound}
import com.google.cloud.dataflow.sdk.transforms.PTransform
import com.twitter.scalding._

object Translator {

  def readText[T]() = new TransformEvaluator[RBound[T]] {
    override def evaluate(transform: RBound[T], ctx: SContext) = {
      ctx.copy(pipe = TextLine(transform.getFilepattern).read(ctx.flowDef, ctx.mode).project(new Fields("line")))
    }

    implicit def pipeToRichPipe(pipe: Pipe): RichPipe = RichPipe(pipe)
  }

  def writeText[T]() = new TransformEvaluator[WBound[T]] {
    override def evaluate(transform: WBound[T], ctx: SContext) = {
      RichPipe(ctx.pipe).write(TextLine(transform.getFilenamePrefix))(ctx.flowDef, ctx.mode)
      new Job(Args("")) {
        override implicit def mode: Mode = ctx.mode
        override protected implicit val flowDef: FlowDef = ctx.flowDef
      }.run
      ctx
    }
  }

  private val EVALUATORS: Map[Class[_ <: PTransform[_, _]], TransformEvaluator[_]] = Map(
    classOf[RBound[_]] -> readText(),
    classOf[WBound[_]] -> writeText()
  )

  def has[PT <: PTransform[_, _]](clazz: Class[PT]) = EVALUATORS.contains(clazz)
  def get[PT <: PTransform[_, _]](clazz: Class[PT]) = EVALUATORS(clazz)
}
