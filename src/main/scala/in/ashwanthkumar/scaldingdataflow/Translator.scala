package in.ashwanthkumar.scaldingdataflow

import com.google.cloud.dataflow.sdk.io.TextIO.Read.{Bound => RBound}
import com.google.cloud.dataflow.sdk.io.TextIO.Write.{Bound => WBound}
import com.google.cloud.dataflow.sdk.transforms.{AppliedPTransform, DoFn, PTransform, ParDo}
import com.twitter.scalding._

import scala.collection.JavaConverters._

object Translator {

  def readText[T]() = new TransformEvaluator[RBound[T]] {
    override def evaluate(appliedPTransform: AppliedPTransform[_, _, RBound[T]], transform: RBound[T], ctx: SContext) = {
      ctx.addOutput(appliedPTransform,
        SPipe(TextLine(transform.getFilepattern)
          .read(ctx.flowDef, ctx.mode)
          .name(transform.getName)
          .project('line)
          .rename('line -> 'record)
        )
      )
    }
  }

  def writeText[T]() = new TransformEvaluator[WBound[T]] {
    override def evaluate(appliedPTransform: AppliedPTransform[_, _, WBound[T]], transform: WBound[T], ctx: SContext) = {
      ctx.addInput(appliedPTransform,
        ctx.lastPipe(_.name(transform.getName)
          .write(TextLine(transform.getFilenamePrefix))(ctx.flowDef, ctx.mode)
        )
      )
    }
  }

  def flatMap[I, O]() = new TransformEvaluator[ParDo.Bound[I, O]] {
    override def evaluate(appliedPTransform: AppliedPTransform[_, _, ParDo.Bound[I, O]], transform: ParDo.Bound[I, O], ctx: SContext): SContext = {
      ctx.addInput(appliedPTransform,
        ctx.lastPipe(_.flatMapTo('record -> 'record) {
          input: I => {
            val context = new ProcessContext[I, O](transform.getFn, input)
            transform.getFn.startBundle(context.asInstanceOf[DoFn[I, O]#Context])
            transform.getFn.processElement(context.asInstanceOf[DoFn[I, O]#ProcessContext])
            transform.getFn.finishBundle(context.asInstanceOf[DoFn[I, O]#Context])
            context.getOutput.asScala
          }
        })
      )
    }
  }

  private val EVALUATORS: Map[Class[_ <: PTransform[_, _]], TransformEvaluator[_]] = Map(
    classOf[RBound[_]] -> readText(),
    classOf[WBound[_]] -> writeText(),
    classOf[ParDo.Bound[_, _]] -> flatMap()
  )

  def has[PT <: PTransform[_, _]](clazz: Class[PT]) = EVALUATORS.contains(clazz)
  def get[PT <: PTransform[_, _]](clazz: Class[PT]) = EVALUATORS(clazz)
}
