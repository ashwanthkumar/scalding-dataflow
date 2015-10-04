package in.ashwanthkumar.scaldingdataflow

import cascading.flow.FlowDef
import com.google.cloud.dataflow.sdk.PipelineResult.State
import com.google.cloud.dataflow.sdk.options.PipelineOptions
import com.google.cloud.dataflow.sdk.runners.{AggregatorValues, PipelineRunner, TransformTreeNode}
import com.google.cloud.dataflow.sdk.transforms.{Aggregator, PTransform}
import com.google.cloud.dataflow.sdk.values.PValue
import com.google.cloud.dataflow.sdk.{Pipeline, PipelineResult}
import com.twitter.scalding.{Args, Job, Mode}
import org.slf4j.LoggerFactory

class ScaldingResult extends PipelineResult {
  override def getAggregatorValues[T](aggregator: Aggregator[_, T]): AggregatorValues[T] = ???
  // We're always blocking
  override def getState: State = State.DONE
}

class Evaluator(var ctx: SContext) extends Pipeline.PipelineVisitor {
  override def visitTransform(node: TransformTreeNode): Unit = {
    val transform: PTransform[_, _] = node.getTransform
    val evalutor: TransformEvaluator[PTransform[_, _]] = Translator.get(transform.getClass).asInstanceOf[TransformEvaluator[PTransform[_, _]]]
    ctx = evalutor.evaluate(transform, ctx)
  }
  override def leaveCompositeTransform(node: TransformTreeNode): Unit = {}
  override def enterCompositeTransform(node: TransformTreeNode): Unit = {
    if (node.getTransform != null && !Translator.has(node.getTransform.getClass)) return
  }
  override def visitValue(value: PValue, producer: TransformTreeNode): Unit = {}
}

class ScaldingPipelineRunner(options: ScaldingPipelineOptions) extends PipelineRunner[ScaldingResult] {
  private val LOG = LoggerFactory.getLogger(classOf[ScaldingPipelineRunner])

  override def run(pipeline: Pipeline): ScaldingResult = {

    val ctx = options.getMode match {
      case "local" => SContext.local(options.getName)
      case "hdfs" => SContext.hdfs(options.getName)
    }

    pipeline.traverseTopologically(new Evaluator(ctx))

    new Job(Args(Iterable())) {
      override implicit def mode: Mode = ctx.mode
      override protected implicit val flowDef: FlowDef = ctx.flowDef
      override def name: String = ctx.name
    }.run

    new ScaldingResult
  }
}

object ScaldingPipelineRunner {
  def fromOptions(options: PipelineOptions) = new ScaldingPipelineRunner(options.as(classOf[ScaldingPipelineOptions]))
}
