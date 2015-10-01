package in.ashwanthkumar.scaldingdataflow

import com.google.cloud.dataflow.sdk.PipelineResult.State
import com.google.cloud.dataflow.sdk.runners.{TransformTreeNode, AggregatorValues, PipelineRunner}
import com.google.cloud.dataflow.sdk.transforms.{PTransform, Aggregator}
import com.google.cloud.dataflow.sdk.values.PValue
import com.google.cloud.dataflow.sdk.{Pipeline, PipelineResult}
import org.slf4j.LoggerFactory

class ScaldingResult extends PipelineResult {
  override def getAggregatorValues[T](aggregator: Aggregator[_, T]): AggregatorValues[T] = ???
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

class LocalScaldingRunner(name: String) extends PipelineRunner[ScaldingResult] {
  private val LOG = LoggerFactory.getLogger(classOf[LocalScaldingRunner])

  override def run(pipeline: Pipeline): ScaldingResult = {
    println(pipeline)
    pipeline.traverseTopologically(new Evaluator(SContext.local("init")))
    new ScaldingResult
  }
}

object ScaldingRunner {
  def local(name: String) = new LocalScaldingRunner(name)
}
