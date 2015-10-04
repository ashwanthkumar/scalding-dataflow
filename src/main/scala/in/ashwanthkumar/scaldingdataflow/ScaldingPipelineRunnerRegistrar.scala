package in.ashwanthkumar.scaldingdataflow

import com.google.cloud.dataflow.sdk.runners.{PipelineRunner, PipelineRunnerRegistrar}
import com.google.common.collect.ImmutableList

class ScaldingPipelineRunnerRegistrar extends PipelineRunnerRegistrar{
  override def getPipelineRunners = ImmutableList.of[Class[_ <: PipelineRunner[_]]](classOf[ScaldingPipelineRunner])
}
