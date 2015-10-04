package in.ashwanthkumar.scaldingdataflow

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform
import com.google.cloud.dataflow.sdk.values.PValue
import com.twitter.scalding.{Hdfs, Local, Mode, Test}
import org.apache.hadoop.conf.Configuration

case class SPipe(pipe: Pipe) {
  def apply(transformed: Pipe => Pipe) = this.copy(pipe = transformed(pipe))
}

case class SContext(pipes: Map[PValue, SPipe], flowDef: FlowDef, mode: Mode, name: String, source: Option[PValue] = None) {
  def apply(pValue: PValue, source: Option[PValue], transformed: SPipe) = {
    this.copy(pipes = this.pipes ++ Map(pValue -> transformed), source = source)
  }

  def addInput(appliedTransform: AppliedPTransform[_, _, _], transformed: SPipe) = {
    apply(getInput(appliedTransform), this.source, transformed)
  }

  def addOutput(appliedTransform: AppliedPTransform[_, _, _], transformed: SPipe) = {
    val pipeStartMarker = getOutput(appliedTransform)
    apply(pipeStartMarker, Some(pipeStartMarker), transformed)
  }

  def lastPipe = pipes.apply(this.source.get) // we should fail if we don't have a start pipe yet

  private def getInput(applied: AppliedPTransform[_, _, _]): PValue = applied.getInput.asInstanceOf[PValue]
  private def getOutput(applied: AppliedPTransform[_, _, _]) = applied.getOutput.asInstanceOf[PValue]
}

object SContext {
  def local(name: String) = SContext(Map(), FlowDef.flowDef(), Local(false), name)
  def test(name: String) = SContext(Map(), FlowDef.flowDef(),
    // FIXME - Need to build wrapeers like in JobTest, btw this is horrible :(
    Test({ _ => None }), name)

  def hdfs(name: String) = SContext(Map(), FlowDef.flowDef(), Hdfs(strict = false, new Configuration), name)
}