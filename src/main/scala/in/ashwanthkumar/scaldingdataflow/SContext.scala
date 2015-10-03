package in.ashwanthkumar.scaldingdataflow

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import com.twitter.scalding.{Hdfs, Local, Mode, Test}
import org.apache.hadoop.conf.Configuration

case class SContext(pipe: Pipe, flowDef: FlowDef, mode: Mode, name: String) {
  def apply(transformed: Pipe) = this.copy(pipe = transformed)
}

object SContext {
  def local(name: String) = SContext(new Pipe(name), FlowDef.flowDef(), Local(false), name)
  def test(name: String) = SContext(new Pipe(name), FlowDef.flowDef(),
    // FIXME - Need to build wrapeers like in JobTest, btw this is horrible :(
    Test({ _ => None }), name)

  def hdfs(name: String) = SContext(new Pipe(name), FlowDef.flowDef(), Hdfs(strict = false, new Configuration), name)
}