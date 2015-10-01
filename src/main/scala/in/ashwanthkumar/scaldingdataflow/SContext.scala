package in.ashwanthkumar.scaldingdataflow

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import com.twitter.scalding.{Hdfs, Local, Mode, Test}
import org.apache.hadoop.conf.Configuration

case class SContext(pipe: Pipe,
                    // Needed to construct the Pipe in Scalding Job
                    flowDef: FlowDef, mode: Mode)

object SContext {
  def local(name: String) = SContext(new Pipe(name), FlowDef.flowDef(), Local(false))
  def test(name: String) = SContext(new Pipe(name), FlowDef.flowDef(),
    // FIXME - Need to build wrapeers like in JobTest, btw this is horrible :(
    Test({ _ => None }))

  def hdfs(name: String) = SContext(new Pipe(name), FlowDef.flowDef(), Hdfs(strict = false, new Configuration))
}