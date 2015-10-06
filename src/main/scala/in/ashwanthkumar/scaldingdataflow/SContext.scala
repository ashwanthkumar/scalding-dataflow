package in.ashwanthkumar.scaldingdataflow

import java.lang.{Iterable => JIterable}

import cascading.flow.FlowDef
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.cloud.dataflow.sdk.options.PipelineOptions
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform
import com.google.cloud.dataflow.sdk.util.WindowedValue
import com.google.cloud.dataflow.sdk.values.PValue
import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding.{Hdfs, Local, Mode}
import org.apache.hadoop.conf.Configuration


case class SPipe(@transient pipe: TypedPipe[_]) {
  def apply[I, O](transformed: TypedPipe[I] => TypedPipe[O]) = this.copy(pipe = transformed(pipe.asInstanceOf[TypedPipe[I]]))

  def ++(another: SPipe) = SPipe(pipe = this.pipe ++ another.pipe)

  def as[T] = pipe.asInstanceOf[TypedPipe[T]]
}

object SPipe {
  val DEFAULT = SPipe(TypedPipe.empty)
  def apply[T](iterable: Iterable[T]): SPipe = SPipe(TypedPipe.from[T](iterable))
}

case class SContext(pipes: Map[PValue, SPipe],
                    @transient flowDef: FlowDef,
                    mode: Mode,
                    name: String,
                    pipelineOptionsInJson: String,
                    views: Map[PValue, JIterable[WindowedValue[_]]] = Map()) {
  def apply(pValue: PValue, transformed: SPipe) = {
    // println("Adding " + pValue + " to known maps")
    this.copy(pipes = this.pipes ++ Map(pValue -> transformed))
  }

  def addInput(appliedTransform: AppliedPTransform[_, _, _], transformed: SPipe) = {
    apply(getOutput[PValue](appliedTransform), transformed)
  }

  def addOutput(appliedTransform: AppliedPTransform[_, _, _], transformed: SPipe) = {
    apply(getOutput[PValue](appliedTransform), transformed)
  }

  def addToView(value: PValue, iterable: JIterable[WindowedValue[_]]) = {
    this.copy(views = views ++ Map(value -> iterable))
  }

  def fromView(value: PValue): JIterable[WindowedValue[_]] = views.apply(value)


  def addSink(appliedTransform: AppliedPTransform[_, _, _], transformed: SPipe) = {
    // PDone can't be cast as PValue
    apply(getInput[PValue](appliedTransform), transformed)
  }

  def lastPipe(value: PValue) = {
//    println("Looking for " + value + " and isFound=" + pipes.contains(value))
    pipes.getOrElse(value, SPipe.DEFAULT)
  }
  def getPipe(value: PValue) = pipes.apply(value)

  def getInput[T](applied: AppliedPTransform[_, _, _]) = applied.getInput.asInstanceOf[T]
  def getOutput[T](applied: AppliedPTransform[_, _, _]) = applied.getOutput.asInstanceOf[T]

  def pipelineOptions: PipelineOptions = JSON.fromJson(pipelineOptionsInJson, classOf[PipelineOptions])
}

object SContext {
  def local(name: String, options: PipelineOptions) = SContext(Map(), FlowDef.flowDef(), Local(false), name, JSON.toJson(options))
  def hdfs(name: String, options: PipelineOptions) = SContext(Map(), FlowDef.flowDef(), Hdfs(strict = false, new Configuration), name, JSON.toJson(options))
}

object JSON {
  private val mapper = new ObjectMapper()
  def toJson(o: Any) = mapper.writeValueAsString(o)
  def fromJson[T](json: String, clazz: Class[T]): T = mapper.readValue(json, clazz)
}
