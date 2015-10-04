package in.ashwanthkumar.scaldingdataflow

import java.lang.{Iterable => JIterable}
import java.util.{List => JList, Map => JMap}

import com.google.cloud.dataflow.sdk.io.TextIO.Read.{Bound => RBound}
import com.google.cloud.dataflow.sdk.io.TextIO.Write.{Bound => WBound}
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn
import com.google.cloud.dataflow.sdk.transforms.Flatten.FlattenPCollectionList
import com.google.cloud.dataflow.sdk.transforms.View.CreatePCollectionView
import com.google.cloud.dataflow.sdk.transforms._
import com.google.cloud.dataflow.sdk.util.WindowedValue
import com.google.cloud.dataflow.sdk.values._
import com.google.common.collect.Maps
import com.twitter.algebird.{Aggregator => TAggregator, Semigroup}
import com.twitter.scalding.typed._
import com.twitter.scalding.{TextLine, TypedTsv}
import in.ashwanthkumar.scaldingdataflow.scalding.ScaldingExecutor
import in.ashwanthkumar.scaldingdataflow.utils.FieldGetter

import scala.collection.JavaConverters._

object Translator {

  def readText[T]() = new TransformEvaluator[RBound[T]] {
    override def evaluate(appliedPTransform: AppliedPTransform[_, _, RBound[T]], transform: RBound[T], ctx: SContext) = {
      ctx.addInput(appliedPTransform,
        SPipe(TypedPipe.from[String](TextLine(transform.getFilepattern)))
      )
    }
  }

  def writeText[T: Manifest]() = new TransformEvaluator[WBound[T]] {
    override def evaluate(appliedPTransform: AppliedPTransform[_, _, WBound[T]], transform: WBound[T], ctx: SContext) = {
      val value = ctx.getInput[PValue](appliedPTransform)
      ctx.addSink(appliedPTransform,
        ctx.lastPipe(value).apply[T, T](_.write(TypedTsv[T](transform.getFilenamePrefix))(ctx.flowDef, ctx.mode))
      )
    }
  }

  def flatMap[I, O]() = new TransformEvaluator[ParDo.Bound[I, O]] {
    override def evaluate(appliedPTransform: AppliedPTransform[_, _, ParDo.Bound[I, O]], transform: ParDo.Bound[I, O], ctx: SContext): SContext = {
      val value = ctx.getInput[PValue](appliedPTransform)
      ctx.addOutput(appliedPTransform,
        ctx.lastPipe(value).apply[I, O](_.flatMap[O] { input: I =>
          val context = new ProcessContext[I, O](transform.getFn, input, sideInputs(transform.getSideInputs, ctx), ctx.pipelineOptions)
          transform.getFn.startBundle(context.asInstanceOf[DoFn[I, O]#Context])
          transform.getFn.processElement(context.asInstanceOf[DoFn[I, O]#ProcessContext])
          transform.getFn.finishBundle(context.asInstanceOf[DoFn[I, O]#Context])
          context.getOutput.asScala
        })
      )
    }

    def sideInputs(sideInputs: JList[PCollectionView[_]], ctx: SContext): JMap[TupleTag[_], JIterable[WindowedValue[_]]] = {
      val map = Maps.newHashMap[TupleTag[_], JIterable[WindowedValue[_]]]()
      if (sideInputs != null) {
        sideInputs.asScala
          .foreach(view => map.put(view.getTagInternal, ctx.fromView(view)))
      }
      map
    }
  }

  def flattenPColl[T]() = new TransformEvaluator[Flatten.FlattenPCollectionList[T]] {
    override def evaluate(appliedPTransform: AppliedPTransform[_, _, FlattenPCollectionList[T]], transform: FlattenPCollectionList[T], ctx: SContext): SContext = {
      val collections = ctx.getInput[PCollectionList[T]](appliedPTransform)
      val head = collections.expand().asScala.head

      val flattened = collections.expand().asScala.drop(1)
        .map(ctx.lastPipe)
        .foldLeft(ctx.lastPipe(head))(_ ++ _)
      ctx.addOutput(appliedPTransform, flattened)
    }
  }

  def groupByKeyOnly[K <: Comparable[K], V]() = new TransformEvaluator[GroupByKey.GroupByKeyOnly[K, V]] {
    override def evaluate(appliedPTransform: AppliedPTransform[_, _, GroupByKey.GroupByKeyOnly[K, V]], transform: GroupByKey.GroupByKeyOnly[K, V], ctx: SContext): SContext = {
      val value = ctx.getInput[PValue](appliedPTransform)
      ctx.addOutput(appliedPTransform,
        // NB - Had to convert null keys to "null", because cascading doesn't like null keys but
        // Combine.Globally creates keys with null keys. Ref - https://goo.gl/CjQhsA
        ctx.lastPipe(value).apply[KV[K, V], KV[K, JIterable[V]]](_.map(kv => Option(kv.getKey).getOrElse("null".asInstanceOf[K]) -> kv.getValue)
          .group
          .mapGroup[KV[K, JIterable[V]]]((key, values) => Iterator(KV.of(key, values.toIterable.asJava)))
          .values
        )
      )
    }
  }

  val COMBINE_FIELDS = new FieldGetter(classOf[Combine.Globally[_, _]])
  def aggregate[I, A, O]() = new TransformEvaluator[Combine.Globally[I, O]] {
    override def evaluate(appliedPTransform: AppliedPTransform[_, _, Combine.Globally[I, O]], transform: Combine.Globally[I, O], ctx: SContext): SContext = {
      val value = ctx.getInput[PValue](appliedPTransform)
      val fn = COMBINE_FIELDS.get[CombineFn[I, A, O]]("fn", transform)
      val acc = fn.createAccumulator()
      ctx.addOutput(appliedPTransform,
        ctx.lastPipe(value).apply[I, O](_.aggregate[A, O](new TAggregator[I, A, O]() {
          override def prepare(input: I): A = fn.addInput(acc, input)
          override def present(reduction: A): O = fn.extractOutput(acc)
          override def semigroup: Semigroup[A] = new Semigroup[A] {
            override def plus(l: A, r: A): A = fn.mergeAccumulators(Iterable(l, r).asJava)
          }
        }))
      )
    }
  }

  def createPCollView[Record, View]() = new TransformEvaluator[View.CreatePCollectionView[Record, View]] {
    override def evaluate(appliedPTransform: AppliedPTransform[_, _, View.CreatePCollectionView[Record, View]], transform: CreatePCollectionView[Record, View], ctx: SContext): SContext = {
      val value = ctx.getInput[PValue](appliedPTransform)
      val result: JIterable[WindowedValue[Record]] = ScaldingExecutor
        .asIterable[Record](ctx.lastPipe(value), ctx)
        .map(WindowedValue.valueInGlobalWindow[Record])
        .asJava
      ctx.addToView(ctx.getOutput[PValue](appliedPTransform), result.asInstanceOf[JIterable[WindowedValue[_]]])
    }
  }

  def create[T]() = new TransformEvaluator[Create.Values[T]] {
    override def evaluate(appliedPTransform: AppliedPTransform[_, _, Create.Values[T]], transform: Create.Values[T], ctx: SContext): SContext = {
      ctx.addInput(appliedPTransform,
        SPipe(TypedPipe.from[T](transform.getElements.asScala))
      )
    }
  }

  private val EVALUATORS: Map[Class[_ <: PTransform[_, _]], TransformEvaluator[_]] = Map(
    classOf[RBound[_]] -> readText(),
    classOf[WBound[_]] -> writeText(),
    classOf[ParDo.Bound[_, _]] -> flatMap(),
    classOf[GroupByKey.GroupByKeyOnly[_, _]] -> groupByKeyOnly(),
    classOf[Combine.Globally[_, _]] -> aggregate(),
    classOf[View.CreatePCollectionView[_, _]] -> createPCollView(),
    classOf[Create.Values[_]] -> create(),
    classOf[Flatten.FlattenPCollectionList[_]] -> flattenPColl()
  )

  def has[PT <: PTransform[_, _]](clazz: Class[PT]) = EVALUATORS.contains(clazz)
  def get[PT <: PTransform[_, _]](clazz: Class[PT]) = EVALUATORS(clazz)
}
