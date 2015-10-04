[![Build Status](https://snap-ci.com/ashwanthkumar/scalding-dataflow/branch/master/build_image)](https://snap-ci.com/ashwanthkumar/scalding-dataflow/branch/master)

# scalding-dataflow
Scalding Runner for Google Dataflow SDK. This project is a WIP, try it at your own risk.

## Usage
Pass the following options to the program (_WordCount_) when running it

`--runner=ScaldingPipelineRunner --name=Main-Test --mode=local`

```scala
  val withOptions = PipelineOptionsFactory
    .fromArgs(args)
    .withValidation()
    .create()
  val pipeline = Pipeline.create(withOptions)
  pipeline
    .apply(TextIO.Read.from("kinglear.txt").named("Source"))
    .apply(ParDo.named("convert-to-length").of[String, Integer](new DoFn[String, Integer]() {
    override def processElement(c: DoFn[String, Integer]#ProcessContext): Unit = c.output(c.element().length)
  }))
    .apply(Filter.greaterThan[Integer](10))
    .apply(ParDo.named("to-string").of[Integer, String](new DoFn[Integer, String]() {
    override def processElement(c: DoFn[Integer, String]#ProcessContext): Unit = c.output(c.element().toString)
  }))
    .apply(TextIO.Write.to("out.txt").named("Sink"))

  pipeline.run()
```

If you want to run it on HDFS, change the `mode=local` to `mode=hdfs`

## Todos
### Translators
- [x] ParDo.Bound
- [x] Filter
- [x] Keys
- [x] Values
- [x] KvSwap
- [x] ParDo.Bound with sideInputs
- [x] Combine
- [x] Flatten
- [ ] ParDo.BoundMulti
- [ ] Combine.GroupedValues
- [ ] Combine.PerKey
- [ ] View.AsSingleton
- [ ] View.AsIterable
- [ ] Window.Bound

### IO
- [x] Text
- [ ] Custom Cascading Scheme
- [ ] Iterable of Items

### Scalding
- [x] Move to TypedPipes
