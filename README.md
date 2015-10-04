[![Build Status](https://snap-ci.com/ashwanthkumar/scalding-dataflow/branch/master/build_image)](https://snap-ci.com/ashwanthkumar/scalding-dataflow/branch/master)

# scalding-dataflow
Scalding Runner for Google Dataflow SDK. This project is a WIP, try it at your own risk.

## Usage
Pass the following options to the program when running it 

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
- [ ] ParDo.Bound with sideInputs
- [ ] Combine
- [ ] Flatten

### IO
- [x] Text
- [ ] Avro
- [ ] Parquet
- [ ] Custom Cascading Scheme
- [ ] Iterable of Items

### Scalding
- [ ] Move to TypedPipes
