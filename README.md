# scalding-dataflow
Scalding Runner for Google Dataflow SDK. This project is a WIP, try it at your own risk.

## Usage
### Running in Local mode (verified)
```java
  val withOptions = PipelineOptionsFactory.fromArgs(Array()).create()
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

  ScaldingRunner.local("Test Pipeline").run(pipeline)
```

### Running in HDFS mode (not tested yet)
```java
  val withOptions = PipelineOptionsFactory.fromArgs(Array()).create()
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

  ScaldingRunner.hdfs("Test Pipeline on Hadoop Cluster").run(pipeline)
```

## Todos
### Translators
- [x] ParDo.Bound
- [x] Filter
- [x] Keys
- [x] Values
- [x] KvSwap
- [ ] ParDo.Bound with sideInputs
- [ ] Combine

### IO
- [x] Text
- [ ] Avro
- [ ] Parquet
- [ ] Custom Cascading Scheme
- [ ] Iterable of Items

### Scalding
- [ ] Move to TypedPipes
