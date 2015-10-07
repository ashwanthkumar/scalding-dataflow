[![Build Status](https://snap-ci.com/ashwanthkumar/scalding-dataflow/branch/master/build_image)](https://snap-ci.com/ashwanthkumar/scalding-dataflow/branch/master)

# scalding-dataflow
Scalding Runner for Google Dataflow SDK. This project is a WIP, try it at your own risk.

## Usage

You can use it in your own SBT projects
### built.sbt
```sbt
resolvers += Resolver.sonatypeRepo("snapshots")

// For more updated version check out the last run version of Build pipeline
libraryDependencies += "in.ashwanthkumar" %% "scalding-dataflow" % "1.0.23-SNAPSHOT"
```

### pom.xml
```xml
  <dependency>
    <groupId>in.ashwanthkumar</groupId>
    <artifactId>scalding-dataflow_2.10</artifactId>
    <!-- For more updated version check out the last run version of Build pipeline -->
    <version>1.0.23</version>
  </dependency>

  ....

  <repositories>
    <repository>
      <id>oss.sonatype.org-snapshot</id>
      <url>http://oss.sonatype.org/content/repositories/snapshots</url>
      <releases>
        <enabled>false</enabled>
      </releases>
      <snapshots>
        <enabled>true</enabled>
      </snapshots>
    </repository>
  </repositories>
```

Pass the following options to the program (_WordCount_) when running it

`--runner=ScaldingPipelineRunner --name=Main-Test --mode=local`

```java
  PipelineOptions options = PipelineOptionsFactory
    .fromArgs(args)
    .withValidation()
    .create();
  Pipeline pipeline = Pipeline.create(options);

  pipeline.apply(TextIO.Read.from("kinglear.txt").named("Source"))
    .apply(Count.<String>perElement())
    .apply(ParDo.of(new DoFn<KV<String, Long>, String>() {
      @Override
      public void processElement(ProcessContext c) throws Exception {
        KV<String, Long> kv = c.element();
        c.output(String.format("%s\t%d", kv.getKey(), kv.getValue()));
      }
    }))
    .apply(TextIO.Write.to("out.txt").named("Sink"));

  pipeline.run();
```

If you want to run it on HDFS (experimental), change the `mode=local` to `mode=hdfs`

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
- [x] Combine.GroupedValues
- [ ] Combine.PerKey
- [ ] View.AsSingleton
- [ ] View.AsIterable
- [ ] Window.Bound

### IO
- [x] Text
- [ ] Custom Cascading Scheme
- [ ] Iterable of Items
- [ ] Google SDK's Coder for SerDe

### Scalding
- [x] Move to TypedPipes
- [ ] Test it on Hadoop Mode
