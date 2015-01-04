TODOs:

- Add most/all standard methods from TraversableOnce (note that it shows how to write toList as a call to collect using
  @uncheckedVariance), and from Iterable and Seq.
- Probably make some things in the run.* interfaces private
- Decide on FastFuture vs async/await vs Func vs branching manually on Future.isCompleted, and document in README
- Support `take` somehow. Maybe we can emit an EOF to downstream, and then not return from onNext until the stream is
  completed. Or maybe we should really throw a DownstreamDeclaredEOF subscription to upstream - but then would upstream
  user code need to have a chance to do something about it, as with onError?
- Maybe make Consumer a trait, and replace instantiations with implementations where possible to save the cost of extra Funcs
- Replace calls to XxxFunc.apply with new XxxFunc implementations. This includes combinators in Func.scala as well as
  external uses. (Is this really beneficial?)
- Add xxxAsync/xxx/xxxFunc triplets to all appropriate constructors on Sink, Source, Transform (matching SourceOps)
- Add general docs about families of methods to Sink, Transform, SourceOps, and mention them in the README
- I really doubt if ALL my uses of @uncheckedVariance are legal
- Note explicitly in README that methods should always declare to return a Pipe even when they use a single Transform to implement it.
- Add a note to the README about the push-through model
- Document Pipe.flatten better in the README, including noting the point of asynchronicity on input
- Add Source and Sink and StreamResult flatteners
- Why are Source, Sink constructors not in StreamInput, StreamOutput? Just for simplicity / usability / less typing,
  but it makes the API less regular...
- Use of Pipe constructors is cumbersome: have to keep track manually of source segment, because the DSLs really build up
  only the downstream side. The .pipe methods are not enough. See e.g. in the Foresight source, what
  HtmlManipulator.manipulatorPipe has to do with `uncompressor` and `tapper`. It's too easy to get this wrong if even I
  do so half the time!
- Make SourceOps methods return a Source[Next] when used on a Source (if I can figure out how to do it without overriding them)
- Because methods like SourceOps.foreach take generic arguments, you can't write source.foreach(println(_)), you have to
  write explicitly source.foreach((x: String) => println(x)). Can this be fixed?
- Why not make the builder an implicit param in the model case classes? It seems to work fine for e.g. Merger
- Add overloads taking simple functions, identical to those in SourceOps, to Source/Sink/Transform constructors.
- Add detailed guide for all SourceOps and constructors to README, with examples
- Document DrivenSource in README
- Rewrite DelayedPipeMachine to use DrivenSource like DelayedSinkMachine does
- Rewrite Source.concat to be more efficient, even if it takes a new state machine
- Some state machines STILL don't complete their completionPromise
