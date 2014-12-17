TODOs:

- Add most/all standard methods from TraversableOnce (note that it shows how to write toList as a call to collect using
  @uncheckedVariance), and from Iterable and Seq.
- Replace SourceInput contract for NoSuchElementException with a custom EofException
- Add pusher/puller, and convenience methods to interface multiple streams 
- Probably make some things in the run.* interfaces private
- Write all scaladocs
- Decide on FastFuture vs async/await vs Func vs branching manually on Future.isCompleted, and document in README
- Fix all TODO comments
- More combinators
- AsyncBuffer
- Support `take` somehow. Maybe we can emit an EOF to downstream, and then not return from onNext until the stream is
  completed. Or maybe we should really throw a DownstreamDeclaredEOF subscription to upstream - but then would upstream
  user code need to have a chance to do something about it, as with onError?
- Copy to main project (presumably in a different namespace like streams2 for the duration of the migration)
- Maybe make Consumer a trait, and replace instantiations with implementations where possible to save the cost of extra Funcs
- Add build* methods on stream components other than Sink
- Replace calls to XxxFunc.apply with new XxxFunc implementations. This includes combinators in Func.scala as well as 
  external uses.

