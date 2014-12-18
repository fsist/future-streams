TODOs:

- Add most/all standard methods from TraversableOnce (note that it shows how to write toList as a call to collect using
  @uncheckedVariance), and from Iterable and Seq.
- Add pusher/puller, and convenience methods to interface multiple streams
- Probably make some things in the run.* interfaces private
- Decide on FastFuture vs async/await vs Func vs branching manually on Future.isCompleted, and document in README
- More combinators
- AsyncBuffer
- Support `take` somehow. Maybe we can emit an EOF to downstream, and then not return from onNext until the stream is
  completed. Or maybe we should really throw a DownstreamDeclaredEOF subscription to upstream - but then would upstream
  user code need to have a chance to do something about it, as with onError?
- Copy to main project (presumably in a different namespace like streams2 for the duration of the migration)
- Maybe make Consumer a trait, and replace instantiations with implementations where possible to save the cost of extra Funcs
- Replace calls to XxxFunc.apply with new XxxFunc implementations. This includes combinators in Func.scala as well as
  external uses.
- Convenience xxxAsync constructors take functions A => Future[B] instead of A => ExecutionContext => Future[B]
  because I can't figure out the syntax for an anonymous function taking implicit arguments
- Add xxxAsync/xxx/xxxFunc triplets to all appropriate constructors on Sink, Source, Transform (matching SourceOps)
- Add trait Pipe[In, Out] which can represent any sink-with-source. It might be a simple Transform, or it might
  have more complex internal structure made of multiple components.

