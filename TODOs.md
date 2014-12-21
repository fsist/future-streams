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
- Add general docs about families of methods to Sink, Transform, SourceOps, and mention them in the README

- Since I've moved all single-result-producing functions like head / collect / concat to Transform, I should probably
  move Sink.foldLeft too. So StreamOutput is only left to 1) produce a result, usually using `single` and 2) have side
  effects using foreach.

- I really doubt if ALL my uses of @uncheckedVariance are legal

- It's annoying to write, most of the time you implement a component by trait,     override implicit val builder: FutureStreamBuilder = new FutureStreamBuilder
  Also there's a common mistake where you write "override implicit def builder" and create a new one on every call,
  which breaks everything.

- Note explicitly in README that methods should always declare to return a Pipe even when they use a single Transform to implement it.

These v1 patterns need v2 equivalents + docs: 

- Often library code has to provide a StreamOutput with a non-Unit result. E.g., HashingUtil provides a Sink[ByteString]
  that calculates a hash. But it may have internal components (i.e. be composed of a Pipe + an actual StreamOutput).
  This limits it to a Sink type, and not a StreamOutput, but then it doesn't have a result. What to do?
- Several places in the code use a Pipe.tapOne + Pipe.flatten to decide what to do with the rest of the pipe based on
  the first observed element. Is there any better solution than just writing a Future-based state machine that supports
  all options? Maybe with some minimal help a la Actor.become?
- SprayAckStream inner class AckActor needs to fail the outer sink (= the stream) asynchronously in some cases, 
  but it only has access to the Sink model, not the running component. The problem is that AckActor is instantiated
  in a special and ugly way which must be called synchronously from its to-be-Parent actor...