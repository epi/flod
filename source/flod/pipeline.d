/** Pipeline composition.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.pipeline;

import flod.meta : isType, NonCopyable, moveIfNonCopyable, str;
import flod.traits;

///
template isPeekPipeline(P) {
	enum isPeekPipeline = isPeekable!P;
}

///
template isPullPipeline(P) {
	enum isPullPipeline = isPullable!P;
}

///
template isImmediatePipeline(P) {
	enum isImmediatePipeline = isPeekPipeline!P || isPullPipeline!P;
}

///
template isPushPipeline(P) {
	template impl() {
		static if (is(typeof({
						static struct PushSink {
							size_t push(T)(const(T)[] buf) { return buf.length; }
						}
						P p;
						return p.create(PushSink());
					}()) Result)) {
			enum impl = isRunnable!Result;
		} else {
			enum impl = false;
		}
	}
	enum isPushPipeline = impl!();
}

///
template isAllocPipeline(P) {
	template impl() {
		static if (is(typeof({
						static struct AllocSink {
							auto alloc(T = ubyte)(size_t n) { return new T[n]; }
							void commit(size_t) {}
						}
						P p;
						return p.create(AllocSink());
					}()) Result)) {
			enum impl = isRunnable!Result;
		} else {
			enum impl = false;
		}
	}
	enum isAllocPipeline = impl!();
}

///
template isDeferredPipeline(P) {
	enum isDeferredPipeline = isAllocPipeline!P || isPushPipeline!P;
}

///
template isPipeline(P) {
	enum isPipeline = isImmediatePipeline!P || isDeferredPipeline!P;
}

// not used yet, not tested
// probably wrappers will be used for all stages, e.g. to implement some optional methods
// as just forwarding the calls to the next/previous stage
struct WrapPeekSource(Source, DefaultType = ubyte) {
	private Source s;
	static if (is(FixedPeekType!Source F)) {
		auto peek(T = F)(size_t n) if (T == F) { return s.peek(n); }
		void consume(T = F)(size_t n) if (T == F) { s.consume(n); }
	} else {
		static if (is(DefaultPeekType!Source D))
			alias DT = D;
		else
			alias DT = DefaultType;
		auto peek(T = DT)(size_t n) { return s.peek!T(n); }
		void consume(T = DT)(size_t n) { s.consume!T(n); }
	}
}

auto wrapPeekSource(S)(auto ref S s) {
	return WrapPeekSource!S(moveIfNonCopyable(s));
}

version(unittest) {
	/*
These are some trivial sources, filters and sink used to test `pipe` primitives.

Implemented:
Sources:
+ Pull
+ Peek
+ Push
+ Alloc

Sinks:
+ Pull
+ Peek
+ Push
+ Alloc

Filters:
  si  \ so  Pull Peek Push Alloc
  Pull       +    +    +    -
  Peek       +    +    -    -
  Push       -    -    -    -
  Alloc      -    -    -    -

*/

	// sources:

	struct TestPullSource {
		size_t pull(T)(T[] buf)
		{
			buf[] = T.init;
			return buf.length;
		}
	}
	static assert(isPullSource!TestPullSource);
	static assert(!isSink!TestPullSource);
	static assert(!is(DefaultPullType!TestPullSource));

	static struct TestPeekSource {
		const(T)[] peek(T = ubyte)(size_t n) { return new T[n]; }
		void consume(T = ubyte)(size_t n) {}
	}
	static assert(isPeekSource!TestPeekSource && !isSink!TestPeekSource);

	struct TestPushSource(Sink) {
		mixin NonCopyable;

		Sink sink;
		ulong dummy;
		const(ubyte)[] blob;

		int step()()
		{
			return sink.push(blob) != blob.length;
		}
	}
	static assert(isPushSource!TestPushSource && !isSink!TestPushSource);

	struct TestAllocSource(Sink) {
		Sink sink;
		string dummy;
		ulong unused;

		int step()()
		{
			auto buf = sink.alloc(15);
			buf[] = typeof(buf[0]).init;
			sink.commit(buf.length);
			return buf.length != 15;
		}
	}
	static assert(isAllocSource!TestAllocSource && !isSink!TestAllocSource);

	// sinks:

	struct TestPullSink(Source) {
		Source src;
		int a;

		int run()
		{
			auto buf = new int[a];
			foreach (i; 1 .. 10) {
				if (src.pull(buf) != buf.length)
					return 1;
			}
			return 0;
		}
	}
	static assert(isPullSink!TestPullSink && !isSource!TestPullSink);

	struct TestPeekSink(Source) {
		Source s;
		int step()
		{
			auto buf = s.peek(10);
			s.consume(buf.length);
			return 1;
		}
	}
	static assert(isPeekSink!TestPeekSink && !isSource!TestPeekSink);

	struct TestPushSink {
		size_t push(T)(const(T)[] buf) { return buf.length - 1; }
	}
	static assert(isPushSink!TestPushSink && !isSource!TestPushSink);

	struct TestAllocSink {
		auto alloc(T = ubyte)(size_t n) { return new T[n - 1]; }
		void commit(T = ubyte)(size_t n) {}
	}
	static assert(isAllocSink!TestAllocSink);
	static assert(!isSource!TestAllocSink);

	// filters:

	struct TestPullFilter(Source) {
		Source source;
		size_t pull(T)(T[] buf) { return source.pull(buf); }
	}
	static assert(isPullSink!TestPullFilter && isPullSource!TestPullFilter);

	struct TestPullPeekFilter(Source) {
		Source s;
		const(T)[] peek(T = ubyte)(size_t n) { auto buf = new T[n]; s.pull(buf); return buf; }
		void consume(T = ubyte)(size_t n) {}
	}
	static assert(isPullSink!TestPullPeekFilter && isPeekSource!TestPullPeekFilter);

	struct TestPullPushFilter(Source, Sink) {
		mixin NonCopyable;
		Source source;
		Sink sink;

		int step()
		{
			ubyte[4096] buf;
			auto n = source.pull(buf);
			return sink.push(buf[0 .. n]) != buf.length;
		}
	}
	static assert(isPullSink!TestPullPushFilter);
	static assert(isPushSource!TestPullPushFilter);

	struct TestPullAllocFilter(Source, Sink) {
		mixin NonCopyable;
		Source source;
		Sink sink;

		int step()
		{
			auto buf = source.alloc(4096);
			auto n = source.pull(buf);
			return sink.push(buf[0 .. n]) != buf.length;
		}
	}

	struct TestPeekFilter(Source) {
		Source s;
		static if (is(FixedPeekType!Source U)) {
			const(T)[] peek(T = U)(size_t n) if (is(T == U)) { return s.peek(n); }
			void consume(T = U)(size_t n) if (is(T == U)) { s.consume(n); }
		} else {
			static if (is(DefaultPeekType!Source D))
				alias DT = D;
			else
				alias DT = ubyte;
			const(T)[] peek(T = DT)(size_t n) { return s.peek!T(n); }
			void consume(T = DT)(size_t n) { s.consume!T(n); }
		}
	}
	static assert(isPeekSink!TestPeekFilter && isPeekSource!TestPeekFilter);

	struct TestPeekPullFilter(Source) {
		Source source;
		size_t pull(T)(T[] buf)
		{
			auto nb = source.peek(buf.length);
			buf[] = typeof(buf[0]).init;
			source.consume(nb.length);
			return buf.length;
		}
	}
	static assert(isPeekSink!TestPeekPullFilter && isPullSource!TestPeekPullFilter);
}

// this is only used to check if all possible tuples (pipeline, stage sink, stage source) are tested
debug private struct PrintWia(string a, string b, string c) {
	pragma(msg, "  Append:  \x1b[31;1m", a, "\x1b[0m:\x1b[33;1m", b, "\x1b[0m:\x1b[32;1m", c, "\x1b[0m");
}

debug { private template whatIsAppended(P, alias S, string file = __FILE__) {
	static if (file != "source/flod/pipeline.d") {
		alias whatIsAppended = void;
	} else {
		static if (isPeekPipeline!P)
			enum pipelineMethod = "peek";
		else static if (isPullPipeline!P)
			enum pipelineMethod = "pull";
		else static if (isPushPipeline!P)
			enum pipelineMethod = "push";
		else static if (isAllocPipeline!P)
			enum pipelineMethod = "alloc";
		else static if (!is(P == void))
			enum pipelineMethod = "(?" ~ str!P ~ ")";
		else
			enum pipelineMethod = "";

		static if (isPeekSink!S)
			enum sinkMethod = "peek";
		else static if (isPullSink!S)
			enum sinkMethod = "pull";
		else static if (isPushSink!S)
			enum sinkMethod = "push";
		else static if (isAllocSink!S)
			enum sinkMethod = "alloc";
		else
			enum sinkMethod = "";

		static if (isPeekSource!S)
			enum sourceMethod = "peek";
		else static if (isPullSource!S)
			enum sourceMethod = "pull";
		else static if (isPushSource!S)
			enum sourceMethod = "push";
		else static if (isAllocSource!S)
			enum sourceMethod = "alloc";
		else static if (sinkMethod == "")
			enum sourceMethod = "(?" ~ str!S ~ ")";
		else
			enum sourceMethod =".";

		alias whatIsAppended = PrintWia!(pipelineMethod, sinkMethod, sourceMethod);
	}
}
}

/**
Run a `pipeline` by repeatedly calling `pipeline.step`.
*/
int run(Pipeline)(auto ref Pipeline pipeline)
	if (isRunnable!Pipeline)
{
	for (;;) {
		int r = pipeline.step();
		if (r != 0)
			return r;
	}
}

/**
Start building a new pipeline, adding `Stage` as its source _stage.

If `Stage` is an active source, the type of its sink is not known at this point and therefore it is not
constructed immediately. Instead, `args` are saved and will be used to construct `Stage!Sink` as
soon as type of `Sink` is known.

Params:
Stage = source to be added as first _stage of the new pipeline.
args  = arguments passed to Stage constructor.
*/
auto pipe(alias Stage, Args...)(auto ref Args args)
	if (!isSink!Stage && isPassiveSource!Stage)
{
	alias X = whatIsAppended!(void, Stage);
	return Stage(args);
}

unittest {
	auto p1 = pipe!TestPeekSource;
	auto p2 = pipe!TestPullSource;
}

/// ditto
auto pipe(alias Stage, Args...)(auto ref Args args)
	if (!isSink!Stage && isActiveSource!Stage)
{
	alias X = whatIsAppended!(void, Stage);
	static struct DeferredCtor {
		mixin NonCopyable;
		Args args;

		auto create(Sink)(auto ref Sink sink)
		{
			return Stage!Sink(moveIfNonCopyable(sink), args);
		}
	}
	return DeferredCtor(args);
}

unittest {
	auto p1 = pipe!TestPushSource(0xdeadUL);
	static assert(isPushPipeline!(typeof(p1)));
	assert(p1.create(TestPushSink()).run() == 1);
	assert(pipe!TestAllocSource("test", 0UL).create(TestAllocSink()).run() == 1);
}

/**
Append `Stage` to `pipeline`.

Params:
 Stage = a `struct` or `class` template parameterized by its source type. `Stage` will be
         instantiated with `Pipeline` as template argument. `Stage` constructor
         must accept the following order of arguments: `(pipeline, args)`.
 pipeline = a pipeline ending with a passive (peek/consume or pull) source.
 args = additional arguments to `Stage` constructor.
Returns: `Stage!Pipeline(pipeline, args)`.
`pipeline` is moved if non-copyable.
*/
auto pipe(alias Stage, Pipeline, string file = __FILE__, int line = __LINE__, Args...)(auto ref Pipeline pipeline, auto ref Args args)
	if (isImmediatePipeline!Pipeline && isActiveSink!Stage && !isActiveSource!Stage)
out(result)
{
	import std.traits : Unqual;
	alias P = Unqual!(typeof(result));
	static assert(isImmediatePipeline!P || isRunnable!P);
}
body
{
	debug alias X = whatIsAppended!(Pipeline, Stage);
	static if (isPeekable!Pipeline && isPeekSink!Stage) {
		return Stage!Pipeline(moveIfNonCopyable(pipeline), args);
	} else static if (isPullable!Pipeline && isPullSink!Stage) {
		return Stage!Pipeline(moveIfNonCopyable(pipeline), args);
	} else static if (isPeekable!Pipeline && isPullSink!Stage) {
		import flod.adapter : DefaultPeekPullAdapter;
		debug pragma(msg, file, ":", line, ": Inserting implicit peek-pull adapter");
		return pipeline.pipe!DefaultPeekPullAdapter.pipe!Stage(args);
	} else static if (isPullable!Pipeline && isPeekSink!Stage) {
		import flod.adapter : DefaultPullPeekAdapter;
		debug pragma(msg, file, ":", line, ": Inserting implicit pull-peek adapter");
		return pipeline.pipe!DefaultPullPeekAdapter.pipe!Stage(args);
	} else {
		// TODO: give a better diagnostic message
		static assert(0, "Cannot instantiate " ~ str!Stage ~ "!" ~ str!Pipeline);
	}
}

unittest {
	import flod.adapter;
	assert(pipe!TestPeekSource().pipe!TestPeekSink.run() == 1);
	assert(pipe!TestPeekSource().pipe!TestPeekFilter.pipe!TestPeekSink.run() == 1);
	assert(pipe!TestPullSource().pipe!TestPullSink(31337).run() == 0);
	assert(pipe!TestPullSource().pipe!TestPullFilter.pipe!TestPullSink(31337).run() == 0);
	assert(pipe!TestPullSource().pipe!TestPullFilter.pipe!TestPullFilter.pipe!TestPullFilter
		.pipe!TestPullSink(31337).run() == 0);
	assert(pipe!TestPullSource()
		.pipe!TestPullFilter
		.pipe!TestPullPeekFilter
		.pipe!TestPeekFilter
		.pipe!TestPeekSink.run() == 1);
	assert(pipe!TestPeekSource()
		.pipe!TestPeekFilter
		.pipe!TestPeekPullFilter
		.pipe!TestPullFilter
		.pipe!TestPullSink.run() == 0);
	assert(pipe!TestPeekSource()
		.pipe!TestPeekFilter
		.pipe!TestPeekPullFilter
		.pipe!TestPullFilter
		.pipe!TestPullFilter
		.pipe!TestPullPeekFilter
		.pipe!TestPeekFilter
		.pipe!TestPeekFilter
		.pipe!TestPeekSink.run() == 1);
	// with implicit adapters
	assert(pipe!TestPullSource()
		.pipe!TestPeekSink().run() == 1);
	assert(pipe!TestPullSource()
		.pipe!TestPeekPullFilter
		.pipe!TestPeekPullFilter
		.pipe!TestPeekSink().run() == 1);
	assert(pipe!TestPullSource()
		.pipe!TestPullFilter
		.pipe!TestPeekFilter
		.pipe!TestPeekSink().run() == 1);
	assert(pipe!TestPeekSource()
		.pipe!TestPullSink().run() == 0);
	assert(pipe!TestPeekSource()
		.pipe!TestPullPeekFilter
		.pipe!TestPullPeekFilter
		.pipe!TestPullSink().run() == 0);
	assert(pipe!TestPeekSource()
		.pipe!TestPeekFilter
		.pipe!TestPullFilter
		.pipe!TestPullSink().run() == 0);
	assert(pipe!TestPeekSource()
		.pipe!TestPeekFilter
		.pipe!TestPullFilter
		.pipe!TestPeekFilter
		.pipe!TestPullFilter
		.pipe!TestPeekFilter
		.pipe!TestPullFilter
		.pipe!TestPullSink().run() == 0);
}

/// ditto
auto pipe(alias Stage, Pipeline, Args...)(auto ref Pipeline pipeline, auto ref Args args)
	if (isImmediatePipeline!Pipeline && isActiveSink!Stage && isActiveSource!Stage)
{
	debug alias X = whatIsAppended!(Pipeline, Stage);
	static struct DeferredCtor {
		mixin NonCopyable;
		Pipeline pipeline;
		Args args;

		auto create(Sink)(auto ref Sink sink)
		{
			//import std.algorithm : move;
			return Stage!(Pipeline, Sink)(moveIfNonCopyable(pipeline), moveIfNonCopyable(sink), args);
		}

	}
	static if (isPullPipeline!Pipeline && isPullSink!Stage) {
		return DeferredCtor(moveIfNonCopyable(pipeline), args);
	} else static if (isPeekPipeline!Pipeline && isPeekSink!Stage) {
		return DeferredCtor(moveIfNonCopyable(pipeline), args);
	} else static if (isPullPipeline!Pipeline && isPeekSink!Stage) {
		import flod.adapter : DefaultPullPeekAdapter;
		debug pragma(msg, "Inserting implicit pull-peek adapter");
		return pipeline.pipe!DefaultPullPeekAdapter.pipe!Stage(args);
	} else static if (isPeekPipeline!Pipeline && isPullSink!Stage) {
		import flod.adapter : DefaultPeekPullAdapter;
		debug pragma(msg, "Inserting implicit peek-pull adapter");
		return pipeline.pipe!DefaultPeekPullAdapter.pipe!Stage(args);
	}
}

/// ditto
auto pipe(alias Stage, Pipeline, Args...)(auto ref Pipeline pipeline, auto ref Args args)
	if (isDeferredPipeline!Pipeline && isPassiveSink!Stage && !isActiveSource!Stage)
{
	debug alias X = whatIsAppended!(Pipeline, Stage);
	return pipeline.create(Stage(args));
}

/// ditto
auto pipe(alias Stage, Pipeline, Args...)(auto ref Pipeline pipeline, auto ref Args args)
	if (isDeferredPipeline!Pipeline && isPassiveSink!Stage && isActiveSource!Stage)
{
	debug alias X = whatIsAppended!(Pipeline, Stage);
	static struct DeferredCtor {
		mixin NonCopyable;
		Pipeline pipeline;
		Args args;

		auto create(Sink)(auto ref Sink sink)
		{
			return pipeline.create(Stage!Sink(moveIfNonCopyable(sink), args));
		}
	}
	return DeferredCtor(moveIfNonCopyable(pipeline), args);
}


unittest {
	// pull - pull/push
	auto p1 = pipe!TestPullSource;
	auto p2 = p1.pipe!TestPullPushFilter;
	static assert(isPushPipeline!(typeof(p2)));
	auto p3 = p2.pipe!TestPushSink;
	assert(p3.run() == 1);
}

unittest {
	// peek - pull/push
	auto p = pipe!TestPeekSource.pipe!TestPullPushFilter.pipe!TestPushSink;
	static assert(isRunnable!(typeof(p)));
	p.run();
}

///
auto pipe(T)(const(T)[] array)
{
	static struct ArraySource {
		mixin NonCopyable;
		const(T)[] array;
		auto peek(U = T)(size_t n) { return array; }
		void consume(U = T)(size_t n) { array = array[n .. $]; }
	}
	return pipe!ArraySource(array);
}
