/** Pipeline composition.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.pipeline;

import std.range : isInputRange, isOutputRange;

import flod.meta : isType, NonCopyable, moveIfNonCopyable, str;
import flod.traits;

///
template isPeekPipeline(P) {
	import std.traits : isDynamicArray;
	enum isPeekPipeline = isPeekable!P || isDynamicArray!P;
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
						import flod.traits : PushSink;
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
						import flod.traits : AllocSink;
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

private auto wrapArray(T)(const(T)[] array)
{
	static struct ArraySource {
		private const(T)[] array;

		auto peek(U = T)(size_t n) { return array; }
		void consume(U = T)(size_t n) { array = array[n .. $]; }
	}
	return ArraySource(array);
}

private auto wrapInputRange(Range)(Range range)
	if (isInputRange!Range)
{
	static struct InputRangeSource {
		private Range range;
		import std.range : ElementType;
		alias T = ElementType!Range;

		size_t pull(T[] buf)
		{
			foreach (i, ref el; buf) {
				if (range.empty)
					return i;
				el = range.front;
				range.popFront();
			}
			return buf.length;
		}
	}
	return InputRangeSource(range);
}

unittest {
	import std.range : iota, array;
	auto r = iota(0, 10);
	int[10] buf;
	r.wrapInputRange.pull(buf[]);
	assert(buf[] == iota(0, 10).array());
}

/// Use any output range as a pushable source.
auto toOutputRange(Pipeline, Range)(auto ref Pipeline pipeline, ref Range range)
	if (isPipeline!Pipeline)
{
	static struct OutputRange {
	private:
		Range* range;
	public:
		size_t push(T)(const(T)[] buf)
		{
			import std.range : put;
			static if (is(typeof({ put(range, buf); }))) {
				put(range, buf);
				return buf.length;
			} else {
				// hack :/
				return 0;
			}
		}
	}
	return pipeline.pipe!OutputRange(&range);
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

	@implements!(PullSource, TestPullSource)
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

	@implements!(PeekSource, TestPeekSource)
	static struct TestPeekSource {
		const(T)[] peek(T = ubyte)(size_t n) { return new T[n]; }
		void consume(T = ubyte)(size_t n) {}
	}
	static assert(isPeekSource!TestPeekSource && !isSink!TestPeekSource);

	@implements!(PushSource, TestPushSource)
	struct TestPushSource(Sink) {
		mixin NonCopyable;

		Sink sink;
		ulong dummy;
		const(ubyte)[] blob;

		void run()()
		{
			while (sink.push(blob) == blob.length) {}
		}
	}
	static assert(!isSink!TestPushSource);

	@implements!(AllocSource, TestAllocSource)
	struct TestAllocSource(Sink) {
		Sink sink;
		string dummy;
		ulong unused;

		void run()()
		{
			for (;;) {
				auto buf = sink.alloc(15);
				buf[] = typeof(buf[0]).init;
				if (sink.commit(buf.length) != 15)
					break;
			}
		}
	}
	static assert(!isSink!TestAllocSource);

	// sinks:

	@implements!(PullSink, TestPullSink)
	struct TestPullSink(Source) {
		Source src;
		int a;

		void run()
		{
			auto buf = new int[a];
			foreach (i; 1 .. 10) {
				if (src.pull(buf) != buf.length)
					break;
			}
		}
	}
	static assert(isPullSink!TestPullSink && !isSource!TestPullSink);

	@implements!(PeekSink, TestPeekSink)
	struct TestPeekSink(Source) {
		Source s;
		void run()
		{
			auto buf = s.peek(10);
			s.consume(buf.length);
		}
	}
	static assert(isPeekSink!TestPeekSink && !isSource!TestPeekSink);

	@implements!(PushSink, TestPushSink)
	struct TestPushSink {
		size_t push(T)(const(T)[] buf) { return buf.length - 1; }
	}
	static assert(isPushSink!TestPushSink && !isSource!TestPushSink);

	@implements!(AllocSink, TestAllocSink)
	struct TestAllocSink {
		auto alloc(T = ubyte)(size_t n) { return new T[n - 1]; }
		size_t commit(T = ubyte)(size_t n) { return n; }
	}
	static assert(isAllocSink!TestAllocSink);
	static assert(!isSource!TestAllocSink);

	// filters:

	@implements!(PullSink, PullSource, TestPullFilter)
	struct TestPullFilter(Source) {
		Source source;
		size_t pull(T)(T[] buf) { return source.pull(buf); }
	}
	static assert(isPullSink!TestPullFilter && isPullSource!TestPullFilter);

	@implements!(PeekSink, PeekSource, TestPeekFilter)
	struct TestPullPeekFilter(Source) {
		Source s;
		const(T)[] peek(T = ubyte)(size_t n) { auto buf = new T[n]; s.pull(buf); return buf; }
		void consume(T = ubyte)(size_t n) {}
	}
	static assert(isPullSink!TestPullPeekFilter && isPeekSource!TestPullPeekFilter);

	@implements!(PullSink, PushSource, TestPullPushFilter)
	struct TestPullPushFilter(Source, Sink) {
		mixin NonCopyable;
		Source source;
		Sink sink;

		void run()
		{
			ubyte[4096] buf;
			for (;;) {
				auto n = source.pull(buf);
				if (sink.push(buf[0 .. n]) != buf.length)
					break;
			}
		}
	}
	static assert(isPullSink!TestPullPushFilter);
	static assert(isPushSource!TestPullPushFilter);

	@implements!(PullSink, AllocSource, TestPullAllocFilter)
	struct TestPullAllocFilter(Source, Sink) {
		mixin NonCopyable;
		Source source;
		Sink sink;

		void run()
		{
			for (;;) {
				auto buf = sink.alloc(4096);
				auto n = source.pull(buf);
				if (sink.commit(n) != 4096)
					break;
			}
		}
	}

	@implements!(PeekSink, PeekSource, TestPeekFilter)
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
	static assert(isPeekSink!TestPeekFilter);
	static assert(isPeekSource!TestPeekFilter);

	@implements!(PeekSink, PullSource, TestPeekPullFilter)
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

	@implements!(PeekSink, PushSource, TestPeekPushFilter)
	struct TestPeekPushFilter(Source, Sink) {
		mixin NonCopyable;
		Source source;
		Sink sink;

		void run()
		{
			for (;;) {
				auto nb = source.peek(4096);
				if (nb.length == 0)
					break;
				auto w = sink.push(nb);
				source.consume(w);
				if (w < nb.length)
					break;
			}
		}
	}
	static assert(isPeekSink!TestPeekPushFilter);
	static assert(isPushSource!TestPeekPushFilter);

	struct TestPushFilter(Sink) {
		Sink sink;
		size_t push(T)(const(T)[] buf) { return sink.push(buf); }
	}
	static assert(isPushSink!TestPushFilter);
	static assert(isPushSource!TestPushFilter);

}

// this is only used to check if all possible tuples (pipeline, stage sink, stage source) are tested
debug private struct PrintWia(string a, string b, string c) {
	pragma(msg, "  Append:  \x1b[31;1m", a, "\x1b[0m:\x1b[33;1m", b, "\x1b[0m:\x1b[32;1m", c, "\x1b[0m");
}

private template whatIsAppended(P, alias S, string file = __FILE__) {
	debug {
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
	} else {
		alias whatIsAppended = void;
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
	if (!isSink!Stage && isSource!Stage)
{
	return startPipe!Stage(args);
}

private auto startPipe(alias Stage, Args...)(auto ref Args args)
	if (!isSink!Stage && isPassiveSource!Stage)
{
	alias X = whatIsAppended!(void, Stage);
	return Stage(args);
}

unittest {
	auto p1 = pipe!TestPeekSource;
	auto p2 = pipe!TestPullSource;
}

private auto startPipe(alias Stage, Args...)(auto ref Args args)
	if (!isSink!Stage && isActiveSource!Stage)
{
	alias X = whatIsAppended!(void, Stage);
	static struct SourceDeferredCtor {
		mixin NonCopyable;
		Args args;

		auto create(Sink)(auto ref Sink sink)
		{
			return Stage!Sink(moveIfNonCopyable(sink), args);
		}

		auto create(Allocator, Sink)(ref Allocator allocator, auto ref Sink sink)
		{
			import std.experimental.allocator : make;
			return allocator.make!(Stage!Sink)(moveIfNonCopyable(sink), args);
		}
	}
	return SourceDeferredCtor(args);
}

unittest {
	auto p1 = pipe!TestPushSource(0xdeadUL);
	static assert(isPushPipeline!(typeof(p1)));
	p1.create(TestPushSink()).run();
	pipe!TestAllocSource("test", 0UL).create(TestAllocSink()).run();
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
auto pipe(alias Stage, Pipeline, Args...)(auto ref Pipeline pipeline, auto ref Args args)
	if (isPipeline!Pipeline && isSink!Stage)
{
	import std.traits : isDynamicArray;
	import std.range : isInputRange;

	static if (isDynamicArray!Pipeline) {
		return wrapArray(pipeline).appendPipe!Stage(args);
	} else static if (isInputRange!Pipeline) {
		return wrapInputRange(pipeline).appendPipe!Stage(args);
	} else {
		return pipeline.appendPipe!Stage(args);
	}
}


auto appendPipe(alias Stage, Pipeline, Args...)(auto ref Pipeline pipeline, auto ref Args args)
	if (isImmediatePipeline!Pipeline && isActiveSink!Stage && !isActiveSource!Stage)
/* TODO: ldc2-1.0.0-alpha1 doesn't like it
out(result)
{
	import std.traits : Unqual;
	alias P = Unqual!(typeof(result));
	static assert(isImmediatePipeline!P || isRunnable!P || isInputRange!P);
}
*/
body
{
	debug alias X = whatIsAppended!(Pipeline, Stage);
	static if (isPeekable!Pipeline && isPeekSink!Stage) {
		return Stage!Pipeline(moveIfNonCopyable(pipeline), args);
	} else static if (isPullable!Pipeline && isPullSink!Stage) {
		return Stage!Pipeline(moveIfNonCopyable(pipeline), args);
	} else static if (isPeekable!Pipeline && isPullSink!Stage) {
		import flod.adapter : peekPull;
		debug pragma(msg, "Inserting implicit peek-pull adapter");
		return pipeline.peekPull.appendPipe!Stage(args);
	} else static if (isPullable!Pipeline && isPeekSink!Stage) {
		import flod.adapter : pullPeek;
		debug pragma(msg, "Inserting implicit pull-peek adapter");
		return pipeline.pullPeek.appendPipe!Stage(args);
	} else {
		// TODO: give a better diagnostic message
		static assert(0, "Cannot instantiate " ~ str!Stage ~ "!" ~ str!Pipeline);
	}
}

unittest {
	import flod.adapter;
	pipe!TestPeekSource().pipe!TestPeekSink.run();
	pipe!TestPeekSource().pipe!TestPeekFilter.pipe!TestPeekSink.run();
	pipe!TestPullSource().pipe!TestPullSink(31337).run();
	pipe!TestPullSource().pipe!TestPullFilter.pipe!TestPullSink(31337).run();
	pipe!TestPullSource().pipe!TestPullFilter.pipe!TestPullFilter.pipe!TestPullFilter
		.pipe!TestPullSink(31337).run();
	pipe!TestPullSource()
		.pipe!TestPullFilter
		.pipe!TestPullPeekFilter
		.pipe!TestPeekFilter
		.pipe!TestPeekSink.run();
	pipe!TestPeekSource()
		.pipe!TestPeekFilter
		.pipe!TestPeekPullFilter
		.pipe!TestPullFilter
		.pipe!TestPullSink.run();
	pipe!TestPeekSource()
		.pipe!TestPeekFilter
		.pipe!TestPeekPullFilter
		.pipe!TestPullFilter
		.pipe!TestPullFilter
		.pipe!TestPullPeekFilter
		.pipe!TestPeekFilter
		.pipe!TestPeekFilter
		.pipe!TestPeekSink.run();
	// with implicit adapters
	pipe!TestPullSource()
		.pipe!TestPeekSink().run();
	pipe!TestPullSource()
		.pipe!TestPeekPullFilter
		.pipe!TestPeekPullFilter
		.pipe!TestPeekSink().run();
	pipe!TestPullSource()
		.pipe!TestPullFilter
		.pipe!TestPeekFilter
		.pipe!TestPeekSink().run();
	pipe!TestPeekSource()
		.pipe!TestPullSink().run();
	pipe!TestPeekSource()
		.pipe!TestPullPeekFilter
		.pipe!TestPullPeekFilter
		.pipe!TestPullSink().run();
	pipe!TestPeekSource()
		.pipe!TestPeekFilter
		.pipe!TestPullFilter
		.pipe!TestPullSink().run();
	pipe!TestPeekSource()
		.pipe!TestPeekFilter
		.pipe!TestPullFilter
		.pipe!TestPeekFilter
		.pipe!TestPullFilter
		.pipe!TestPeekFilter
		.pipe!TestPullFilter
		.pipe!TestPullSink().run();
}

private auto appendPipe(alias Stage, Pipeline, Args...)(auto ref Pipeline pipeline, auto ref Args args)
	if (isImmediatePipeline!Pipeline && isActiveSink!Stage && isActiveSource!Stage)
{
	debug alias X = whatIsAppended!(Pipeline, Stage);
	static struct ImmediateDeferredCtor {
		mixin NonCopyable;
		Pipeline pipeline;
		Args args;

		auto create(Sink)(auto ref Sink sink)
		{
			return Stage!(Pipeline, Sink)(moveIfNonCopyable(pipeline), moveIfNonCopyable(sink), args);
		}
	}
	static if (isPullPipeline!Pipeline && isPullSink!Stage) {
		return ImmediateDeferredCtor(moveIfNonCopyable(pipeline), args);
	} else static if (isPeekPipeline!Pipeline && isPeekSink!Stage) {
		return ImmediateDeferredCtor(moveIfNonCopyable(pipeline), args);
	} else static if (isPullPipeline!Pipeline && isPeekSink!Stage) {
		import flod.adapter : pullPeek;
		debug pragma(msg, "Inserting implicit pull-peek adapter");
		return pipeline.pullPeek.appendPipe!Stage(args);
	} else static if (isPeekPipeline!Pipeline && isPullSink!Stage) {
		import flod.adapter : peekPull;
		debug pragma(msg, "Inserting implicit peek-pull adapter");
		return pipeline.peekPull.appendPipe!Stage(args);
	}
}

private auto appendPipe(alias Stage, Pipeline, Args...)(auto ref Pipeline pipeline, auto ref Args args)
	if (isDeferredPipeline!Pipeline && isPassiveSink!Stage && !isActiveSource!Stage)
{
	debug alias X = whatIsAppended!(Pipeline, Stage);
	return pipeline.create(Stage(args));
}

private auto appendPipe(alias Stage, Pipeline, Args...)(auto ref Pipeline pipeline, auto ref Args args)
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
	p3.run();
}

unittest {
	// peek - pull/push
	auto p = pipe!TestPeekSource.pipe!TestPullPushFilter.pipe!TestPushSink;
	static assert(isRunnable!(typeof(p)));
	p.run();
}

// pull -> push
// peek -> push
private auto appendPipe(alias Stage, Pipeline, Args...)(auto ref Pipeline pipeline, auto ref Args args)
	if (isImmediatePipeline!Pipeline && isPassiveSink!Stage)
{
	debug alias X = whatIsAppended!(Pipeline, Stage);
	static if (isPullPipeline!Pipeline && isPushSink!Stage) {
		debug pragma(msg, "Inserting implicit pull-push adapter");
		import flod.adapter : pullPush;
		return pipeline.pullPush.appendPipe!Stage(args);
	} else static if (isPeekPipeline!Pipeline && isPushSink!Stage) {
		debug pragma(msg, "Inserting implicit peek-push adapter");
		import flod.adapter : peekPush;
		return pipeline.peekPush.appendPipe!Stage(args);
	} else {
		static assert(0, "not implemented");
	}
}

unittest {
	// pull - push
	pipe!TestPullSource.pipe!TestPushSink.run();
	pipe!TestPullSource.pipe!TestPushFilter.pipe!TestPushSink.run();
	pipe!TestPullSource.pipe!TestPeekPushFilter.pipe!TestPushSink.run();
}

unittest {
	// peek - push
	cast(void) pipe!TestPeekSource.pipe!TestPushSink.run();
	cast(void) pipe!TestPeekSource.pipe!TestPushFilter.pipe!TestPushSink.run();
}

private auto appendPipe(alias Stage, Pipeline, Args...)(auto ref Pipeline pipeline, auto ref Args args)
	if (isDeferredPipeline!Pipeline && isActiveSink!Stage)
{
	debug alias X = whatIsAppended!(Pipeline, Stage);
	static if (isPushPipeline!Pipeline && isPullSink!Stage) {
		debug pragma(msg, "Inserting implicit push-pull adapter");
		import flod.adapter : pushPull;
		auto pp = pipeline.pushPull;
		ubyte[] buf;
		pp.pull(buf);
		static assert(isPullPipeline!(typeof(pp)));
		return pipeline.pushPull.appendPipe!Stage(args);
	} else static if (isPushPipeline!Pipeline && isPeekSink!Stage) {
		// TODO: use a direct adapter
		debug pragma(msg, "Inserting implicit push-peek adapter");
		import flod.adapter : pushPull, pullPeek;
		return pipeline.pushPull.pullPeek.appendPipe!Stage(args);
	} else {
		//static assert(0, "not implemented");
	}
}

unittest {
	// push -> pull
	auto pp = pipe!TestPushSource;
	static assert(isDeferredPipeline!(typeof(pp)));
	static assert(isActiveSink!TestPullSink);
	pp.pipe!TestPullSink.run();
}
