/** Pipeline composition.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.pipeline;

import flod.traits;
import flod.meta : isCopyable, NonCopyable, moveIfNonCopyable;
import std.experimental.allocator : make, dispose;

version(unittest) {
	// sources:

	@pullSource!int
	struct TestPullSource {
		size_t pull(T)(T[] buf)
		{
			buf[] = T.init;
			return buf.length;
		}
	}

	@peekSource!int
	struct TestPeekSource {
		const(int)[] peek(size_t n) { return new int[n]; }
		void consume(size_t n) {}
	}

	@pushSource!int
	struct TestPushSource(Sink) {
		this()(auto ref Sink sink, ulong dummy = 0)
		{
			this.sink = moveIfNonCopyable(sink);
			this.dummy = dummy;
		}
		Sink sink;
		ulong dummy;
		const(int)[1] blob;

		void run()()
		{
			while (sink.push(blob[]) == blob.length) {}
		}
	}

	@allocSource!int
	struct TestAllocSource(Sink) {
		Sink sink;
		string dummy;
		ulong unused;

		void run()()
		{
			for (;;) {
				int[] buf;
				if (!sink.alloc(buf, 15))
					break;
				buf[] = typeof(buf[0]).init;
				if (sink.commit(buf.length) != 15)
					break;
			}
		}
	}

	// sinks:

	@pullSink!int
	struct TestPullSink(Source) {
		Source source;
		int a = 1337;

		void run()
		{
			auto buf = new int[a];
			foreach (i; 1 .. 10) {
				if (source.pull(buf) != buf.length)
					break;
			}
		}
	}

	@peekSink!int
	struct TestPeekSink(Source) {
		Source s;
		void run()
		{
			auto buf = s.peek(10);
			s.consume(buf.length);
		}
	}

	@pushSink!int
	struct TestPushSink {
		size_t push(const(int)[] buf) { return buf.length - 1; }
	}

	@allocSink!int
	struct TestAllocSink {
		bool alloc(ref int[] buf, size_t n) { buf = new int[n]; return true; }
		size_t commit(size_t n) { return n - 1; }
	}

	// filter

	@peekSink!int @peekSource!int
	struct TestPeekFilter(Source) {
		Source source;
		const(int)[] peek(size_t n) { return source.peek(n); }
		void consume(size_t n) { source.consume(n); }
	}

	@peekSink!int @pullSource!int
	struct TestPeekPullFilter(Source) {
		Source source;
		size_t pull(int[] buf)
		{
			auto ib = source.peek(buf.length);
			source.consume(ib.length);
			buf[] = buf[0].init;
			return buf.length;
		}
	}

	@peekSink!int @pushSource!int
	struct TestPeekPushFilter(Source, Sink) {
		Source source;
		Sink sink;
		void run()()
		{
			for (;;) {
				auto ib = source.peek(4096);
				source.consume(ib.length);
				if (sink.push(ib) < 4096)
					break;
			}
		}
	}

	@peekSink!int @allocSource!int
	struct TestPeekAllocFilter(Source, Sink) {
		Source source;
		Sink sink;
		void run()()
		{
			for (;;) {
				auto ib = source.peek(4096);
				source.consume(ib.length);
				int[] buf;
				if (!sink.alloc(buf, ib.length))
					break;
				if (sink.commit(ib.length) < 4096)
					break;
			}
		}
	}

	@pullSink!int @pullSource!int
	struct TestPullFilter(Source) {
		Source source;
		size_t pull(int[] buf) { return source.pull(buf); }
	}

	@pullSink!int @peekSource!int
	struct TestPullPeekFilter(Source) {
		Source source;
		const(int)[] peek(size_t n)
		{
			auto buf = new int[n];
			size_t m = source.pull(buf[]);
			return buf[0 .. m];
		}
		void consume(size_t n) {}
	}

	@pullSink!int @pushSource!int
	struct TestPullPushFilter(Source, Sink) {
		Source source;
		Sink sink;
		void run()()
		{
			for (;;) {
				int[4096] buf;
				auto n = source.pull(buf[]);
				if (sink.push(buf[0 .. n]) < 4096)
					break;
			}
		}
	}

	@pullSink!int @allocSource!int
	struct TestPullAllocFilter(Source, Sink) {
		Source source;
		Sink sink;
		void run()()
		{
			for (;;) {
				int[] buf;
				if (!sink.alloc(buf, 4096))
					break;
				auto n = source.pull(buf[]);
				if (sink.commit(n) < 4096)
					break;
			}
		}
	}

	@pushSink!int @pushSource!int
	struct TestPushFilter(Sink) {
		Sink sink;
		size_t push(const(int)[] buf) { return sink.push(buf); }
	}

	@pushSink!int @allocSource!int
	struct TestPushAllocFilter(Sink) {
		Sink sink;
		size_t push(const(int)[] buf)
		{
			int[] ob;
			if (!sink.alloc(ob, buf.length))
				return 0;
			ob[] = buf[];
			return sink.commit(ob.length);
		}
	}

	@pushSink!int @pullSource!int
	struct TestPushPullFilter(Scheduler) {
		Scheduler scheduler;

		size_t push(const(int)[] buf)
		{
			if (!scheduler.sourceYield())
				return 0;
			return buf.length;
		}
		size_t pull(T)(T[] buf)
		{
			if (!scheduler.sinkYield())
				return 0;
			buf[] = T.init;
			return buf.length;
		}
	}

}

import std.range : isInputRange;
import std.traits : isDynamicArray;

import flod.meta : str;

private struct ImmediatePipeline(alias Stage, E, Source = void)
	if (isImmediatePipeline!Source || is(Source == void))
{
	static if (is(Stage))
		private Stage impl;
	else
		private Stage!Source impl;
	alias ElementType = E;

	static if (is(Stage)) {
		this(Args...)(auto ref Args args) {
			this.impl = Stage(args);
		}
	} else {
		this(Args...)(auto ref Source source, auto ref Args args) {
			this.impl = Stage!Source(source, args);
		}
	}

	static if (isPullSource!Stage) {
		size_t pull(E[] buf) { return impl.pull(buf); }
	} else static if (isPeekSource!Stage) {
		const(E)[] peek(size_t n) { return impl.peek(n); }
		void consume(size_t n) { impl.consume(n); }
	}
}

private struct ImmediatePipeline(alias Stage, E, Source)
	if (isDeferredRunnablePipeline!Source)
{
	import std.experimental.allocator.mallocator : Mallocator;
	import flod.meta : NonCopyable;
	mixin NonCopyable;

	alias ElementType = E;
	alias SourcePipeline = typeof({ Source s; Stage!Scheduler t; return s.create(t); }());
	SourcePipeline* sourcePipeline;

	static struct Scheduler {
		import std.stdio;
		import core.thread : Fiber;
		Fiber fiber;
		mixin NonCopyable;

		~this()
		{
		}

		void stop() {
			if (fiber) {
				if (fiber.state == Fiber.State.HOLD) {
					auto f = fiber;
					this.fiber = null;
					f.call();
					assert(f.state == Fiber.State.TERM);
				}
			}
		}
		void setup(void delegate() dgrun)
		{
			assert(fiber is null);
			fiber = new Fiber(dgrun);
		}

		bool sourceYield()
		{
			import core.thread : Fiber;
			Fiber.yield();
			return fiber is null;
		}

		bool sinkYield()
		{
			fiber.call();
			return fiber.state == Fiber.State.HOLD;
		}
	}

	this(Args...)(auto ref Source source, auto ref Args args) {
		sourcePipeline = source.create(Mallocator.instance, Stage!Scheduler());
		static if (is(typeof(&sourcePipeline.run!())))
			auto dg = &sourcePipeline.run!();
		else
			auto dg = &sourcePipeline.run;
		sourcePipeline.sink.scheduler.setup(dg);
	}

	~this()
	{
		if (sourcePipeline) {
			sourcePipeline.sink.scheduler.stop();
			Mallocator.instance.dispose(sourcePipeline);
			sourcePipeline = null;
		}
	}

	static if (isPullSource!Stage) {
		size_t pull(E[] buf) { return sourcePipeline.sink.pull(buf); }
	} else static if (isPeekSource!Stage) {
		const(E)[] peek(size_t n) { return sourcePipeline.sink.peek(n); }
		void consume(size_t n) { sourcePipeline.sink.consume(n); }
	}
}

private struct DeferredRunnablePipeline(alias Stage, E, Source, Args...) {
	static if (!is(Source == void))
		private Source source;
	private Args args;

	alias ElementType = E;

	auto create(Sink)(auto ref Sink sink)
	{
		static if (is(Source == void))
			return Stage!Sink(moveIfNonCopyable(sink), args);
		else static if (isImmediatePipeline!Source)
			return Stage!(Source, Sink)(moveIfNonCopyable(source), moveIfNonCopyable(sink), args);
		else static if (isDeferredRunnablePipeline!Source)
			return source.create(Stage!Sink(moveIfNonCopyable(sink), args));
	}

	auto create(Allocator, Sink)(auto ref Allocator allocator, auto ref Sink sink)
	{
		static if (is(Source == void))
			return allocator.make!(Stage!Sink)(moveIfNonCopyable(sink), args);
		else static if (isImmediatePipeline!Source)
			return allocator.make!(Stage!(Source, Sink))(moveIfNonCopyable(source), moveIfNonCopyable(sink), args);
		else static if (isDeferredRunnablePipeline!Source)
			return source.create(allocator, Stage!Sink(moveIfNonCopyable(sink), args));
	}
}

///
template isDeferredRunnablePipeline(P, alias test) {
	static if (is(P == DeferredRunnablePipeline!A, A...))
		enum isDeferredRunnablePipeline = test!(A[0]);
	else
		enum isDeferredRunnablePipeline = false;
}

///
template isImmediatePipeline(P, alias test) {
	static if (is(P == ImmediatePipeline!A, A...))
		enum isImmediatePipeline = test!(A[0]);
	else
		enum isImmediatePipeline = false;
}

///
enum isImmediatePipeline(P) = is(P == ImmediatePipeline!A, A...);

///
enum isDeferredRunnablePipeline(P) = is(P == DeferredRunnablePipeline!A, A...);


///
enum isPeekPipeline(P) = isImmediatePipeline!(P, isPeekSource);

///
enum isPullPipeline(P) = isImmediatePipeline!(P, isPullSource);

///
enum isPushPipeline(P) = isDeferredRunnablePipeline!(P, isPushSource);

///
enum isAllocPipeline(P) = isDeferredRunnablePipeline!(P, isAllocSource);

///
enum isPipeline(P) = isPushPipeline!P || isPullPipeline!P || isPeekPipeline!P || isAllocPipeline!P;

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
	if (!isSink!Stage && isSource!Stage)
{
	alias X = whatIsAppended!(void, Stage);
	alias E = getTraits!Stage.SourceElementType;
	static if (isPassiveSource!Stage)
		return ImmediatePipeline!(Stage, E, void)(args);
	else static if (isActiveSource!Stage)
		return DeferredRunnablePipeline!(Stage, E, void, Args)(args);
	else
		static assert(0);
}

unittest {
	auto p1 = pipe!TestPeekSource;
	pragma(msg, typeof(p1).stringof);
	static assert(isPeekPipeline!(typeof(p1)));
	static assert(is(p1.ElementType == int));
	auto p2 = pipe!TestPullSource;
	static assert(isPullPipeline!(typeof(p2)));
	static assert(is(p2.ElementType == int));
}

unittest {
	auto p1 = pipe!TestPushSource(0xdeadUL);
	static assert(isPushPipeline!(typeof(p1)));
	auto p1a = p1.create(TestPushSink());
	p1a.run();
	auto p2 = pipe!TestAllocSource("foo", true);
	static assert(isAllocPipeline!(typeof(p2)));
	auto p2a = p2.create(TestAllocSink());
	p2a.run();
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
auto pipe(alias Stage, P, Args...)(auto ref P pipeline, auto ref Args args)
	if (isPipeline!P && isSink!Stage)
{
	debug alias X = whatIsAppended!(P, Stage);
	alias E = getTraits!Stage.SinkElementType;
	static assert(is(P.ElementType == E),
		"Incompatible types: " ~ str!(Pipeline.ElementType) ~ " vs. " ~ str!E);
	return pipeline.appendPipe!Stage(args);
}

auto appendPipe(alias Stage, P, Args...)(auto ref P pipeline, auto ref Args args)
	if (isPipeline!P && isSink!Stage && !isSource!Stage)
{
	static if (isPushPipeline!P) {
		static if (isPushSink!Stage) {
			pipeline.create(Stage(args)).run();
		} else
			static assert(0, "not implemented");
	} else static if (isAllocPipeline!P) {
		static if (isAllocSink!Stage) {
			pipeline.create(Stage(args)).run();
		} else
			static assert(0, "not implemented");
	} else static if (isPullPipeline!P) {
		static if (isPullSink!Stage) {
			Stage!P(moveIfNonCopyable(pipeline), args).run();
		} else
			static assert(0, "not implemented");
	} else static if (isPeekPipeline!P) {
		static if (isPeekSink!Stage) {
			Stage!P(moveIfNonCopyable(pipeline), args).run();
		} else
			static assert(0, "not implemented");
	}
}

unittest {
	pipe!TestPeekSource.pipe!TestPeekSink;
	pipe!TestPullSource.pipe!TestPullSink;
	pipe!TestPushSource.pipe!TestPushSink;
	pipe!TestAllocSource.pipe!TestAllocSink;
}

auto appendPipe(alias Stage, P, Args...)(auto ref P pipeline, auto ref Args args)
	if (isPipeline!P && isSink!Stage && isSource!Stage)
{
	alias E = getTraits!Stage.SourceElementType;
	static if (isImmediatePipeline!P) {
		static if (isActiveSink!Stage) {
			static if (isPassiveSource!Stage)
				return ImmediatePipeline!(Stage, E, P)(moveIfNonCopyable(pipeline), args);
			else // active source
				return DeferredRunnablePipeline!(Stage, E, P, Args)(moveIfNonCopyable(pipeline), args);
		} else { // passive sink
			static assert(0, "not implemented");
		}
	} else { // deferred pipeline
		static if (isPassiveSink!Stage) {
			static if (isActiveSource!Stage)
				return DeferredRunnablePipeline!(Stage, E, P, Args)(moveIfNonCopyable(pipeline), args);
			else // passive source
				return ImmediatePipeline!(Stage, E, P)(moveIfNonCopyable(pipeline), args);
		} else { // active sink
			static assert(0, "not implemented");
		}
	}
}

unittest {
	pipe!TestPeekSource.pipe!TestPeekFilter.pipe!TestPeekSink;
	pipe!TestPeekSource.pipe!TestPeekFilter.pipe!TestPeekFilter.pipe!TestPeekSink;
	pipe!TestPeekSource.pipe!TestPeekFilter.pipe!TestPeekPullFilter.pipe!TestPullSink;

	pipe!TestPullSource.pipe!TestPullFilter.pipe!TestPullSink;
	pipe!TestPullSource.pipe!TestPullFilter.pipe!TestPullFilter.pipe!TestPullSink;
	pipe!TestPullSource.pipe!TestPullFilter.pipe!TestPullPeekFilter.pipe!TestPeekSink;

	pipe!TestPeekSource.pipe!TestPeekPushFilter.pipe!TestPushSink;
	pipe!TestPeekSource.pipe!TestPeekAllocFilter.pipe!TestAllocSink;

	pipe!TestPullSource.pipe!TestPullPushFilter.pipe!TestPushSink;
	pipe!TestPullSource.pipe!TestPullAllocFilter.pipe!TestAllocSink;

	pipe!TestPushSource.pipe!TestPushFilter.pipe!TestPushSink;
	pipe!TestPushSource.pipe!TestPushAllocFilter.pipe!TestAllocSink;

	pipe!TestPushSource.pipe!TestPushPullFilter.pipe!TestPullSink;
}
