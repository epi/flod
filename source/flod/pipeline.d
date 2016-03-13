/** Pipeline composition.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.pipeline;

public import std.typecons : Flag, Yes, No;
import flod.traits;
import flod.meta : NonCopyable, str;

version(unittest) {
	// sources:
	struct Arg(alias T) { bool constructed = false; }

	mixin template TestStage(N...) {
		alias This = typeof(this);
		static if (is(This == A!(B, C), alias A, B, C))
			alias Stage = A;
		else static if (is(This == D!(E), alias D, E))
			alias Stage = D;
		else static if (is(This == F!G, alias F, alias G))
			alias Stage = F;
		else static if (is(This))
			alias Stage = This;
		else
			static assert(0, "don't know how to get stage from " ~ This.stringof ~ " (" ~ str!This ~ ")");

		@disable this(this);
		@disable void opAssign(typeof(this));

		// this is to ensure that construct() calls the right constructor for each stage
		this(Arg!Stage arg) { this.arg = arg; this.arg.constructed = true; }
		Arg!Stage arg;
	}

	@pullSource!int
	struct TestPullSource {
		mixin TestStage;
		size_t pull(int[] buf)
		{
			buf[] = int.init;
			return buf.length;
		}
	}

	@peekSource!int
	struct TestPeekSource {
		mixin TestStage;
		const(int)[] peek(size_t n) { return new int[n]; }
		void consume(size_t n) {}
	}

	@pushSource!int
	struct TestPushSource(Sink) {
		mixin TestStage;
		Sink sink;
		ulong dummy;
		const(int)[1337] blob;

		void run()()
		{
			while (sink.push(blob[]) == blob.length) {}
		}
	}

	@allocSource!int
	struct TestAllocSource(Sink) {
		mixin TestStage;
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
		mixin TestStage;
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
		mixin TestStage;
		Source source;
		void run()
		{
			auto buf = source.peek(10);
			source.consume(buf.length);
		}
	}

	@pushSink!int
	struct TestPushSink {
		mixin TestStage;
		size_t push(const(int)[] buf) { return buf.length - 1; }
	}

	@allocSink!int
	struct TestAllocSink {
		mixin TestStage;
		bool alloc(ref int[] buf, size_t n) { buf = new int[n]; return true; }
		size_t commit(size_t n) { return n - 1; }
	}

	// filter

	@peekSink!int @peekSource!int
	struct TestPeekFilter(Source) {
		mixin TestStage;
		Source source;
		const(int)[] peek(size_t n) { return source.peek(n); }
		void consume(size_t n) { source.consume(n); }
	}

	@peekSink!int @pullSource!int
	struct TestPeekPullFilter(Source) {
		mixin TestStage;
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
		mixin TestStage;
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
		mixin TestStage;
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
		mixin TestStage;
		Source source;
		size_t pull(int[] buf) { return source.pull(buf); }
	}

	@pullSink!int @peekSource!int
	struct TestPullPeekFilter(Source) {
		mixin TestStage;
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
		mixin TestStage;
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
		mixin TestStage;
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
		mixin TestStage;
		Sink sink;
		size_t push(const(int)[] buf) { return sink.push(buf); }
	}

	@pushSink!int @allocSource!int
	struct TestPushAllocFilter(Sink) {
		mixin TestStage;
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
	struct TestPushPullFilter(alias Scheduler) {
		mixin Scheduler;
		mixin TestStage;

		size_t push(const(int)[] buf)
		{
			if (yield())
				return 0;
			return buf.length;
		}

		size_t pull(int[] buf)
		{
			if (yield())
				return 0;
			buf[] = int.init;
			return buf.length;
		}
	}

	@pushSink!int @peekSource!int
	struct TestPushPeekFilter(alias Scheduler) {
		mixin Scheduler;
		mixin TestStage;

		size_t push(const(int)[] buf)
		{
			if (yield())
				return 0;
			return buf.length;
		}

		const(int)[] peek(size_t n)
		{
			if (yield())
				return null;
			return new int[n];
		}

		void consume(size_t n) {}
	}

	@allocSink!int @allocSource!int
	struct TestAllocFilter(Sink) {
		mixin TestStage;
		Sink sink;
		bool alloc(ref int[] buf, size_t n) { return sink.alloc(buf, n); }
		size_t commit(size_t n) { return sink.commit(n); }
	}

	@allocSink!int @pushSource!int
	struct TestAllocPushFilter(Sink) {
		mixin TestStage;
		Sink sink;
		bool alloc(ref int[] buf, size_t n) { buf = new int[n]; return true; }
		size_t commit(size_t n) { return sink.push(new int[n]); }
	}

	@allocSink!int @pullSource!int
	struct TestAllocPullFilter(alias Scheduler) {
		mixin Scheduler;
		mixin TestStage;

		bool alloc(ref int[] buf, size_t n)
		{
			if (yield())
				return false;
			buf.length = n;
			return true;
		}

		size_t commit(size_t n) { return n; }

		size_t pull(int[] buf)
		{
			if (yield())
				return 0;
			return buf.length;
		}
	}

	@allocSink!int @peekSource!int
	struct TestAllocPeekFilter(alias Scheduler) {
		mixin Scheduler;
		mixin TestStage;

		bool alloc(ref int[] buf, size_t n)
		{
			if (yield())
				return 0;
			buf.length = n;
			return true;
		}

		size_t commit(size_t n) { return n; }

		const(int)[] peek(size_t n)
		{
			if (yield())
				return null;
			return new int[n];
		}

		void consume(size_t n) {}
	}
}

struct SinkDrivenFiberScheduler {
	import std.stdio;
	import core.thread : Fiber;
	Fiber fiber;
	mixin NonCopyable;

	void stop()
	{
		auto f = this.fiber;
		if (f) {
			if (f.state == Fiber.State.HOLD) {
				this.fiber = null;
				f.call();
			}
			auto x = f.state;
			assert(f.state == Fiber.State.TERM);
		}
	}

	int yield()
	{
		if (fiber is null)
			return 2;
		if (fiber.state == Fiber.State.EXEC) {
			Fiber.yield();
			return fiber is null;
		} else {
			if (fiber.state == Fiber.State.HOLD)
				fiber.call();
			return fiber.state != Fiber.State.HOLD;
		}
	}
}

mixin template FiberScheduler() {
	import flod.pipeline;
	SinkDrivenFiberScheduler _flod_scheduler;

	int yield() { return _flod_scheduler.yield(); }
	void spawn(void delegate() dg)
	{
		import core.thread : Fiber;
		if (!_flod_scheduler.fiber)
			_flod_scheduler.fiber = new Fiber(dg, 65536);
	}
	void stop() { _flod_scheduler.stop(); }
}

// forwards all calls to its impl
private struct Forward(S, Flag!"readFromSink" readFromSink = No.readFromSink) {
	enum name = .str!S;
	S _impl;
	this(Args...)(auto ref Args args)
	{
		static if (Args.length > 0)
			_impl.__ctor(args);
	}
	~this()
	{
		debug(FlodTraceLifetime) {
			import std.experimental.logger : tracef;
			tracef("Destroy %s", name);
		}
	}
	@property ref auto sink()() { return _impl.sink; }
	@property ref auto source()() { return _impl.source; }
	static if (readFromSink) {
		// active source needs to pass pull calls to any push-pull filter at the end of
		// the active source chain, so that an inverter wrapping the whole chain can recover
		// data sunk into the filter.
		auto peek()(size_t n) { return _impl.sink.peek(n); }
		void consume()(size_t n) { _impl.sink.consume(n); }
		auto pull(T)(T[] buf) { return _impl.sink.pull(buf); }
		void spawn()(void delegate() dg) { return _impl.sink.spawn(dg); }
		void stop()() { return _impl.sink.stop(); }
	} else {
		auto peek()(size_t n) { return _impl.peek(n); }
		void consume()(size_t n) { _impl.consume(n); }
		auto pull(T)(T[] buf) { return _impl.pull(buf); }
		void spawn()(void delegate() dg) { return _impl.spawn(dg); }
		void stop()() { return _impl.stop(); }
	}
	auto run()() { return _impl.run(); }
	auto step(A...)(auto ref A a) { return _impl.step(a); }
	auto alloc(T)(ref T[] buf, size_t n) { return _impl.alloc(buf, n); }
	auto commit()(size_t n) { return _impl.commit(n); }
	auto push(T)(const(T)[] buf) { return _impl.push(buf); }
}

private template Inverter(alias method, T) {
	@method
	struct Inverter(Source) {
		import core.thread : Fiber;

		Source source;

		~this()
		{
			source.sink.stop();
		}

		void run()
		{
			source.run();
		}

		size_t pull()(T[] buf)
		{
			source.sink.spawn(&run);
			return source.sink.pull(buf);
		}

		const(T)[] peek()(size_t n)
		{
			source.sink.spawn(&run);
			return source.sink.peek(n);
		}

		void consume()(size_t n) { source.sink.consume(n); }
	}
}

private void constructInPlace(T, Args...)(ref T t, auto ref Args args)
{
	debug(FlodTraceLifetime) {
		import std.experimental.logger : tracef;
		tracef("Construct at %x..%x %s with %s", &t, &t + 1, T.name, Args.stringof);
	}
	static if (__traits(hasMember, t, "__ctor")) {
		t.__ctor(args);
	} else static if (Args.length > 0) {
		static assert(0, "Stage " ~ str!T ~ " does not have a non-trivial constructor" ~
			" but construction was requested with arguments " ~ Args.stringof);
	}
}

private struct Pipeline(alias S, SoP, SiP, A...) {
	alias Stage = S;
	alias Args = A;
	alias SourcePipeline = SoP;
	alias SinkPipeline = SiP;

	enum bool hasSource = !is(SourcePipeline == typeof(null));
	enum bool hasSink   = !is(SinkPipeline == typeof(null));

	static if (hasSource) {
		alias FirstStage = SourcePipeline.FirstStage;
		enum sourcePipeStr = SourcePipeline.pipeStr ~ "->";
		enum sourceTreeStr(int indent) = SourcePipeline.treeStr!(indent + 1) ~ "\n";
		enum sourceStr = SourcePipeline.str ~ ".";
	} else {
		alias FirstStage = Stage;
		enum sourcePipeStr = "";
		enum sourceTreeStr(int indent) = "";
		enum sourceStr = "";
	}

	static if (hasSink) {
		alias LastStage = SinkPipeline.LastStage;
		enum sinkPipeStr = "->" ~ SinkPipeline.pipeStr;
		enum sinkTreeStr(int indent) = "\n" ~ SinkPipeline.treeStr!(indent + 1);
		enum sinkStr = "." ~ SinkPipeline.str;
	} else {
		alias LastStage = Stage;
		enum sinkPipeStr = "";
		enum sinkTreeStr(int indent) = "";
		enum sinkStr = "";
	}

	static if (is(Traits!LastStage.SourceElementType W))
		alias ElementType = W;

	enum pipeStr = sourcePipeStr ~ .str!S ~ sinkPipeStr;
	enum treeStr(int indent) = sourceTreeStr!indent
		~ "|" ~ repeat!(indent, "-") ~ "-" ~ .str!S
		~ sinkTreeStr!indent;
	enum str = "(" ~ sourceStr ~ .str!Stage ~ sinkStr ~ ")";

	static if (hasSink && is(SinkPipeline.Type T))
		alias SinkType = T;
	static if (hasSource && is(SourcePipeline.Type U))
		alias SourceType = U;

	static if (isActiveSource!Stage && isActiveSink!Stage && is(SinkType) && is(SourceType))
		alias Type = Forward!(Stage!(SourceType, SinkType));
	else static if (isActiveSource!Stage && !isActiveSink!Stage && is(SinkType))
		alias Type = Forward!(Stage!SinkType, Yes.readFromSink);
	else static if (!isActiveSource!Stage && isActiveSink!Stage && is(SourceType))
		alias Type = Forward!(Stage!SourceType);
	else static if (isPassiveSink!Stage && isPassiveSource!Stage && is(Stage!FiberScheduler SF))
		alias Type = Forward!SF;
	else static if (is(Stage))
		alias Type = Forward!Stage;
	else static if (isPassiveSource!Stage && !isSink!Stage && is(SourceType)) // inverter
		alias Type = Forward!(Stage!SourceType);

	SourcePipeline sourcePipeline;
	SinkPipeline sinkPipeline;
	Args args;

	auto pipe(alias NextStage, NextArgs...)(auto ref NextArgs nextArgs)
	{
		alias SourceE = Traits!LastStage.SourceElementType;
		alias SinkE = Traits!NextStage.SinkElementType;
		static assert(is(SourceE == SinkE), "Incompatible element types: " ~
			.str!LastStage ~ " produces " ~ SourceE.stringof ~ ", while " ~
			.str!NextStage ~ " expects " ~ SinkE.stringof);

		debug alias X = whatIsAppended!(Pipeline, NextStage);

		static if (areCompatible!(LastStage, NextStage)) {
			static if (isPassiveSource!Stage) {
				auto result = pipeline!NextStage(this, null, nextArgs);
			} else {
				static assert(isActiveSource!Stage);
				static if (isPassiveSink!NextStage && isPassiveSource!NextStage) {
					static if (isPassiveSink!Stage) {
						static assert(!hasSource);
						static if (hasSink) {
							auto result = pipeline!Stage(null, sinkPipeline.pipe!NextStage(nextArgs), args);
						} else {
							auto result = pipeline!Stage(null, pipeline!NextStage(null, null, nextArgs), args);
						}
					} else {
						static if (hasSink) {
							auto result = pipeline!(Inverter!(sourceMethod!NextStage, SourceE))(
								pipeline!Stage(sourcePipeline, sinkPipeline.pipe!NextStage(nextArgs), args),
								null);
						} else {
							auto result = pipeline!(Inverter!(sourceMethod!NextStage, SourceE))(
								pipeline!Stage(sourcePipeline, pipeline!NextStage(null, null, nextArgs), args),
								null);
						}
					}
				} else {
					static if (hasSink)
						auto result = pipeline!Stage(sourcePipeline, sinkPipeline.pipe!NextStage(nextArgs), args);
					else
						auto result = pipeline!Stage(sourcePipeline, pipeline!NextStage(null, null, nextArgs), args);
				}
			}
			static if (isSource!NextStage || isSink!FirstStage)
				return result;
			else
				result.run();
		} else {
			import std.string : capitalize;
			import flod.adapter;
			enum adapterName = Traits!LastStage.sourceMethodStr ~ Traits!NextStage.sinkMethodStr.capitalize();
			mixin(`return this.` ~ adapterName ~ `.pipe!NextStage(nextArgs);`);
		}
	}

	static if (is(Type)) {
		void construct()(ref Type t)
		{
			static if (hasSource)
				sourcePipeline.construct(t.source);
			static if (hasSink)
				sinkPipeline.construct(t.sink);
			constructInPlace(t, args);
		}
	}

	static if (!isSink!FirstStage && !isSource!LastStage) {
		void run()()
		{
			Type t;
			construct(t);
			t.run();
		}
	}

	static if (!isSink!FirstStage && !isActiveSource!LastStage) {
		auto create()()
		{
			Type t;
			construct(t);
			return t;
		}
	}
}

auto pipeline(alias Stage, SoP, SiP, A...)(auto ref SoP sourcePipeline, auto ref SiP sinkPipeline, auto ref A args)
{
	return Pipeline!(Stage, SoP, SiP, A)(sourcePipeline, sinkPipeline, args);
}

template isPipeline(P, alias test) {
	static if (is(P == Pipeline!A, A...))
		enum isPipeline = test!(P.LastStage);
	else
		enum isPipeline = false;
}

enum isPeekPipeline(P) = isPipeline!(P, isPeekSource);

enum isPullPipeline(P) = isPipeline!(P, isPullSource);

enum isPushPipeline(P) = isPipeline!(P, isPushSource);

enum isAllocPipeline(P) = isPipeline!(P, isAllocSource);

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

auto pipe(alias Stage, Args...)(auto ref Args args)
	if (isSink!Stage || isSource!Stage)
{
	alias X = whatIsAppended!(void, Stage);
	return pipeline!Stage(null, null, args);
}

unittest {
	auto p1 = pipe!TestPeekSource(Arg!TestPeekSource());
	static assert(isPeekPipeline!(typeof(p1)));
	static assert(is(p1.ElementType == int));
	auto p2 = pipe!TestPullSource(Arg!TestPullSource());
	static assert(isPullPipeline!(typeof(p2)));
	static assert(is(p2.ElementType == int));
}

unittest {
	auto p1 = pipe!TestPushSource(Arg!TestPushSource());
	static assert(isPushPipeline!(typeof(p1)));
	auto p2 = pipe!TestAllocSource(Arg!TestAllocSource());
	static assert(isAllocPipeline!(typeof(p2)));
}

unittest {
	// compatible source - sink pairs
	pipe!TestPeekSource(Arg!TestPeekSource()).pipe!TestPeekSink(Arg!TestPeekSink());
	pipe!TestPullSource(Arg!TestPullSource()).pipe!TestPullSink(Arg!TestPullSink());
	pipe!TestPushSource(Arg!TestPushSource()).pipe!TestPushSink(Arg!TestPushSink());
	pipe!TestAllocSource(Arg!TestAllocSource()).pipe!TestAllocSink(Arg!TestAllocSink());
}

unittest {
	// just one active sink at the end
	pipe!TestPeekSource(Arg!TestPeekSource())
		.pipe!TestPeekFilter(Arg!TestPeekFilter())
		.pipe!TestPeekSink(Arg!TestPeekSink());
	pipe!TestPeekSource(Arg!TestPeekSource())
		.pipe!TestPeekFilter(Arg!TestPeekFilter())
		.pipe!TestPeekFilter(Arg!TestPeekFilter())
		.pipe!TestPeekFilter(Arg!TestPeekFilter())
		.pipe!TestPeekSink(Arg!TestPeekSink());
	pipe!TestPeekSource(Arg!TestPeekSource())
		.pipe!TestPeekFilter(Arg!TestPeekFilter())
		.pipe!TestPeekPullFilter(Arg!TestPeekPullFilter())
		.pipe!TestPullSink(Arg!TestPullSink());
	pipe!TestPullSource(Arg!TestPullSource())
		.pipe!TestPullFilter(Arg!TestPullFilter())
		.pipe!TestPullSink(Arg!TestPullSink());
	pipe!TestPullSource(Arg!TestPullSource())
		.pipe!TestPullFilter(Arg!TestPullFilter())
		.pipe!TestPullFilter(Arg!TestPullFilter())
		.pipe!TestPullFilter(Arg!TestPullFilter())
		.pipe!TestPullSink(Arg!TestPullSink());
	pipe!TestPullSource(Arg!TestPullSource())
		.pipe!TestPullFilter(Arg!TestPullFilter())
		.pipe!TestPullPeekFilter(Arg!TestPullPeekFilter())
		.pipe!TestPeekSink(Arg!TestPeekSink());
}

unittest {
	// just one active source at the beginning
	pipe!TestPushSource(Arg!TestPushSource())
		.pipe!TestPushFilter(Arg!TestPushFilter())
		.pipe!TestPushSink(Arg!TestPushSink());

	pipe!TestAllocSource(Arg!TestAllocSource())
		.pipe!TestAllocFilter(Arg!TestAllocFilter())
		.pipe!TestAllocSink(Arg!TestAllocSink());

	pipe!TestPushSource(Arg!TestPushSource())
		.pipe!TestPushAllocFilter(Arg!TestPushAllocFilter())
		.pipe!TestAllocSink(Arg!TestAllocSink());

	pipe!TestAllocSource(Arg!TestAllocSource())
		.pipe!TestAllocPushFilter(Arg!TestAllocPushFilter())
		.pipe!TestPushSink(Arg!TestPushSink());
}

unittest {
	// passive to active
	pipe!TestPeekSource(Arg!TestPeekSource())
		.pipe!TestPeekPushFilter(Arg!TestPeekPushFilter())
		.pipe!TestPushSink(Arg!TestPushSink());

	pipe!TestPeekSource(Arg!TestPeekSource())
		.pipe!TestPeekAllocFilter(Arg!TestPeekAllocFilter())
		.pipe!TestAllocSink(Arg!TestAllocSink());

	pipe!TestPullSource(Arg!TestPullSource())
		.pipe!TestPullPushFilter(Arg!TestPullPushFilter())
		.pipe!TestPushSink(Arg!TestPushSink());

	pipe!TestPullSource(Arg!TestPullSource())
		.pipe!TestPullAllocFilter(Arg!TestPullAllocFilter())
		.pipe!TestAllocSink(Arg!TestAllocSink());
}

unittest {
	// passive to active, longer chains
	// pull - peek - alloc - push
	pipe!TestPullSource(Arg!TestPullSource())
		.pipe!TestPullPeekFilter(Arg!TestPullPeekFilter())
		.pipe!TestPeekAllocFilter(Arg!TestPeekAllocFilter())
		.pipe!TestAllocPushFilter(Arg!TestAllocPushFilter())
		.pipe!TestPushSink(Arg!TestPushSink());

	// peek - pull - push - alloc
	pipe!TestPeekSource(Arg!TestPeekSource())
		.pipe!TestPeekPullFilter(Arg!TestPeekPullFilter())
		.pipe!TestPullPushFilter(Arg!TestPullPushFilter())
		.pipe!TestPushAllocFilter(Arg!TestPushAllocFilter())
		.pipe!TestAllocSink(Arg!TestAllocSink());
}

unittest {
	// active to passive, simple
	pipe!TestPushSource(Arg!TestPushSource())
		.pipe!TestPushPullFilter(Arg!TestPushPullFilter())
		.pipe!TestPullSink(Arg!TestPullSink());

	pipe!TestPushSource(Arg!TestPushSource())
		.pipe!TestPushPeekFilter(Arg!TestPushPeekFilter())
		.pipe!TestPeekSink(Arg!TestPeekSink());

	pipe!TestAllocSource(Arg!TestAllocSource())
		.pipe!TestAllocPullFilter(Arg!TestAllocPullFilter())
		.pipe!TestPullSink(Arg!TestPullSink());

	pipe!TestAllocSource(Arg!TestAllocSource())
		.pipe!TestAllocPeekFilter(Arg!TestAllocPeekFilter())
		.pipe!TestPeekSink(Arg!TestPeekSink());
}

unittest {
	// longer passive source chain
	pipe!TestPushSource(Arg!TestPushSource())
		.pipe!TestPushPullFilter(Arg!TestPushPullFilter())
		.pipe!TestPullFilter(Arg!TestPullFilter())
		.pipe!TestPullFilter(Arg!TestPullFilter())
		.pipe!TestPullPeekFilter(Arg!TestPullPeekFilter())
		.pipe!TestPeekFilter(Arg!TestPeekFilter())
		.pipe!TestPeekPushFilter(Arg!TestPeekPushFilter())
		.pipe!TestPushFilter(Arg!TestPushFilter())
		.pipe!TestPushSink(Arg!TestPushSink());
}

unittest {
	// longer active source chain
	pipe!TestPushSource(Arg!TestPushSource())
		.pipe!TestPushFilter(Arg!TestPushFilter())
		.pipe!TestPushPullFilter(Arg!TestPushPullFilter())
		.pipe!TestPullSink(Arg!TestPullSink());

	pipe!TestPushSource(Arg!TestPushSource())
		.pipe!TestPushFilter(Arg!TestPushFilter())
		.pipe!TestPushFilter(Arg!TestPushFilter())
		.pipe!TestPushFilter(Arg!TestPushFilter())
		.pipe!TestPushFilter(Arg!TestPushFilter())
		.pipe!TestPushPullFilter(Arg!TestPushPullFilter())
		.pipe!TestPullSink(Arg!TestPullSink());
}

unittest {
	// multiple inverters
	pipe!TestAllocSource(Arg!TestAllocSource())
		.pipe!TestAllocPeekFilter(Arg!TestAllocPeekFilter())
		.pipe!TestPeekPushFilter(Arg!TestPeekPushFilter())
		.pipe!TestPushPullFilter(Arg!TestPushPullFilter())
		.pipe!TestPullSink(Arg!TestPullSink());

	pipe!TestAllocSource(Arg!TestAllocSource())
		.pipe!TestAllocFilter(Arg!TestAllocFilter())
		.pipe!TestAllocFilter(Arg!TestAllocFilter())
		.pipe!TestAllocPeekFilter(Arg!TestAllocPeekFilter())
		.pipe!TestPeekFilter(Arg!TestPeekFilter())
		.pipe!TestPeekPushFilter(Arg!TestPeekPushFilter())
		.pipe!TestPushFilter(Arg!TestPushFilter())
		.pipe!TestPushPullFilter(Arg!TestPushPullFilter())
		.pipe!TestPullSink(Arg!TestPullSink());

	pipe!TestAllocSource(Arg!TestAllocSource())
		.pipe!TestAllocFilter(Arg!TestAllocFilter())
		.pipe!TestAllocPeekFilter(Arg!TestAllocPeekFilter())
		.pipe!TestPeekPushFilter(Arg!TestPeekPushFilter())
		.pipe!TestPushPullFilter(Arg!TestPushPullFilter())
		.pipe!TestPullSink(Arg!TestPullSink());

	pipe!TestAllocSource(Arg!TestAllocSource())
		.pipe!TestAllocFilter(Arg!TestAllocFilter())
		.pipe!TestAllocFilter(Arg!TestAllocFilter())
		.pipe!TestAllocPeekFilter(Arg!TestAllocPeekFilter())
		.pipe!TestPeekPushFilter(Arg!TestPeekPushFilter())
		.pipe!TestPushPullFilter(Arg!TestPushPullFilter())
		.pipe!TestPullPushFilter(Arg!TestPullPushFilter())
		.pipe!TestPushFilter(Arg!TestPushFilter())
		.pipe!TestPushAllocFilter(Arg!TestPushAllocFilter())
		.pipe!TestAllocFilter(Arg!TestAllocFilter())
		.pipe!TestAllocPushFilter(Arg!TestAllocPushFilter())
		.pipe!TestPushPeekFilter(Arg!TestPushPeekFilter())
		.pipe!TestPeekAllocFilter(Arg!TestPeekAllocFilter())
		.pipe!TestAllocPullFilter(Arg!TestAllocPullFilter())
		.pipe!TestPullSink(Arg!TestPullSink());
}

unittest {
	// implicit adapters, pull->push
	pipe!TestPullSource(Arg!TestPullSource())
		.pipe!TestPushSink(Arg!TestPushSink());

	pipe!TestPullSource(Arg!TestPullSource())
		.pipe!TestPushFilter(Arg!TestPushFilter())
		.pipe!TestPushSink(Arg!TestPushSink());

	pipe!TestPullSource(Arg!TestPullSource())
		.pipe!TestPushAllocFilter(Arg!TestPushAllocFilter())
		.pipe!TestAllocSink(Arg!TestAllocSink());

	pipe!TestPullSource(Arg!TestPullSource())
		.pipe!TestPushPeekFilter(Arg!TestPushPeekFilter())
		.pipe!TestPeekSink(Arg!TestPeekSink());

	pipe!TestPullSource(Arg!TestPullSource())
		.pipe!TestPushPullFilter(Arg!TestPushPullFilter())
		.pipe!TestPullSink(Arg!TestPullSink());
}

unittest {
	// implicit adapters, pull->peek
	pipe!TestPullSource(Arg!TestPullSource())
		.pipe!TestPeekSink(Arg!TestPeekSink());

	pipe!TestPullSource(Arg!TestPullSource())
		.pipe!TestPeekFilter(Arg!TestPeekFilter())
		.pipe!TestPeekSink(Arg!TestPeekSink());

	pipe!TestPullSource(Arg!TestPullSource())
		.pipe!TestPeekAllocFilter(Arg!TestPeekAllocFilter())
		.pipe!TestAllocSink(Arg!TestAllocSink());

	pipe!TestPullSource(Arg!TestPullSource())
		.pipe!TestPeekPushFilter(Arg!TestPeekPushFilter())
		.pipe!TestPushSink(Arg!TestPushSink());

	pipe!TestPullSource(Arg!TestPullSource())
		.pipe!TestPeekPullFilter(Arg!TestPeekPullFilter())
		.pipe!TestPullSink(Arg!TestPullSink());
}

unittest {
	// implicit adapters, push->pull
	pipe!TestPushSource(Arg!TestPushSource())
		.pipe!TestPullSink(Arg!TestPullSink());

	pipe!TestPushSource(Arg!TestPushSource())
		.pipe!TestPullFilter(Arg!TestPullFilter())
		.pipe!TestPullSink(Arg!TestPullSink());

	pipe!TestPushSource(Arg!TestPushSource())
		.pipe!TestPullAllocFilter(Arg!TestPullAllocFilter())
		.pipe!TestAllocSink(Arg!TestAllocSink());

	pipe!TestPushSource(Arg!TestPushSource())
		.pipe!TestPullPeekFilter(Arg!TestPullPeekFilter())
		.pipe!TestPeekSink(Arg!TestPeekSink());

	pipe!TestPushSource(Arg!TestPushSource())
		.pipe!TestPullPushFilter(Arg!TestPullPushFilter())
		.pipe!TestPushSink(Arg!TestPushSink());
}

unittest {
	// implicit adapters, peek->pull
	pipe!TestPeekSource(Arg!TestPeekSource())
		.pipe!TestPullSink(Arg!TestPullSink());

	pipe!TestPeekSource(Arg!TestPeekSource())
		.pipe!TestPullFilter(Arg!TestPullFilter())
		.pipe!TestPullSink(Arg!TestPullSink());

	pipe!TestPeekSource(Arg!TestPeekSource())
		.pipe!TestPullAllocFilter(Arg!TestPullAllocFilter())
		.pipe!TestAllocSink(Arg!TestAllocSink());

	pipe!TestPeekSource(Arg!TestPeekSource())
		.pipe!TestPullPeekFilter(Arg!TestPullPeekFilter())
		.pipe!TestPeekSink(Arg!TestPeekSink());

	pipe!TestPeekSource(Arg!TestPeekSource())
		.pipe!TestPullPushFilter(Arg!TestPullPushFilter())
		.pipe!TestPushSink(Arg!TestPushSink());
}

unittest {
	// implicit adapters, peek->push
	pipe!TestPeekSource(Arg!TestPeekSource())
		.pipe!TestPushSink(Arg!TestPushSink());

	pipe!TestPeekSource(Arg!TestPeekSource())
		.pipe!TestPushFilter(Arg!TestPushFilter())
		.pipe!TestPushSink(Arg!TestPushSink());

	pipe!TestPeekSource(Arg!TestPeekSource())
		.pipe!TestPushAllocFilter(Arg!TestPushAllocFilter())
		.pipe!TestAllocSink(Arg!TestAllocSink());

	pipe!TestPeekSource(Arg!TestPeekSource())
		.pipe!TestPushPeekFilter(Arg!TestPushPeekFilter())
		.pipe!TestPeekSink(Arg!TestPeekSink());

	pipe!TestPeekSource(Arg!TestPeekSource())
		.pipe!TestPushPullFilter(Arg!TestPushPullFilter())
		.pipe!TestPullSink(Arg!TestPullSink());
}
