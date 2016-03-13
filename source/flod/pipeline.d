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
	import std.algorithm : min, max, map, copy;
	import std.conv : to;
	import std.experimental.logger : logf, errorf;
	import std.range : isInputRange, ElementType, array, take;
	import std.string : split, toLower, startsWith, endsWith;

	ulong[] inputArray;
	ulong[] outputArray;
	size_t outputIndex;

	uint filterMark(string f) {
		f = f.toLower;
		uint fm;
		if (f.startsWith("pull"))
			fm = 1;
		else if (f.startsWith("push"))
			fm = 2;
		else if (f.startsWith("alloc"))
			fm = 3;
		if (f.endsWith("pull"))
			fm |= 1 << 2;
		else if (f.endsWith("push"))
			fm |= 2 << 2;
		else if (f.endsWith("alloc"))
			fm |= 3 << 2;
		return fm;
	}

	ulong filter(string f)(ulong a) {
		enum fm = filterMark(f);
		return (a << 4) | fm;
	}

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

	@pullSource!ulong
	struct TestPullSource {
		mixin TestStage;

		size_t pull(ulong[] buf)
		{
			auto len = min(buf.length, inputArray.length);
			buf[0 .. len] = inputArray[0 .. len];
			inputArray = inputArray[len .. $];
			return len;
		}
	}

	@peekSource!ulong
	struct TestPeekSource {
		mixin TestStage;

		const(ulong)[] peek(size_t n)
		{
			auto len = min(max(n, 2909), inputArray.length);
			return inputArray[0 .. len];
		}

		void consume(size_t n) { inputArray = inputArray[n .. $]; }
	}

	@pushSource!ulong
	struct TestPushSource(Sink) {
		mixin TestStage;
		Sink sink;

		void run()()
		{
			while (inputArray.length) {
				auto len = min(1337, inputArray.length);
				if (sink.push(inputArray[0 .. len]) != len)
					break;
				inputArray = inputArray[len .. $];
			}
		}
	}

	@allocSource!ulong
	struct TestAllocSource(Sink) {
		mixin TestStage;
		Sink sink;

		void run()()
		{
			ulong[] buf;
			while (inputArray.length) {
				auto len = min(1337, inputArray.length);
				if (!sink.alloc(buf, len))
					assert(0);
				buf[] = inputArray[0 .. len];
				if (sink.commit(len) != len)
					break;
				inputArray = inputArray[len .. $];
			}
		}
	}

	// sinks:

	@pullSink!ulong
	struct TestPullSink(Source) {
		mixin TestStage;
		Source source;

		void run()
		{
			while (outputIndex < outputArray.length) {
				auto len = min(4157, outputArray.length - outputIndex);
				auto pd = source.pull(outputArray[outputIndex .. outputIndex + len]);
				outputIndex += pd;
				if (pd < len)
					break;
			}
		}
	}

	@peekSink!ulong
	struct TestPeekSink(Source) {
		mixin TestStage;
		Source source;

		void run()
		{
			while (outputIndex < outputArray.length) {
				auto len = min(4157, outputArray.length - outputIndex);
				auto ib = source.peek(len);
				auto olen = min(len, ib.length, 6379);
				outputArray[outputIndex .. outputIndex + olen] = ib[0 .. olen];
				outputIndex += olen;
				source.consume(olen);
				if (olen < len)
					break;
			}
		}
	}

	@pushSink!ulong
	struct TestPushSink {
		mixin TestStage;

		size_t push(const(ulong)[] buf)
		{
			auto len = min(buf.length, outputArray.length - outputIndex);
			if (len) {
				outputArray[outputIndex .. outputIndex + len] = buf[0 .. len];
				outputIndex += len;
			}
			return len;
		}
	}

	@allocSink!ulong
	struct TestAllocSink {
		mixin TestStage;
		ulong[] last;

		bool alloc(ref ulong[] buf, size_t n)
		{
			if (n < outputArray.length - outputIndex)
				buf = outputArray[outputIndex .. outputIndex + n];
			else
				buf = last = new ulong[n];
			return true;
		}

		size_t commit(size_t n)
		out(result) { assert(result <= n); }
		body
		{
			if (!last) {
				outputIndex += n;
				return n;
			} else {
				auto len = min(n, outputArray.length - outputIndex);
				outputArray[outputIndex .. outputIndex + len] = last[0 .. len];
				outputIndex += len;
				return len;
			}
		}
	}

	// filter

	@peekSink!ulong @peekSource!ulong
	struct TestPeekFilter(Source) {
		mixin TestStage;
		Source source;
		const(ulong)[] peek(size_t n)
		{
			return source.peek(n).map!(filter!"peek").array();
		}
		void consume(size_t n) { source.consume(n); }
	}

	@peekSink!ulong @pullSource!ulong
	struct TestPeekPullFilter(Source) {
		mixin TestStage;
		Source source;
		size_t pull(ulong[] buf)
		{
			auto ib = source.peek(buf.length);
			auto len = min(ib.length, buf.length);
			ib.take(len).map!(filter!"peekPull").copy(buf);
			source.consume(len);
			return len;
		}
	}

	@peekSink!ulong @pushSource!ulong
	struct TestPeekPushFilter(Source, Sink) {
		mixin TestStage;
		Source source;
		Sink sink;
		void run()()
		{
			for (;;) {
				auto ib = source.peek(4096);
				auto ob = ib.map!(filter!"peekPush").array();
				source.consume(ib.length);
				if (sink.push(ob) < 4096)
					break;
			}
		}
	}

	@peekSink!ulong @allocSource!ulong
	struct TestPeekAllocFilter(Source, Sink) {
		mixin TestStage;
		Source source;
		Sink sink;
		void run()()
		{
			ulong[] buf;
			for (;;) {
				auto ib = source.peek(4096);
				if (!sink.alloc(buf, ib.length))
					assert(0);
				auto len = min(ib.length, buf.length);
				ib.take(len).map!(filter!"peekAlloc").copy(buf);
				source.consume(len);
				if (sink.commit(len) < 4096)
					break;
			}
		}
	}

	@pullSink!ulong @pullSource!ulong
	struct TestPullFilter(Source) {
		mixin TestStage;
		Source source;
		size_t pull(ulong[] buf)
		{
			size_t n = source.pull(buf);
			foreach (ref b; buf[0 .. n])
				b = b.filter!"pull";
			return n;
		}
	}

	@pullSink!ulong @peekSource!ulong
	struct TestPullPeekFilter(Source) {
		mixin TestStage;
		Source source;
		const(ulong)[] peek(size_t n)
		{
			auto buf = new ulong[n];
			size_t m = source.pull(buf[]);
			foreach (ref b; buf[0 .. m])
				b = b.filter!"pullPeek";
			return buf[0 .. m];
		}
		void consume(size_t n) {}
	}

	@pullSink!ulong @pushSource!ulong
	struct TestPullPushFilter(Source, Sink) {
		mixin TestStage;
		Source source;
		Sink sink;
		void run()()
		{
			for (;;) {
				ulong[4096] buf;
				auto n = source.pull(buf[]);
				foreach (ref b; buf[0 .. n])
					b = b.filter!"pullPush";
				if (sink.push(buf[0 .. n]) < 4096)
					break;
			}
		}
	}

	@pullSink!ulong @allocSource!ulong
	struct TestPullAllocFilter(Source, Sink) {
		mixin TestStage;
		Source source;
		Sink sink;
		void run()()
		{
			for (;;) {
				ulong[] buf;
				if (!sink.alloc(buf, 4096))
					assert(0);
				auto n = source.pull(buf[]);
				foreach (ref b; buf[0 .. n])
					b = b.filter!"pullAlloc";
				if (sink.commit(n) < 4096)
					break;
			}
		}
	}

	@pushSink!ulong @pushSource!ulong
	struct TestPushFilter(Sink) {
		mixin TestStage;
		Sink sink;
		size_t push(const(ulong)[] buf)
		{
			return sink.push(buf.map!(filter!"push").array());
		}
	}

	@pushSink!ulong @allocSource!ulong
	struct TestPushAllocFilter(Sink) {
		mixin TestStage;
		Sink sink;
		size_t push(const(ulong)[] buf)
		out(result) { assert(result <= buf.length); }
		body
		{
			ulong[] ob;
			if (!sink.alloc(ob, buf.length))
				assert(0);
			auto len = min(buf.length, ob.length);
			buf.take(len).map!(filter!"pushAlloc").copy(ob);
			return sink.commit(len);
		}
	}

	@pushSink!ulong @pullSource!ulong
	struct TestPushPullFilter(alias Scheduler) {
		mixin Scheduler;
		mixin TestStage;
		ulong[] buffer;

		size_t push(const(ulong)[] buf)
		{
			buffer ~= buf.map!(filter!"pushPull").array();
			if (yield())
				return 0;
			return buf.length;
		}

		size_t pull(ulong[] buf)
		{
			size_t n = buf.length;
			while (buffer.length < n) {
				if (yield())
					break;
			}
			size_t len = min(n, buffer.length);
			buf[0 .. len] = buffer[0 .. len];
			buffer = buffer[len .. $];
			return len;
		}
	}

	@pushSink!ulong @peekSource!ulong
	struct TestPushPeekFilter(alias Scheduler) {
		mixin Scheduler;
		mixin TestStage;
		ulong[] buffer;

		size_t push(const(ulong)[] buf)
		{
			buffer ~= buf.map!(filter!"pushPeek").array();
			if (yield())
				return 0;
			return buf.length;
		}

		const(ulong)[] peek(size_t n)
		{
			while (buffer.length < n) {
				if (yield())
					break;
			}
			return buffer;
		}

		void consume(size_t n)
		{
			buffer = buffer[n .. $];
		}
	}

	@allocSink!ulong @allocSource!ulong
	struct TestAllocFilter(Sink) {
		mixin TestStage;
		Sink sink;
		ulong[] buf;

		bool alloc(ref ulong[] buf, size_t n)
		{
			auto r = sink.alloc(buf, n);
			this.buf = buf;
			return r;
		}

		size_t commit(size_t n)
		{
			foreach (ref b; buf[0 .. n])
				b = b.filter!"alloc";
			return sink.commit(n);
		}
	}

	@allocSink!ulong @pushSource!ulong
	struct TestAllocPushFilter(Sink) {
		mixin TestStage;
		Sink sink;
		ulong[] buffer;

		bool alloc(ref ulong[] buf, size_t n)
		{
			buffer = buf = new ulong[n];
			return true;
		}

		size_t commit(size_t n)
		{
			size_t m = sink.push(buffer[0 .. n].map!(filter!"allocPush").array());
			buffer = buffer[m .. $];
			return m;
		}
	}

	@allocSink!ulong @pullSource!ulong
	struct TestAllocPullFilter(alias Scheduler) {
		mixin Scheduler;
		mixin TestStage;
		ulong[] buffer;
		size_t readOffset;
		size_t writeOffset;

		bool alloc(ref ulong[] buf, size_t n)
		{
			buffer.length = writeOffset + n;
			buf = buffer[writeOffset .. $];
			return true;
		}

		size_t commit(size_t n)
		{
			foreach (ref b; buffer[writeOffset .. writeOffset + n])
				b = b.filter!"allocPull";
			writeOffset += n;
			if (yield())
				return 0;
			return n;
		}

		size_t pull(ulong[] buf)
		{
			size_t n = buf.length;
			while (writeOffset - readOffset < n) {
				if (yield())
					break;
			}
			size_t len = min(n, writeOffset - readOffset);
			buf[0 .. len] = buffer[readOffset .. readOffset + len];
			readOffset += len;
			return len;
		}
	}

	@allocSink!ulong @peekSource!ulong
	struct TestAllocPeekFilter(alias Scheduler) {
		mixin Scheduler;
		mixin TestStage;
		ulong[] buffer;
		size_t readOffset;
		size_t writeOffset;

		bool alloc(ref ulong[] buf, size_t n)
		{
			buffer.length = writeOffset + n;
			buf = buffer[writeOffset .. $];
			return true;
		}

		size_t commit(size_t n)
		{
			foreach (ref b; buffer[writeOffset .. writeOffset + n])
				b = b.filter!"allocPeek";
			writeOffset += n;
			if (yield())
				return 0;
			return n;
		}

		const(ulong)[] peek(size_t n)
		{
			while (writeOffset - readOffset < n) {
				if (yield())
					break;
			}
			return buffer[readOffset .. writeOffset];
		}

		void consume(size_t n)
		{
			readOffset += n;
		}
	}

	string genStage(string filter, string suf)
	{
		import std.ascii : toUpper;
		auto cf = filter[0].toUpper ~ filter[1 .. $];
		return "pipe!Test" ~ cf ~ suf ~ "(Arg!Test" ~ cf ~ suf ~ "())";
	}

	string genChain(string filterList)
	{
		import std.algorithm : map;
		import std.array : join, split;
		auto filters = filterList.split(",");
		string midstr;
		if (filters.length > 2)
			midstr = filters[1 .. $ - 1].map!(f => "." ~ genStage(f, "Filter")).join;
		return genStage(filters[0], "Source")
			~ midstr
			~ "." ~ genStage(filters[$ - 1], "Sink") ~ ";";
	}

	void testChain(string filterlist, R)(R r)
		if (isInputRange!R && is(ElementType!R : ulong))
	{
		auto input = r.map!(a => ulong(a)).array();
		logf("Testing %s with %d elements", filterlist, input.length);
		auto expectedOutput = input.dup;
		auto filters = filterlist.split(",");
		if (filters.length > 2) {
			foreach (filter; filters[1 .. $ - 1]) {
				auto fm = filterMark(filter);
				foreach (ref eo; expectedOutput)
					eo = (eo << 4) | fm;
			}
		}
		foreach(expectedLength; [ size_t(0), input.length / 3, input.length - 1, input.length,
			input.length + 1, input.length * 5 ]) {
			outputArray.length = expectedLength;
			outputArray[] = 0xbadc0ffee0ddf00d;
			inputArray = input;
			outputIndex = 0;
			mixin(genChain(filterlist));
			auto len = min(outputIndex, expectedLength, input.length);
			uint left = 8;
			size_t all = 0;
			if (outputIndex != min(expectedLength, input.length)) {
				errorf("Output length is %d, expected %d", outputIndex, min(expectedLength, input.length));
				assert(0);
			}
			for (size_t i = 0; i < len; i++) {
				if (expectedOutput[i] != outputArray[i]) {
					if (left > 0) {
						logf("expected[%d] != output[%d]: %x vs. %x", i, i, expectedOutput[i], outputArray[i]);
						--left;
					}
					all++;
				}
			}
			if (all > 0) {
				logf("%s", genChain(filterlist));
				logf("total: %d differences", all);
			}
			assert(all == 0);
		}
	}

	void testChain(string filterlist)()
	{
		import std.range : iota;
		testChain!filterlist(iota(0, 173447));
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
	static assert(is(p1.ElementType == ulong));
	auto p2 = pipe!TestPullSource(Arg!TestPullSource());
	static assert(isPullPipeline!(typeof(p2)));
	static assert(is(p2.ElementType == ulong));
}

unittest {
	auto p1 = pipe!TestPushSource(Arg!TestPushSource());
	static assert(isPushPipeline!(typeof(p1)));
	auto p2 = pipe!TestAllocSource(Arg!TestAllocSource());
	static assert(isAllocPipeline!(typeof(p2)));
}

unittest {
	// compatible source-sink pairs
	testChain!`peek,peek`;
	testChain!`pull,pull`;
	testChain!`push,push`;
	testChain!`alloc,alloc`;
}

unittest {
	// compatible, with 1 filter
	testChain!`peek,peek,peek`;
	testChain!`peek,peekPull,pull`;
	testChain!`peek,peekPush,push`;
	testChain!`peek,peekAlloc,alloc`;
	testChain!`pull,pullPeek,peek`;
	testChain!`pull,pull,pull`;
	testChain!`pull,pullPush,push`;
	testChain!`pull,pullAlloc,alloc`;
	testChain!`push,pushPeek,peek`;
	testChain!`push,pushPull,pull`;
	testChain!`push,push,push`;
	testChain!`push,pushAlloc,alloc`;
	testChain!`alloc,allocPeek,peek`;
	testChain!`alloc,allocPull,pull`;
	testChain!`alloc,allocPush,push`;
	testChain!`alloc,alloc,alloc`;
}

unittest {
	// just one active sink at the end
	testChain!`peek,peek,peek,peek,peek`;
	testChain!`peek,peek,peekPull,pull,pull`;
	testChain!`pull,pull,pull,pull,pull`;
	testChain!`pull,pull,pullPeek,peek,peek`;
}

unittest {
	// just one active source at the beginning
	testChain!`push,push,push,push,push`;
	testChain!`push,push,pushAlloc,alloc,alloc`;
	testChain!`alloc,alloc,alloc,alloc,alloc`;
	testChain!`alloc,alloc,allocPush,push,push`;
}

unittest {
	// convert passive source to active source, longer chains
	testChain!`pull,pullPeek,peekAlloc,allocPush,push`;
	testChain!`pull,pullPeek,peekPush,pushAlloc,alloc`;
	testChain!`peek,peekPull,pullPush,pushAlloc,alloc`;
	testChain!`peek,peekPull,pullAlloc,allocPush,push`;
}

unittest {
	// convert active source to passive source at stage 2, longer passive source chain
	testChain!`push,pushPull,pull,pull,pullPeek,peek,peekPush,push,push`;
}

unittest {
	// convert active source to passive source at stage >2 (longer active source chain)
	testChain!`push,push,pushPull,pull`;
	testChain!`push,push,push,push,push,pushPull,pull`;
	testChain!`push,push,pushAlloc,alloc,alloc,allocPeek,peek`;
}

unittest {
	// multiple inverters
	testChain!`alloc,allocPeek,peekPush,pushPull,pull`;
	testChain!`alloc,alloc,alloc,allocPeek,peek,peekPush,push,pushPull,pull`;
	testChain!`alloc,alloc,allocPeek,peekPush,pushPull,pull`;
	testChain!`alloc,alloc,alloc,allocPeek,peekPush,pushPull,pullPush,push,pushAlloc,alloc,allocPush,pushPeek,peekAlloc,allocPull,pull`;
}

unittest {
	// implicit adapters, pull->push
	testChain!`pull,push`;
	testChain!`pull,push,push`;
	testChain!`pull,pushPeek,peek`;
	testChain!`pull,pushPull,pull`;
	testChain!`pull,pushAlloc,alloc`;
}

unittest {
	// implicit adapters, pull->peek
	testChain!`pull,peek`;
	testChain!`pull,peekPush,push`;
	testChain!`pull,peek,peek`;
	testChain!`pull,peekPull,pull`;
	testChain!`pull,peekAlloc,alloc`;
}

unittest {
	// implicit adapters, push->pull
	testChain!`push,pull`;
	testChain!`push,pullPush,push`;
	testChain!`push,pullAlloc,alloc`;
	testChain!`push,pullPeek,peek`;
	testChain!`push,pull,pull`;
}

unittest {
	// implicit adapters, peek->pull
	testChain!`peek,pull`;
	testChain!`peek,pullPush,push`;
	testChain!`peek,pullAlloc,alloc`;
	testChain!`peek,pullPeek,peek`;
	testChain!`peek,pull,pull`;
}

unittest {
	// implicit adapters, peek->push
	testChain!`peek,push`;
	testChain!`peek,push,push`;
	testChain!`peek,pushAlloc,alloc`;
	testChain!`peek,pushPeek,peek`;
	testChain!`peek,pushPull,pull`;
}
