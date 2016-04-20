/** Pipeline composition.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.pipeline;

import std.meta : AliasSeq, staticMap;
import std.range : isDynamicArray, isInputRange;
import std.typecons : Flag, Yes, No;

import flod.meta : NonCopyable, str;
import flod.metadata;
import flod.range;
import flod.traits;

/**
Find optimal choice of methods for all stages.

Each stage may implement more than one method or method pair. This function is used in compile time
to find a set of methods that will result in the smallest overhead, according to predefined
static costs of connecting sinks to sources implementing different methods.
Compatible methods always have a cost of 0. Implicit adapters are given positive costs depending
on whether they involve e.g. data copying or context switching.
*/
private MethodAttribute[] chooseOptimalMethods(Flag!`bruteForce` brute_force = No.bruteForce)(MethodAttribute[][] stages)
{
	if (stages.length == 1)
		return [ stages[0][0] ];

	assert(stages.length >= 2);

	// FIXME: these numbers are made up out of thin air, update them based on some benchmarks.
	enum pairwiseCost = [
		[ 0, 5, 15, 10 ],
		[ 50, 0, 10, 60 ],
		[ 145, 95, 0, 50 ],
		[ 95, 45, 5, 0 ]
	];

	static if (brute_force) {
		// just test all possible permutations
		auto result = new size_t[stages.length];
		auto minCost = uint.max;
		auto ind = new size_t[stages.length];
		for (;;) {
			uint cost;
			foreach (i; 1 .. stages.length)
				cost += pairwiseCost[stages[i - 1][ind[i - 1]].sourceMethod][stages[i][ind[i]].sinkMethod];
			if (cost < minCost) {
				minCost = cost;
				result[] = ind[];
			}
			foreach (i; 0 .. stages.length) {
				if (++ind[i] < stages[i].length)
					break;
				ind[i] = 0;
				if (i == stages.length - 1) {
					import std.range : enumerate, array;
					return result.enumerate.map!(t => stages[t.index][t.value]).array();
				}
			}
		}
	} else {
		// solve using dynamic programming
		static struct Choice {
			size_t previous = 0;
			uint totalCost = 0;
		}
		Choice[][] choices;
		choices.length = stages.length;
		choices[0].length = stages[0].length;
		foreach (stage; 1 .. stages.length) {
			choices[stage].length = stages[stage].length;
			foreach (sinkindex, sink; stages[stage]) {
				size_t ch;
				uint mincost = uint.max;
				foreach (sourceindex, source; stages[stage - 1]) {
					auto cost = choices[stage - 1][sourceindex].totalCost
						+ pairwiseCost[source.sourceMethod][sink.sinkMethod];
					if (cost < mincost) {
						ch = sourceindex;
						mincost = cost;
					}
				}
				choices[stage][sinkindex] = Choice(ch, mincost);
			}
		}
		auto result = new MethodAttribute[](stages.length);
		// topNIndex doesn't work in CT...
		import std.algorithm : minPos;
		auto lastindex = choices[$ - 1].length - minPos!((a, b) => a.totalCost < b.totalCost)(choices[$ - 1]).length;
		foreach_reverse (stage, ref ma; result) {
			ma = stages[stage][lastindex];
			lastindex = choices[stage][lastindex].previous;
		}
		return result;
	}
}

version(unittest) {
	MethodAttribute[][] methodAttributesFromString(string str)
	{
		enum tr = [
			"pull" : Method.pull,
			"peek" : Method.peek,
			"push" : Method.push,
			"alloc" : Method.alloc ];
		auto stages = str.split(",");
		auto result = [ stages[0].split("/").map!(a => source(tr[a]).methods).array ];
		if (stages.length > 1) {
			result ~= stages[1 .. $ - 1].map!(st =>
					st.split("/")
						.map!(m => m.split("-"))
						.map!(m => filter(tr[m[0]], tr[m[1]]).methods)
						.array
					).array
				~ stages[$ - 1].split("/").map!(a => sink(tr[a]).methods).array;
		}
		return result;
	}

	unittest {
		assert(methodAttributesFromString(
			"pull/peek,push") == [
				[ source(Method.pull), source(Method.peek) ],
				[ sink(Method.push) ]
			]);
		assert(methodAttributesFromString(
			"pull/peek/alloc,push-alloc/alloc-pull,peek-push,peek-pull/push-peek/push-alloc,push/peek") == [
				[ source(Method.pull), source(Method.peek), source(Method.alloc) ],
				[ filter(Method.push, Method.alloc), filter(Method.alloc, Method.pull) ],
				[ filter(Method.peek, Method.push) ],
				[ filter(Method.peek, Method.pull), filter(Method.push, Method.peek), filter(Method.push, Method.alloc) ],
				[ sink(Method.push), sink(Method.peek) ]
			]);
	}

	bool testOptimizeChain(string str)
	{
		auto inp = methodAttributesFromString(str);
		auto outp = chooseOptimalMethods(inp);
		auto outpb = chooseOptimalMethods!(Yes.bruteForce)(inp);
		assert(outp == outpb, {
				import std.experimental.logger : logf;
				logf("bf:  %s", outpb);
				logf("dyn: %s", outp);
				return str;
			}());
		return true;
	}
}

unittest {
	testOptimizeChain("pull");
	testOptimizeChain("pull,pull");
	testOptimizeChain("pull/peek/alloc,push-alloc/alloc-peek,peek-alloc/alloc-peek,push-alloc/alloc-peek/peek-push,push");
	enum a = testOptimizeChain("pull,peek-pull/alloc-pull,peek");
}

struct FiberSwitch {
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

	/**
	Switch context.

	Executes the other context up to the point where it also calls yield()
	on the same FiberSwitch, or returns.

	Returns: non-zero if the other context finished execution.
	*/
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

private mixin template Context(PL, InputE, OutputE,
	Flag!`passiveFilter` passiveFilter = Yes.passiveFilter,
	size_t index, size_t driverIndex)
{
	@property ref PL outer()() { return PL.outer!index(this); }
	@property ref auto source()() { return outer.tup[index - 1]; }
	@property ref auto sink()() { return outer.tup[index + 1]; }

	alias InputElementType = InputE;
	alias OutputElementType = OutputE;

	static if (passiveFilter) {
		FiberSwitch _flod_switch;

		int yield()() { return _flod_switch.yield(); }
		void spawn()()
		{
			import core.thread : Fiber;
			auto next_driver = &outer.tup[driverIndex];
			if (!_flod_switch.fiber) {
				static if (__traits(compiles, &next_driver.run!()))
					auto runf = &next_driver.run!();
				else
					auto runf = &next_driver.run;
				_flod_switch.fiber = new Fiber(runf, 65536);
			}
		}
		void stop()() { _flod_switch.stop(); }
	}

	@property void tag(string key)(PL.Metadata.ValueType!key value)
	{
		import flod.meta : tupleFromArray;
		outer.metadata.set!(key, index)(value);

		foreach (i; tupleFromArray!(size_t, PL.Metadata.getters!(key, index))) {
			static if (__traits(hasMember, typeof(outer.tup[i]), "onChange"))
				outer.tup[i].onChange!key();
			else
				pragma(msg, "Warning: no property `onChange` for stage " ~ .str!(typeof(outer.tup[i])));
		}
	}

	@property PL.Metadata.ValueType!key tag(string key)()
	{
		return outer.metadata.get!(key, index);
	}
}

private void constructInPlace(T, Args...)(ref T t, auto ref Args args)
{
	static if (__traits(hasMember, t, "__ctor")) {
		t.__ctor(args);
	} else static if (Args.length > 0) {
		static assert(0, "Stage " ~ str!T ~ " does not have a non-trivial constructor" ~
			" but construction was requested with arguments " ~ Args.stringof);
	}
}

private struct StageSpec(alias S, A...) {
	alias Stage = S;
	alias Args = A;
	Args args;
}

// Used to catch StageSpec from a stage factory function.
private static struct FakeSchema {
	auto pipe(alias NextStage, NextArgs...)(auto ref NextArgs args)
	{
		return StageSpec!(NextStage, NextArgs)(args);
	}
}

/**
Determines which driver drives the entire pipeline in pipelines with multiple drivers.
*/
enum DriveMode {
	source, /// The leftmost driver drives the entire pipeline.
	sink    /// The rightmost driver drives the entire pipeline.
}

private enum bool isDriver(alias Stage) =
	   (isActiveSource!Stage && !isPassiveSink!Stage)
	|| (isActiveSink!Stage && !isPassiveSource!Stage);

private template getNextDriver(DriveMode mode, size_t i, StageSeq...) {
	static if (i >= StageSeq.length)
		enum getNextDriver = -1;
	else static if (isDriver!(StageSeq[i]))
		enum getNextDriver = i;
	else
		enum getNextDriver = getNextDriver!(mode, i + (mode == DriveMode.source ? 1 : -1), StageSeq);
}

private enum getFirstDriver(DriveMode mode, StageSeq...) =
	getNextDriver!(mode, (mode == DriveMode.source ? 0 : StageSeq.length - 1), StageSeq);

private static struct AdapterInsertionInfo {
	size_t index;
	string name;
}

// Used in CT to build list of adapters that need to be inserted according to the specified methods.
private static AdapterInsertionInfo[] buildListOfAdapters(const(MethodAttribute)[] methods)
{
	import std.string : capitalize;
	string[Method.max + 1] methodNames = [
		Method.pull : "pull", Method.peek : "peek",
		Method.push : "push", Method.alloc : "alloc" ];
	AdapterInsertionInfo[] result;
	foreach (i; 0 .. methods.length - 1) {
		if (methods[i].sourceMethod != methods[i + 1].sinkMethod)
			result ~= AdapterInsertionInfo(i + 1,
				methodNames[methods[i].sourceMethod] ~ methodNames[methods[i + 1].sinkMethod].capitalize);
	}
	return result;
}

/**
Holds the information (both static and dynamic) needed to create a pipeline instance.

The static information includes the template aliases of all stages in the pipeline, as well as
types of their constructor arguments.
The dynamic information is the constructor arguments themselves.

Params:
	S   = The stage added as the last one to the schema.
    Src = Type of schema object describing the previous stages.
    A   = Types of arguments passed to S's instance ctor.
*/
private struct Schema(DriveMode mode, S...) {
	import std.conv : to;
	alias StageSpecSeq = S;
	alias getStage(Z) = Z.Stage;
	alias StageSeq = staticMap!(getStage, S, StageSpec!StageSpec)[0 .. $ - 1];
	alias FirstStage = StageSeq[0];
	alias LastStage = StageSeq[$ - 1];

	enum size_t length = S.length;
	enum driveMode = mode;

	alias ElementType = SourceElementType!(length - 1, StageSeq);

	StageSpecSeq stages;

	version(unittest) {
		auto drive(DriveMode mode)() {
			return Schema!(mode, StageSpecSeq)(stages);
		}
	}

	/// Appends NextStage to this schema to be executed in the same thread as LastStage.
	auto pipe(alias NextStage, NextArgs...)(auto ref NextArgs nextArgs)
	{
		alias T = StageSpec!(NextStage, NextArgs);
		auto result = Schema!(driveMode, StageSpecSeq, T)(stages, T(nextArgs));
		static if (isSource!NextStage || isSink!FirstStage)
			return result;
		else
			result.run();
	}

	void run()()
	{
		create().run();
	}

	auto construct(size_t i = 0)(ref Pipeline!(driveMode, StageSeq) pipeline)
	{
		static if (i < StageSeq.length) {
			constructInPlace(pipeline.tup[i], stages[i].args);
			construct!(i + 1)(pipeline);
		}
	}

	private auto insertAdapters(const(AdapterInsertionInfo)[] info)()
	{
		static if (info.length == 0) {
			return this;
		} else {
			import flod.adapter;
			enum lastAdapter = info[$ - 1];
			mixin(`auto spec = FakeSchema().` ~ lastAdapter.name ~ `;`);
			alias T = Schema!(driveMode, S[0 .. lastAdapter.index], typeof(spec), S[lastAdapter.index .. $]);
			auto schema = T(stages[0 .. lastAdapter.index], spec, stages[lastAdapter.index .. $]);
			static if (info.length == 1)
				return schema;
			else
				return schema.insertAdapters!(info[0 .. $ - 1]);
		}
	}

	private auto doCreate()()
	{
		Pipeline!(driveMode, StageSeq) p;
		construct(p);
		return p;
	}

	auto create()()
	{
		enum adapters = [ staticMap!(getMethods, StageSeq) ].chooseOptimalMethods.buildListOfAdapters;
		static if (adapters.length == 0)
			return doCreate();
		else
			return insertAdapters!adapters().doCreate();
	}
}

private template testSchema(Sch, alias test) {
	static if (is(Sch == Schema!A, A...))
		enum testSchema = test!(Sch.LastStage);
	else
		enum testSchema = false;
}

enum isSchema(P) =
	   isDynamicArray!P || testSchema!(P, isPeekSource)
	|| isInputRange!P || testSchema!(P, isPullSource)
	|| testSchema!(P, isPushSource)
	|| testSchema!(P, isAllocSource)
	|| is(P == FakeSchema);

///
auto pipe(alias Stage, Args...)(auto ref Args args)
	if (isSink!Stage || isSource!Stage)
{
	static if (isSink!Stage && Args.length > 0 && isDynamicArray!(Args[0]))
		return pipeFromArray(args[0]).pipe!Stage(args[1 .. $]);
	else static if (isSink!Stage && Args.length > 0 && isInputRange!(Args[0]))
		return pipeFromInputRange(args[0]).pipe!Stage(args[1 .. $]);
	else {
		alias T = StageSpec!(Stage, Args);
		return Schema!(DriveMode.sink, T)(T(args));
	}
}

///
auto pipe(E, alias Dg)()
{
	return pipeFromDelegate!(E, Dg);
}

/// A pipeline built based on schema S.
struct Pipeline(DriveMode mode, S...) {
	enum driveMode = mode;
	alias StageSeq = S;
	alias LastStage = StageSeq[$ - 1];

	template StageType(size_t i) {
		alias Stage = StageSeq[i];
		alias StageType = Stage!(Context, Pipeline,
			SinkElementType!(i, StageSeq), SourceElementType!(i, StageSeq),
			(isPassiveSink!Stage && isPassiveSource!Stage) ? Yes.passiveFilter : No.passiveFilter,
			i, getNextDriver!(driveMode, i, StageSeq));
	}

	template StageTypeTuple(size_t i) {
		static if (i >= StageSeq.length)
			alias Tuple = AliasSeq!();
		else {
			alias Tuple = AliasSeq!(StageType!i, StageTypeTuple!(i + 1).Tuple);
		}
	}

	alias TagSpecs = FilterTagAttributes!(0, StageSeq);
	alias Metadata = .Metadata!TagSpecs;
	alias Tuple = StageTypeTuple!0.Tuple;

	Tuple tup;
	Metadata metadata;

	static ref Pipeline outer(size_t thisIndex)(ref StageType!thisIndex thisref) nothrow @trusted
	{
		return *(cast(Pipeline*) (cast(void*) &thisref - Pipeline.init.tup[thisIndex].offsetof));
	}

	static if (isPeekSource!(LastStage)) {
		alias ElementType = SourceElementType!LastStage;
		const(ElementType)[] peek()(size_t n) { return tup[$ - 1].peek(n); }
		void consume()(size_t n) { tup[$ - 1].consume(n); }
	} else static if (isPullSource!(LastStage)) {
		alias ElementType = SourceElementType!LastStage;
		size_t pull()(ElementType[] buf) { return tup[$ - 1].pull(buf); }
	} else {
		// TODO: sink pipelines.

		// Spawns secondary drivers, if any.
		private void spawn(size_t i = 0)()
		{
			static if (isPassiveSink!(StageSeq[i]) && isPassiveSource!(StageSeq[i]))
				tup[i].spawn();
			static if (i + 1 < StageSeq.length)
				spawn!(i + 1)();
		}

		// Calls all secondary drivers for the last time to make sure they have completed.
		private void stop(size_t i = 0)()
		{
			static if (isPassiveSink!(StageSeq[i]) && isPassiveSource!(StageSeq[i]))
				tup[i].stop();
			static if (i + 1 < StageSeq.length)
				stop!(i + 1)();
		}

		void run()()
		{
			spawn();
			enum driver = getFirstDriver!(driveMode, StageSeq);
			static assert(driver < StageSeq.length, "Pipeline " ~ .str!(StageSeq) ~ " has no driver");
			tup[driver].run();
			stop();
		}
	}
}

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

	ulong filterImpl(string f)(ulong a) {
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

	@source!ulong(Method.pull)
	struct TestPullSource(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

		size_t pull(ulong[] buf)
		{
			auto len = min(buf.length, inputArray.length);
			buf[0 .. len] = inputArray[0 .. len];
			inputArray = inputArray[len .. $];
			return len;
		}
	}

	@source!ulong(Method.peek)
	struct TestPeekSource(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

		const(ulong)[] peek(size_t n)
		{
			auto len = min(max(n, 2909), inputArray.length);
			return inputArray[0 .. len];
		}

		void consume(size_t n) { inputArray = inputArray[n .. $]; }
	}

	@source!ulong(Method.push)
	struct TestPushSource(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

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

	@source!ulong(Method.alloc)
	struct TestAllocSource(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

		void run()()
		{
			ulong[] buf;
			while (inputArray.length) {
				auto len = min(1337, inputArray.length);
				if (!sink.alloc(buf, len))
					assert(0);
				buf[0 .. len] = inputArray[0 .. len];
				if (sink.commit(len) != len)
					break;
				inputArray = inputArray[len .. $];
			}
		}
	}

	// sinks:

	@sink(Method.pull)
	struct TestPullSink(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

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

	@sink(Method.peek)
	struct TestPeekSink(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

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

	@sink(Method.push)
	struct TestPushSink(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

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

	@sink(Method.alloc)
	struct TestAllocSink(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;
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

	@filter(Method.peek)
	struct TestPeekFilter(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

		const(ulong)[] peek(size_t n)
		{
			return source.peek(n).map!(filterImpl!"peek").array();
		}
		void consume(size_t n) { source.consume(n); }
	}

	@filter(Method.peek, Method.pull)
	struct TestPeekPullFilter(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

		size_t pull(ulong[] buf)
		{
			auto ib = source.peek(buf.length);
			auto len = min(ib.length, buf.length);
			ib.take(len).map!(filterImpl!"peekPull").copy(buf);
			source.consume(len);
			return len;
		}
	}

	@filter(Method.peek, Method.push)
	struct TestPeekPushFilter(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

		void run()()
		{
			for (;;) {
				auto ib = source.peek(4096);
				auto ob = ib.map!(filterImpl!"peekPush").array();
				source.consume(ib.length);
				if (sink.push(ob) < 4096)
					break;
			}
		}
	}

	@filter(Method.peek, Method.alloc)
	struct TestPeekAllocFilter(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

		void run()()
		{
			ulong[] buf;
			for (;;) {
				auto ib = source.peek(4096);
				if (!sink.alloc(buf, ib.length))
					assert(0);
				auto len = min(ib.length, buf.length);
				ib.take(len).map!(filterImpl!"peekAlloc").copy(buf);
				source.consume(len);
				if (sink.commit(len) < 4096)
					break;
			}
		}
	}

	@filter(Method.pull)
	struct TestPullFilter(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

		size_t pull(ulong[] buf)
		{
			size_t n = source.pull(buf);
			foreach (ref b; buf[0 .. n])
				b = b.filterImpl!"pull";
			return n;
		}
	}

	@filter(Method.pull, Method.peek)
	struct TestPullPeekFilter(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

		const(ulong)[] peek(size_t n)
		{
			auto buf = new ulong[n];
			size_t m = source.pull(buf[]);
			foreach (ref b; buf[0 .. m])
				b = b.filterImpl!"pullPeek";
			return buf[0 .. m];
		}
		void consume(size_t n) {}
	}

	@filter(Method.pull, Method.push)
	struct TestPullPushFilter(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

		void run()()
		{
			for (;;) {
				ulong[4096] buf;
				auto n = source.pull(buf[]);
				foreach (ref b; buf[0 .. n])
					b = b.filterImpl!"pullPush";
				if (sink.push(buf[0 .. n]) < 4096)
					break;
			}
		}
	}

	@filter(Method.pull, Method.alloc)
	struct TestPullAllocFilter(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

		void run()()
		{
			for (;;) {
				ulong[] buf;
				if (!sink.alloc(buf, 4096))
					assert(0);
				auto n = source.pull(buf[]);
				foreach (ref b; buf[0 .. n])
					b = b.filterImpl!"pullAlloc";
				if (sink.commit(n) < 4096)
					break;
			}
		}
	}

	@filter(Method.push)
	struct TestPushFilter(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

		size_t push(const(ulong)[] buf)
		{
			return sink.push(buf.map!(filterImpl!"push").array());
		}
	}

	@filter(Method.push, Method.alloc)
	struct TestPushAllocFilter(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;

		size_t push(const(ulong)[] buf)
		out(result) { assert(result <= buf.length); }
		body
		{
			ulong[] ob;
			if (!sink.alloc(ob, buf.length))
				assert(0);
			auto len = min(buf.length, ob.length);
			buf.take(len).map!(filterImpl!"pushAlloc").copy(ob);
			return sink.commit(len);
		}
	}

	@filter(Method.push, Method.pull)
	struct TestPushPullFilter(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;
		ulong[] buffer;

		size_t push(const(ulong)[] buf)
		{
			buffer ~= buf.map!(filterImpl!"pushPull").array();
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

	@filter(Method.push, Method.peek)
	struct TestPushPeekFilter(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;
		ulong[] buffer;

		size_t push(const(ulong)[] buf)
		{
			buffer ~= buf.map!(filterImpl!"pushPeek").array();
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

	@filter(Method.alloc, Method.alloc)
	struct TestAllocFilter(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;
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
				b = b.filterImpl!"alloc";
			return sink.commit(n);
		}
	}

	@filter(Method.alloc, Method.push)
	struct TestAllocPushFilter(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;
		ulong[] buffer;

		bool alloc(ref ulong[] buf, size_t n)
		{
			buffer = buf = new ulong[n];
			return true;
		}

		size_t commit(size_t n)
		{
			size_t m = sink.push(buffer[0 .. n].map!(filterImpl!"allocPush").array());
			buffer = buffer[m .. $];
			return m;
		}
	}

	@filter(Method.alloc, Method.pull)
	struct TestAllocPullFilter(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;
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
				b = b.filterImpl!"allocPull";
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

	@filter(Method.alloc, Method.peek)
	struct TestAllocPeekFilter(alias Context, A...) {
		mixin TestStage;
		mixin Context!A;
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
				b = b.filterImpl!"allocPeek";
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

	string genChain(DriveMode mode, string filterList)
	{
		import std.algorithm : map;
		import std.array : join, split;
		auto filters = filterList.split(",");
		string midstr;
		if (filters.length > 2)
			midstr = filters[1 .. $ - 1].map!(f => "." ~ genStage(f, "Filter")).join;
		return genStage(filters[0], "Source")
			~ ".drive!(DriveMode.s" ~ (mode == DriveMode.source ? "ource" : "ink") ~ ")"
			~ midstr
			~ "." ~ genStage(filters[$ - 1], "Sink") ~ ";";
	}

	void testChain(DriveMode mode, string filterlist, R)(R r)
		if (isInputRange!R && is(ElementType!R : ulong))
	{
		auto input = r.map!(a => ulong(a)).array();
		logf("Testing %s-driven %s with %d elements",
			mode == DriveMode.source ? "source" : "sink", filterlist, input.length);
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
			mixin(genChain(mode, filterlist));
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
				logf("%s", genChain(mode, filterlist));
				logf("total: %d differences", all);
			}
			assert(all == 0);
		}
	}

	void testChain(string filterlist)()
	{
		import std.range : iota;
		testChain!(DriveMode.sink, filterlist)(iota(0, 173447));
		testChain!(DriveMode.source, filterlist)(iota(0, 173447));
	}

}

unittest {
	auto p1 = pipe!TestPeekSource(Arg!TestPeekSource());
	static assert(isSchema!(typeof(p1)));
	static assert(is(p1.ElementType == ulong));
	auto p2 = pipe!TestPullSource(Arg!TestPullSource());
	static assert(isSchema!(typeof(p2)));
	static assert(is(p2.ElementType == ulong));
}

unittest {
	auto p1 = pipe!TestPushSource(Arg!TestPushSource());
	static assert(isSchema!(typeof(p1)));
	auto p2 = pipe!TestAllocSource(Arg!TestAllocSource());
	static assert(isSchema!(typeof(p2)));
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
	// implicit adapters, pull->alloc
	testChain!`pull,alloc`;
	testChain!`pull,allocPush,push`;
	testChain!`pull,allocPeek,peek`;
	testChain!`pull,allocPull,pull`;
	testChain!`pull,alloc,alloc`;
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
	// implicit adapters, push->peek
	testChain!`push,peek`;
	testChain!`push,peekPush,push`;
	testChain!`push,peekAlloc,alloc`;
	testChain!`push,peek,peek`;
	testChain!`push,peekPull,pull`;
}

unittest {
	// implicit adapters, push->alloc
	testChain!`push,alloc`;
	testChain!`push,allocPush,push`;
	testChain!`push,allocPeek,peek`;
	testChain!`push,allocPull,pull`;
	testChain!`push,alloc,alloc`;
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

unittest {
	// implicit adapters, peek->alloc
	testChain!`peek,alloc`;
	testChain!`peek,allocPush,push`;
	testChain!`peek,allocPeek,peek`;
	testChain!`peek,allocPull,pull`;
	testChain!`peek,alloc,alloc`;
}

unittest {
	// implicit adapters, alloc->peek
	testChain!`alloc,peek`;
	testChain!`alloc,peekPush,push`;
	testChain!`alloc,peekAlloc,alloc`;
	testChain!`alloc,peek,peek`;
	testChain!`alloc,peekPull,pull`;
}

unittest {
	// implicit adapters, alloc->pull
	testChain!`alloc,pull`;
	testChain!`alloc,pullPush,push`;
	testChain!`alloc,pullAlloc,alloc`;
	testChain!`alloc,pullPeek,peek`;
	testChain!`alloc,pull,pull`;
}

unittest {
	// implicit adapters, alloc->push
	testChain!`alloc,push`;
	testChain!`alloc,push,push`;
	testChain!`alloc,pushAlloc,alloc`;
	testChain!`alloc,pushPeek,peek`;
	testChain!`alloc,pushPull,pull`;
}

unittest {
	// implicit adapters, all in one pipeline
	testChain!`alloc,push,peek,pull,alloc,peek,push,pull,peek,alloc,pull,push,alloc`;
}

unittest {
	auto array = [ 1UL, 0xdead, 6 ];
	assert(isSchema!(typeof(array)));
	outputArray.length = 4;
	outputIndex = 0;
	array.pipe!TestPeekSink(Arg!TestPeekSink());
	assert(outputArray[0 .. outputIndex] == array[]);
}

unittest {
	import std.range : iota, array, take;
	import std.algorithm : equal;
	auto r = iota(37UL, 1337);
	static assert(isSchema!(typeof(r)));
	outputArray.length = 5000;
	outputIndex = 0;
	r.pipe!TestPullSink(Arg!TestPullSink());
	assert(outputArray[0 .. outputIndex] == iota(37, 1337).array());
	r = iota(55UL, 1555);
	outputArray.length = 20;
	outputIndex = 0;
	r.pipe!TestPullSink(Arg!TestPullSink());
	assert(outputArray[0 .. outputIndex] == iota(55, 1555).take(20).array());
}

unittest {
	import std.algorithm : copy;
	import std.range : iota;
	outputArray.length = 5000;
	outputIndex = 0;
	pipe!(ulong, (o)
		{
			auto r = iota(42UL, 1024);
			r.copy(o);
		})
		.pipe!TestPushSink(Arg!TestPushSink());
	assert(outputArray[0 .. outputIndex] == iota(42UL, 1024).array());
}
