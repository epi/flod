/** Pipeline composition.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.pipeline;

import std.stdio;
import std.meta : staticMap, Filter, allSatisfy;
import std.experimental.allocator : IAllocator;

import flod.traits;

// Stage holds the information required to construct a stream component.
private struct Stage(alias C, ArgsList...) if (isStreamComponent!C) {
	/// The actual stream component type or template alias
	alias Impl = C;
	alias Args = ArgsList;
	// Arguments passed to stream component's ctor
	Args args;
}

private template isStage(Ss...) {
	static if (Ss.length == 1) {
		alias S = Ss[0];
		enum bool isStage = is(S == Stage!TL, TL...);
	} else {
		enum bool isStage = false;
	}
}

private template hasCtorArgs(S) if (isStage!S) {
	enum bool hasCtorArgs = S.Args.length > 0;
}

private template staticSum(x...) {
	static if (x.length == 0)
		enum staticSum = 0;
	else
		enum staticSum = x[0] + staticSum!(x[1 .. $]);
}

private template streamDescription(Stages...)
if (allSatisfy!(isStage, Stages)) {
	static if (Stages.length == 0) {
		enum string streamDescription = "(empty)";
	} else static if (Stages.length == 1) {
		enum string streamDescription = __traits(identifier, Stages[0].Impl);
	} else {
		enum string streamDescription = __traits(identifier, Stages[0].Impl)
			~ "->" ~ streamDescription!(Stages[1 .. $]);
	}
}

// StageTuple stores only stages that have some args passed to their ctors
// tupleIndex converts the index in the list of all Stages to
// an index in the compressed list.
private template tupleIndex(int index, Stages...) {
	enum tupleIndex = staticSum!(staticMap!(hasCtorArgs, Stages)[0 .. index]);
}

// dynamically allocated tuple of stages
// quite hacky, void ptrs are cast there and back in Pipeline
private struct StageTuple {
	IAllocator allocator;
	uint refs = 1;
	void function(StageTuple* p) free;
	void*[] stages;
}

private void freeStages(Stages...)(StageTuple* p)
	if (allSatisfy!(hasCtorArgs, Stages))
{
	import std.experimental.allocator : dispose;

	static if (Stages.length > 0) {
		// dispose last stage spec
		enum index = Stages.length - 1;
		alias Disposed = Stages[index];
		p.allocator.dispose(cast(Disposed*) p.stages[index]);
		// and then the rest, recursively
		freeStages!(Stages[0 .. $ - 1])(p);
	}
}

/** Pipeline is a container which holds all information necessary to instantiate and _run a _stream.
 *
 * Pipeline does not have a publicly accessible constructor. Instead, free function `pipe` should be used.
 */
private struct Pipeline(Stages...) if (Stages.length > 0 && allSatisfy!(isStage, Stages)) {
	import std.experimental.allocator : make, makeArray, expandArray, dispose, allocatorObject;

	alias AllStages = Stages;
	alias StagesWithCtorArgs = Filter!(hasCtorArgs, Stages);

	enum firstStageName = __traits(identifier, Stages[0].Impl);
	enum lastStageName = __traits(identifier, Stages[$ - 1].Impl);

	alias FirstStage = Stages[0].Impl;
	alias LastStage = Stages[$ - 1].Impl;

	enum isInput = isSourceOnly!(Stages[0].Impl) && isSource!(Stages[$ - 1].Impl);
	enum isOutput = isSink!(Stages[0].Impl) && isSinkOnly!(Stages[$ - 1].Impl);
	enum isComplete = isSourceOnly!(Stages[0].Impl) && isSinkOnly!(Stages[$ - 1].Impl);
	enum string staticToString = streamDescription!Stages;

	// Reference-counted storage for arguments passed to constructors of each stage
	private StageTuple* _res; // TODO: rename to _stageTuple

	static if (Stages.length == 1) {
		private this(Args...)(IAllocator allocator, auto ref Args args)
		{
			_res = allocator.make!StageTuple;
			assert(_res);
			assert(_res.refs == 1);
			_res.allocator = allocator;
			_res.stages = _res.allocator.makeArray!(void*)(4);
			static if (hasCtorArgs!(Stages[0])) {
				assert(_res.stages[0] is null);
				_res.stages[0] = _res.allocator.make!(Stages[0])(args);
			}
			_res.free = &freeStages!StagesWithCtorArgs;
		}
	} else static if (Stages.length > 1) {
		private this(Args...)(StageTuple* st, auto ref Args args)
		{
			assert(st);
			assert(st.refs);
			_res = st;
			static if (hasCtorArgs!(Stages[$ - 1])) {
				if (_res.stages.length < StagesWithCtorArgs.length) {
					_res.allocator.expandArray(_res.stages, 4);
					import std.exception : enforce;
					enforce(_res.stages.length >= StagesWithCtorArgs.length,
						"Failed to expand the list of stages: out of memory");
				}
				enum index = StagesWithCtorArgs.length - 1;
				assert(_res.stages[index] is null);
				_res.stages[index] = _res.allocator.make!(StagesWithCtorArgs[index])(args);
				_res.free = &freeStages!StagesWithCtorArgs;
			}
			_res.refs++;
		}
	}

	this(this)
	{
		if (!_res)
			return;
		assert(_res.refs);
		_res.refs++;
	}

	void opAssign(Pipeline rhs)
	{
		import std.algorithm : swap;
		swap(this, rhs);
	}

	~this()
	{
		if (!_res)
			return;
		assert(_res.refs >= 1);
		if (_res.refs == 1) {
			_res.free(_res);
			_res.refs = 0;
			auto allocator = _res.allocator;
			allocator.dispose(_res.stages);
			allocator.dispose(_res);
			_res = null;
		} else {
			_res.refs--;
			assert(_res.refs);
		}
	}

	private template StreamBuilder(int begin, int cur, int end) {
		static assert(begin <= end && begin <= cur && cur <= end && end <= Stages.length,
			"Invalid parameters: " ~
			Stages.stringof ~ "[" ~ begin.stringof ~ "," ~ cur.stringof ~ "," ~ end.stringof ~ "]");
		static if (cur < end) {
			alias Cur = Stages[cur].Impl;
			alias Lhs = StreamBuilder!(begin, begin, cur);
			alias Rhs = StreamBuilder!(cur + 1, cur + 1, end);
			static if (is(Lhs.Impl _Li))
				alias LhsImpl = _Li;
			static if (is(Rhs.Impl _Ri))
				alias RhsImpl = _Ri;
			static if (begin + 1 == end && is(Cur _Impl)) {
				alias Impl = _Impl;
			} else static if (cur + 1 == end && is(Cur!LhsImpl _Impl)) {
				alias Impl = _Impl;
			} else static if (cur == begin && is(Cur!RhsImpl _Impl)) {
				alias Impl = _Impl;
			} else static if (is(Cur!(LhsImpl, RhsImpl) _Impl)) {
				alias Impl = _Impl;
			}
			static if (is(Impl)) {
				void construct()(ref Impl impl) {
					static if (is(LhsImpl))
						Lhs.construct(impl.source);
					static if (is(RhsImpl))
						Rhs.construct(impl.sink);
					static if (hasCtorArgs!(Stages[cur])) {
						alias Requested = Stages[cur];
						enum index = tupleIndex!(cur, Stages);
						auto stage = cast(Stages[cur]*) _res.stages[index];
						impl.__ctor(stage.args);
					}
					// writefln("%-30s %X[%d]: [%(%02x%|, %)]", Stages[cur].Impl.stringof, &impl, impl.sizeof, (cast(ubyte*) &impl)[0 .. impl.sizeof]);
				}
			} static if (cur + 1 < end) {
				alias Next = StreamBuilder!(begin, cur + 1, end);
				static if (is(Next.Impl))
					alias StreamBuilder = Next;
			}
		}
	}

	static if (is(StreamBuilder!(0, 0, Stages.length).Impl _Impl)) {
		alias Impl = _Impl;
		void construct()(ref Impl impl) {
			StreamBuilder!(0, 0, Stages.length).construct(impl);
		}
	}

	/** Appends next stage at the end of data processing pipeline.
	 *
	 * Params:
	 * NextStage = Stream component (struct or struct template) to be used as the next stage in the pipeline.
	 * args      = Arguments passed to the constructor when the stream is instantiated.
	 *
	 * Returns:
	 * A new Pipeline with NextStage appended at the end.
	 *
	 * Notes:
	 * Constructor of NextStage is not executed at this point.
	 * args are copied to temporary storage for a deferred invocation of the constructor.
	 */
	auto pipe(alias NextStage, Args...)(auto ref Args args)
	{
		static if (areCompatible!(LastStage, NextStage)) {
			assert(_res);
			alias S = Stage!(NextStage, Args);
			return Pipeline!(Stages, S)(_res, args);
		} else {
			static if (!isSource!LastStage) {
				pragma(msg, "Error: ", lastStageName, " is not a source");
			}
			static if (!isSink!NextStage) {
				pragma(msg, "Error: ", NextStage.stringof, " is not a sink");
			}
			static if (!areCompatible!(LastStage, NextStage)) {
				pragma(msg, "Error: ", lastStageName,
				" produces data using different method than ",
					NextStage.stringof, " requires");
			}
			static assert(0, NextStage.stringof ~ " cannot sink data from " ~ lastStageName);
		}
	}

	/// Instantiate the stream and perform all the processing.
	void run()()
	{
		static assert(isComplete, "Cannot run an incomplete pipeline");
		import std.stdio;
		//	alias Builder = StreamBuilder!(0, 0, Stages.length);
		static assert(is(Impl), "Could not build stream out of the following list of stages: " ~ Stages.stringof);
		Impl impl;
		construct(impl);
		writeln(typeof(impl).stringof, ": ", impl.sizeof);
		static if (canRun!Impl)
			impl.run();
		else static if (canStep!Impl)
			while (impl.step()) {}
		else
			static assert(false);
	}

	@property IAllocator allocator()
	{
		return _res.allocator;
	}
}

/// Returns `true` if `Ss` is a pipeline.
template isPipeline(P) {
	static if (is(P == Pipeline!TL, TL...)) {
		import std.meta : allSatisfy;
		enum isPipeline = TL.length > 0 && allSatisfy!(isStage, TL);
	}
	else {
		enum isPipeline = false;
	}
}

/// An empty _pipeline.
private struct Pipeline() {
	private IAllocator allocator;

	/// Adds FirstStage to the _pipeline.
	auto pipe(alias FirstStage, Args...)(auto ref Args args)
		if (isStreamComponent!FirstStage)
	{
		alias S = Stage!(FirstStage, Args);
		return Pipeline!S(allocator, args);
	}

	/// Adds array as first _stage in the _pipeline.
	auto pipe(T)(const(T)[] array)
	{
		static struct ArraySource {
			const(T)[] array;
			this(const(T)[] array) { this.array = array; }
			const(T)[] peek(size_t n) { return array; }
			void consume(size_t n) { array = array[n .. $]; }
		}
		return this.pipe!ArraySource(array);
	}
}

/// Creates a _pipeline which will use allocator for all its memory management.
auto pipe(IAllocator allocator)
{
	return Pipeline!()(allocator);
}

/// Creates a _pipeline with default configuration and adds FirstStage as its only _stage.
auto pipe(alias FirstStage, Args...)(auto ref Args args)
	if (isStreamComponent!FirstStage)
{
	import std.experimental.allocator : theAllocator;
	return pipe(theAllocator).pipe!FirstStage(args);
}

/// Creates a _pipeline with default configuration and adds array as its first _stage.
auto pipe(T)(const(T)[] array)
{
	import std.experimental.allocator : theAllocator;
	return pipe(theAllocator).pipe(array);
}

struct RefTracker
{
	import std.stdio : stderr;

	import core.stdc.stdlib: malloc, free;

	this(int a)
	{
		this.refs = cast(uint*) malloc(uint.sizeof);
		this.a = a;
		*refs = 1;
//		stderr.writefln("%d ctor refs=%d", a, *refs);
	}

	int a;
	uint* refs;

	auto opAssign(RefTracker rhs)
	{
		import std.algorithm : swap;
		swap(this, rhs);
	}

	this(this)
	{
		++*refs;
//		stderr.writefln("%d copy refs=%d", a, *refs);
	}

	~this()
	{
		if (!refs)
			return;
		--*refs;
//		stderr.writefln("%d dtor refs=%d", a, *refs);
		if (*refs == 0) {
			free(this.refs);
			this.refs = null;
		}
	}
}

struct Forward(Sink) {
	Sink sink;
	RefTracker x;

	///
	this(ref RefTracker x)
	{
		this.x = x;
	}

	size_t push(T)(const(T[]) data)
	{
		return sink.push(data);
	}
}

unittest
{
	import flod.common : discard;
	import std.stdio;

	import flod.file :FileReader, FileWriter;
	import flod.common : take, drop, NullSource;
	import flod.adapter : PullPush;

	stderr.writeln("BEFORE");
	{
		import std.experimental.allocator.building_blocks.stats_collector : StatsCollector, Options;
		import std.experimental.allocator.building_blocks.region : InSituRegion;
		import std.experimental.allocator.mallocator : Mallocator;
		import std.experimental.allocator : allocatorObject;
		auto x = allocatorObject(StatsCollector!(Mallocator, Options.all)());

		{
			auto s = pipe(x)
				.pipe!FileReader("/etc/passwd")
					.drop(3)
					.pipe!PullPush.take(3)
					.pipe!Forward(RefTracker(1))
					.pipe!Forward(RefTracker(2))
					.pipe!Forward(RefTracker(3))
					.pipe!Forward(RefTracker(4))
					.pipe!Forward(RefTracker(5))
					.pipe!Forward(RefTracker(6))
					.pipe!Forward(RefTracker(7))
					.pipe!Forward(RefTracker(8))
					.pipe!Forward(RefTracker(9))
					.pipe!Forward(RefTracker(10))
					.pipe!Forward(RefTracker(11))
					.pipe!Forward(RefTracker(12))
					.pipe!FileWriter("ep.out");

	//		x.impl.reportStatistics(stderr);

			stderr.writeln("CREATED");
			s.run();
			x.impl.writeln("EXECUTED");
		}
	//	x.impl.reportStatistics(stderr);
	}
	stderr.writeln("AFTER");
	//	auto stream1 = stream!CurlReader("http://icecast.radiovox.org:8000/live.ogg").discard();
	//	pragma(msg, typeof(stream1));
	//	pragma(msg, stream1.sizeof);


	//	auto stream2 = stream!CurlReader("http://icecast.radiovox.org:8000/live.ogg", Test!()(14)).pipe!BufferedToUnbufferedPushSource.pipe!OggReader.pipe!PushTee.pipe!VorbisDecoder.pipe!AlsaSink;
	//	pragma(msg, typeof(stream2));
	//	pragma(msg, stream2.sizeof);
	//stream0.run;
	//stream1.run();
	//stream2a.run();
	//stream2.run();
}
