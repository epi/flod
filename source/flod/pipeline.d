/** Pipeline composition.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.pipeline;

import flod.meta : isType, NonCopyable, moveIfNonCopyable, str;
import flod.traits;

/*

Append:

  to what        what              comment
------------------------------------------------------
  nothing        pull source       direct
  nothing        peek source       direct
t nothing        push source       deferred
t nothing        alloc source      deferred


  nothing        pull sink
  nothing        pull/pull filter
  nothing        pull/peek filter
  nothing        pull/push filter
  nothing        pull/alloc filter

  nothing        peek sink
  nothing        peek/pull filter
  nothing        peek/peek filter
  nothing        peek/push filter
  nothing        peek/alloc filter

  nothing        push sink
  nothing        push/pull filter
  nothing        push/peek filter
  nothing        push/push filter
  nothing        push/alloc filter

  nothing        alloc sink
  nothing        alloc/pull filter
  nothing        alloc/peek filter
  nothing        alloc/push filter
  nothing        alloc/alloc filter


t pull source    pull sink         direct
t pull source    pull/pull filter  direct
t pull source    pull/peek filter  direct
  pull source    pull/push filter  direct, deferred
  pull source    pull/alloc filter direct, deferred

t pull source    peek sink         needs buffering adapter
t pull source    peek/pull filter  needs buffering adapter
t pull source    peek/peek filter  needs buffering adapter
  pull source    peek/push filter  needs buffering adapter, deferred
  pull source    peek/alloc filter needs buffering adapter, deferred

  pull source    push sink
  pull source    push/pull filter
  pull source    push/peek filter
  pull source    push/push filter
  pull source    push/alloc filter

  pull source    alloc sink
  pull source    alloc/pull filter
  pull source    alloc/peek filter
  pull source    alloc/push filter
  pull source    alloc/alloc filter

t peek source    pull sink         needs copying adapter
t peek source    pull/pull filter  needs copying adapter
t peek source    pull/peek filter  needs copying adapter
  peek source    pull/push filter  needs copying adapter, deferred
  peek source    pull/alloc filter needs copying adapter, deferred

t peek source    peek sink         direct
t peek source    peek/pull filter  direct
t peek source    peek/peek filter  direct
  peek source    peek/push filter  direct
  peek source    peek/alloc filter direct

  peek source    push sink
  peek source    push/pull filter
  peek source    push/peek filter
  peek source    push/push filter
  peek source    push/alloc filter

  peek source    alloc sink
  peek source    alloc/pull filter
  peek source    alloc/peek filter
  peek source    alloc/push filter
  peek source    alloc/alloc filter

*/


/**
Run a `pipeline` by repeatedly calling `pipeline.step`.
*/
int run(Pipeline)(auto ref Pipeline pipeline)
{
	for (;;) {
		int r = pipeline.step();
		if (r != 0)
			return r;
	}
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
	if (isPassiveSource!Pipeline && isActiveSink!Stage)
{
	import flod.adapter : DefaultPullPeekAdapter, DefaultPeekPullAdapter;
	static if ((isPeekSource!Pipeline && isPeekSink!Stage) || (isPullSource!Pipeline && isPullSink!Stage)) {
		return Stage!Pipeline(moveIfNonCopyable(pipeline), args);
	} else static if (isPeekSource!Pipeline && isPullSink!Stage) {
		debug pragma(msg, file, ":", line, ": Inserting implicit peek-pull adapter");
		return pipeline.pipe!DefaultPeekPullAdapter.pipe!Stage(args);
	} else static if (isPullSource!Pipeline && isPeekSink!Stage) {
		debug pragma(msg, file, ":", line, ": Inserting implicit pull-peek adapter");
		return pipeline.pipe!DefaultPullPeekAdapter.pipe!Stage(args);
	} else {
		// TODO: give a better diagnostic message
		static assert(0, "Cannot instantiate " ~ str!Stage ~ "!" ~ str!Pipeline);
	}
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
  Pull       +    +    -    -
  Peek       +    +    -    -
  Push       +    -    -    -
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
	static assert(isAllocSink!TestAllocSink && !isSource!TestAllocSink);

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
		static if (hasGenericPeek!Source) {
			const(T)[] peek(T = ubyte)(size_t n) { return s.peek!T(n); }
			void consume(T = ubyte)(size_t n) { s.consume!T(n); }
		} else {
			alias T = DefaultPeekType!Source;
			const(T)[] peek(size_t n) { return s.peek(n); }
			void consume(size_t n) { s.consume(n); }
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

unittest {
	import flod.adapter;
	assert(TestPeekSource().pipe!TestPeekSink.run() == 1);
	assert(TestPeekSource().pipe!TestPeekFilter.pipe!TestPeekSink.run() == 1);
	assert(TestPullSource().pipe!TestPullSink(31337).run() == 0);
	assert(TestPullSource().pipe!TestPullFilter.pipe!TestPullSink(31337).run() == 0);
	assert(TestPullSource().pipe!TestPullFilter.pipe!TestPullFilter.pipe!TestPullFilter
		.pipe!TestPullSink(31337).run() == 0);
	assert(TestPullSource()
		.pipe!TestPullFilter
		.pipe!TestPullPeekFilter
		.pipe!TestPeekFilter
		.pipe!TestPeekSink.run() == 1);
	assert(TestPeekSource()
		.pipe!TestPeekFilter
		.pipe!TestPeekPullFilter
		.pipe!TestPullFilter
		.pipe!TestPullSink.run() == 0);
	assert(TestPeekSource()
		.pipe!TestPeekFilter
		.pipe!TestPeekPullFilter
		.pipe!TestPullFilter
		.pipe!TestPullFilter
		.pipe!TestPullPeekFilter
		.pipe!TestPeekFilter
		.pipe!TestPeekFilter
		.pipe!TestPeekSink.run() == 1);
	// with implicit adapters
	assert(TestPullSource()
		.pipe!TestPeekSink().run() == 1);
	assert(TestPullSource()
		.pipe!TestPeekPullFilter
		.pipe!TestPeekPullFilter
		.pipe!TestPeekSink().run() == 1);
	assert(TestPullSource()
		.pipe!TestPullFilter
		.pipe!TestPeekFilter
		.pipe!TestPeekSink().run() == 1);
	assert(TestPeekSource()
		.pipe!TestPullSink().run() == 0);
	assert(TestPeekSource()
		.pipe!TestPullPeekFilter
		.pipe!TestPullPeekFilter
		.pipe!TestPullSink().run() == 0);
	assert(TestPeekSource()
		.pipe!TestPeekFilter
		.pipe!TestPullFilter
		.pipe!TestPullSink().run() == 0);
	assert(TestPeekSource()
		.pipe!TestPeekFilter
		.pipe!TestPullFilter
		.pipe!TestPeekFilter
		.pipe!TestPullFilter
		.pipe!TestPeekFilter
		.pipe!TestPullFilter
		.pipe!TestPullSink().run() == 0);
}

/**
Create a new pipeline starting with `Stage`.

Since the sink type is not known at this point, `Stage` is not constructed immediately.
Instead, `args` are saved and will be used to construct `Stage!Sink` as soon as type of `Sink` is known.
*/
auto pipe(alias Stage, Args...)(auto ref Args args)
	if (isActiveSource!Stage)
{
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
	assert(pipe!TestPushSource(0xdeadUL).create(TestPushSink()).run() == 1);
	assert(pipe!TestAllocSource("test", 0UL).create(TestAllocSink()).run() == 1);
}

/+
struct PullSource {
	mixin NonCopyable;

	size_t pull(T)(T[] buf)
	{
		buf[] = T.init;
		return buf.length;
	}
}

auto inits()
{
	return PullSource();
}
+/

/+
template PullFilter(T) {
	struct PullFilter(Source) {
	private:
		Source source;
		T what;
		T withWhat;

	public:
		mixin NonCopyable;

		size_t pull(T[] buf)
		{
			source.pull(buf);
			foreach (ref b; buf) {
				if (b == what)
					b = withWhat;
			}
			return buf.length;
		}
	}
}


auto replace(Pipeline, T)(auto ref Pipeline pipeline, T what, T withWhat)
{
	return pipeline.pipe!(PullFilter!T)(what, withWhat);
}

unittest
{
	auto x = inits().replace(ubyte.init, ubyte(5));
	ubyte[100] buf;
	x.pull(buf);
	import std.range : repeat, array;
	assert(buf[] == repeat(ubyte(5), 100).array());
}

unittest
{
	auto i = inits();
	auto r = i.replace(ubyte.init, ubyte(5));
	ubyte[100] buf;
}
+/

auto deferredCreate2(alias Stage, Pipeline, Args...)(auto ref Pipeline pipeline, auto ref Args args)
{
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
	return DeferredCtor(moveIfNonCopyable(pipeline), args);
}

auto chainedDeferredCreate(alias Stage, Next, Args...)(auto ref Next next, auto ref Args args)
{
	static struct ChainedDeferredCtor {
		mixin NonCopyable;
		Next next;
		Args args;

		auto create(Sink)(auto ref Sink sink)
		{
			//import std.algorithm : move;
			return next.create(Stage!Sink(moveIfNonCopyable(sink), args));
		}
	}
	//import std.algorithm : move;
	return ChainedDeferredCtor(moveIfNonCopyable(next), args);
}

/+
template PushSource(T) {
	struct PushSource(Sink) {
		mixin NonCopyable;

		Sink sink;
		const(T)[] blob;

		bool step()()
		{
			return sink.push(blob) != blob.length;
		}
	}
}

auto blobPush(T)(const(T)[] arr)
{
	return pipe!(PushSource!T)(arr);
}
+/

/+
struct PushSink {
	mixin NonCopyable;

	size_t push(T)(const(T)[] buf)
	{
		import std.stdio : writeln;
		writeln(buf);
		return buf.length;
	}
}

auto pushSink(Chain)(auto ref Chain chain)
{
	return chain.create(PushSink());
}
unittest
{
	auto x = pipe!(PushSource!int)([1, 2, 3]);
	auto y = blobPush([1, 2, 3]);
	blobPush([ 1, 2, 3 ]).pushSink().step();
}

struct PushTake(Sink) {
	mixin NonCopyable;

private:
	Sink sink;
	size_t count;

public:
	size_t push(T)(const(T)[] buf)
	{
		if (count >= buf.length) {
			count -= buf.length;
			return sink.push(buf);
		} else if (count > 0) {
			auto c = count;
			count = 0;
			return sink.push(buf[0 .. c]);
		} else {
			return 0;
		}
	}
}

auto take(Chain)(auto ref Chain chain, size_t count)
{
	return chainedDeferredCreate!PushTake(chain, count);
}

unittest
{
	blobPush("test").take(20).take(20).pushSink().run();
}

import std.stdio : File;

struct FileWriter {
	mixin NonCopyable;
	File file;

	size_t push(T)(const(T)[] buf)
	{
		file.rawWrite(buf);
		return buf.length;
	}
}

auto writeFile(Pipeline)(auto ref Pipeline pipeline, File file)
{
	return pipeline.create(FileWriter(file));
}

unittest
{
	blobPush("test").take(100).writeFile(File("test", "wb")).run();
}

void writerep(T)(ref const(T) p)
{
	import std.stdio : writefln;

	writefln("%(%02x%| %)", (cast(const(ubyte)*) &p)[0 .. T.sizeof]);
}

unittest
{
	auto f = File("best", "wb");
	auto source = blobPush("test");
	writerep(source);
	auto tak = source.take(100);
	writerep(tak);
	auto wri = tak.writeFile(f);
	writerep(source);
	writerep(tak);
	writerep(wri);

	blobPush("test").take(100).writeFile(f).run();
	f.rawWrite("X");
}

auto fromFile(File file)
{
	static struct FileReader {
		mixin NonCopyable;
		File file;

		size_t pull(T)(T[] buf)
		{
			return file.rawRead(buf).length;
		}
	}

	return FileReader(file);
}


auto pullPush(size_t chunkSize = 4096, Pipeline)(auto ref Pipeline pipeline)
{
	return deferredCreate2!(PullPush!(chunkSize))(moveIfNonCopyable(pipeline));
}

unittest
{
	fromFile(File("log")).pullPush.take(31337).take(31337).take(31337).take(31337).take(31337).take(31337).take(31337).take(31337).writeFile(File("log_copy", "wb")).run();
}

+/


version(FlodBloat):

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
