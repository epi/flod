/** Adapters connecting stages with incompatible interfaces.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.adapter;

import flod.pipeline: pipe, isPullPipeline, isPeekPipeline;
import flod.traits;

private template DefaultPullPeekAdapter(Buffer, E)
{
	@pullSink!E @peekSource!E
	struct DefaultPullPeekAdapter(Source) {
		Source source;
		Buffer buffer;

		this()(auto ref Buffer buffer)
		{
			import flod.meta : moveIfNonCopyable;
			this.buffer = moveIfNonCopyable(buffer);
		}

		const(E)[] peek(size_t size)
		{
			auto ready = buffer.peek!E();
			if (ready.length >= size)
				return ready;
			auto chunk = buffer.alloc!E(size - ready.length);
			size_t r = source.pull(chunk);
			buffer.commit!E(r);
			return buffer.peek!E();
		}

		void consume(size_t size)
		{
			buffer.consume!E(size);
		}
	}
}

unittest {
	import flod.buffer : NullBuffer;
	static assert(isPeekSource!(DefaultPullPeekAdapter!(NullBuffer, int)));
}

///
auto pullPeek(Pipeline, Buffer)(auto ref Pipeline pipeline, auto ref Buffer buffer)
	if (isPullPipeline!Pipeline)
{
	return pipeline.pipe!(DefaultPullPeekAdapter!(Buffer, Pipeline.ElementType))(buffer);
}

///
auto pullPeek(Pipeline)(auto ref Pipeline pipeline)
	if (isPullPipeline!Pipeline)
{
	import flod.buffer : movingBuffer;
	return pipeline.pullPeek(movingBuffer());
}

unittest {

}

/+
struct DefaultPeekPullAdapter(Source) {
private:
	Source source;

public:
	size_t pull(T)(T[] buf)
	{
		import std.algorithm : min;
		static if (is(FixedPeekType!Source == T)) {
			auto inbuf = source.peek(buf.length);
		} else static if (!is(FixedPeekType!Source)) {
			auto inbuf = source.peek!T(buf.length);
		} else {
			import flod.meta : str;
			pragma(msg, str!Source ~ " does not support type " ~ str!T);
		}
		auto l = min(buf.length, inbuf.length);
		buf[0 .. l] = inbuf[0 .. l];
		source.consume(l);
		return buf.length;
	}
}
static assert(isPullSource!DefaultPeekPullAdapter);
static assert(isPeekSink!DefaultPeekPullAdapter);


struct DefaultPullPushAdapter(Source, Sink) {
	pragma(msg, "instantiating with ", str!Source, ",", str!Sink);
//private:
	alias T = CommonType!(Source, Sink, ubyte);
	Source source;
	Sink sink;
	size_t chunkSize;

public:
	void run()()
	{
		import core.stdc.stdlib : alloca;
		auto buf = (cast(T*) alloca(T.sizeof * chunkSize))[0 .. chunkSize];
		for (;;) {
			size_t inp = source.pull(buf[]);
			if (inp == 0)
				break;
			if (sink.push(buf[0 .. inp]) < chunkSize)
				break;
		}
	}
}
static assert(isPullSink!DefaultPullPushAdapter);
static assert(isPushSource!DefaultPullPushAdapter);

///
auto pullPush(Pipeline)(auto ref Pipeline pipeline, size_t chunkSize = 4096)
	if (isPullPipeline!Pipeline)
{
	import flod.pipeline : pipe, isDeferredPipeline;
	auto result = pipeline.pipe!DefaultPullPushAdapter(chunkSize);
	static assert(isDeferredPipeline!(typeof(result)));
	return result;
}

struct DefaultPeekPushAdapter(Source, Sink) {
private:
	alias T = CommonType!(Source, Sink, ubyte);
	Source source;
	Sink sink;
	size_t chunkSize;

public:
	void run()()
	{
		for (;;) {
			auto buf = source.peek!T(chunkSize);
			if (buf.length == 0)
				break;
			size_t w = sink.push(buf[]);
			if (w < chunkSize)
				break;
			source.consume(w);
		}
	}
}
static assert(isPeekSink!DefaultPeekPushAdapter);
static assert(isPushSource!DefaultPeekPushAdapter);

///
auto peekPush(Pipeline)(auto ref Pipeline pipeline, size_t chunkSize = size_t.sizeof)
	if (isPeekPipeline!Pipeline)
{
	import flod.pipeline : pipe, isDeferredPipeline;
	auto result = pipeline.pipe!DefaultPeekPushAdapter(chunkSize);
	pragma(msg, typeof(result).stringof, " ", str!(typeof(result)));
	static assert(isDeferredPipeline!(typeof(result)));
	return result;
}

unittest {
	import std.array : appender;
	import std.range : iota, array;
	import flod.pipeline : toOutputRange;
	auto app = appender!(uint[]);
	iota(0, 1048576).array().peekPush().toOutputRange(app).run();
	assert(app.data == iota(0, 1048576).array());
}

+/


template DefaultPushPullAdapter(Buffer, E) {
	@pushSink!E @pullSource!E
	struct DefaultPushPullAdapter(alias Scheduler) {
		mixin Scheduler;

		Buffer buffer;
		const(void)[] pushed;

		this()(auto ref Buffer buffer) {
			this.buffer = moveIfNonCopyable(buffer);
		}

		size_t push(const(E)[] buf)
		{
			if (pushed.length > 0)
				return 0;
			pushed = cast(const(void)[]) buf;
			yield();
			return buf.length;
		}

		private E[] pullFromBuffer(E[] dest)
		{
			import std.algorithm : min;
			auto src = buffer.peek!E();
			auto len = min(src.length, dest.length);
			if (len > 0) {
				dest[0 .. len] = src[0 .. len];
				buffer.consume!E(len);
				return dest[len .. $];
			}
			return dest;
		}

		// FIXME: unsafe, type information is lost between push and pull
		size_t pull(E[] dest)
		{
			size_t requestedLength = dest.length;
			// first, give off whatever was left from this.pushed on previous pull();
			dest = pullFromBuffer(dest);
			if (dest.length == 0)
				return requestedLength;
			// if not satisfied yet, switch to source fiber till push() is called again
			// enough times to fill dest[]
			auto untyped = cast(void[]) dest;
			do {
				import std.algorithm : min;
				if (yield())
					break;

				// pushed is the slice of the original buffer passed to push() by the source.
				auto len = min(pushed.length, untyped.length);
				assert(len > 0);
				untyped.ptr[0 .. len] = pushed[0 .. len];
				untyped = untyped[len .. $];
				pushed = pushed[len .. $];
			} while (untyped.length > 0);

			// whatever's left in pushed, keep it in buffer for the next time pull() is called
			while (pushed.length > 0) {
				import std.algorithm : min;
				auto b = buffer.alloc!void(pushed.length);
				if (b.length == 0) {
					import core.exception : OutOfMemoryError;
					throw new OutOfMemoryError();
				}
				auto len = (b.length, pushed.length);
				b[0 .. len] = pushed[0 .. len];
				buffer.commit!void(len);
				pushed = pushed[len .. $];
			}
			return requestedLength - untyped.length / E.sizeof;
		}
	}
}

///
auto pushPull(Pipeline, Buffer)(auto ref Pipeline pipeline, auto ref Buffer buffer)
{
	alias E = Pipeline.ElementType;
	alias PP = DefaultPushPullAdapter!(Buffer, E);
	return pipeline.pipe!PP(buffer);
}

///
auto pushPull(Pipeline)(auto ref Pipeline pipeline)
{
	import flod.buffer : movingBuffer;
	return pipeline.pushPull(movingBuffer());
}

import flod.meta;
import flod.traits : pushSource;

@pushSource!int @check!ArraySource
static struct ArraySource(Sink) {
	int[] array;
	int counter = 1;

	this(int[] arr) { array = arr; }
	Sink sink;

	void run()
	{
		while (array.length) {
			import std.algorithm : min;
			auto l = min(array.length, counter);
			assert(l);
			if (l != sink.push(array[0 .. l]))
				break;
			array = array[l .. $];
			++counter;
		}
	}
}

unittest {

	import std.range : iota, array;
	import flod.buffer;
	import flod.pipeline : pipe;

	auto arr = iota(0, 1048576).array();
	int[] result;
	auto pl = pipe!ArraySource(arr.dup).pushPull().create();
	auto n = 100;
	result.length = n;
	assert(pl.pull(result) == n);
	assert(result[0 .. n] == arr[0 .. n]);
	arr = arr[n .. $];

	n = 1337;
	result.length = n;
	assert(pl.pull(result) == n);
	assert(result[0 .. n] == arr[0 .. n]);
	arr = arr[n .. $];

	n = 4000000;
	result.length = n;
	assert(pl.pull(result) == arr.length);
	assert(result[0 .. arr.length] == arr[0 .. arr.length]);
}
