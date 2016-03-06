/** Adapters connecting stages with incompatible interfaces.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.adapter;

import flod.pipeline: pipe, isPullPipeline, isPeekPipeline;
import flod.traits;
import flod.meta;

template PullElementType(Source, Default) {
	static if (is(FixedPullType!Source F))
		alias PullElementType = F;
	else
		alias PullElementType = Default;
}

template isPullable(Source, ElementType) {
	static if (is(FixedPullType!Source F))
		enum isPullable = is(ElementType == F);
	else
		enum isPullable = true;
}

private template DefaultPullPeekAdapter(Buffer)
{
	@implements!(PullSink, PeekSource, DefaultPullPeekAdapter)
	struct DefaultPullPeekAdapter(Source) {
	private:
		Source source;
		Buffer buffer;

		alias ElementType = PullElementType!(Source, ubyte);

	package:
		this()(auto ref Source source, auto ref Buffer buffer)
		{
			import flod.meta : moveIfNonCopyable;
			this.source = moveIfNonCopyable(source);
			this.buffer = moveIfNonCopyable(buffer);
		}

	public:
		const(T)[] peek(T = ElementType)(size_t size)
			if (isPullable!(Source, T))
		{
			auto ready = buffer.peek!T();
			if (ready.length >= size)
				return ready;
			auto chunk = buffer.alloc!T(size - ready.length);
			size_t r = source.pull(chunk);
			buffer.commit!T(r);
			return cast(T[]) buffer.peek!T();
		}

		void consume(T = ElementType)(size_t size)
			if (isPullable!(Source, T))
		{
			buffer.consume!T(size * T.sizeof);
		}
	}
}

unittest {
	import flod.buffer : NullBuffer;
	static assert(isPeekSource!(DefaultPullPeekAdapter!NullBuffer));
}

///
auto pullPeek(Pipeline, Buffer)(auto ref Pipeline pipeline, auto ref Buffer buffer)
	if (isPullPipeline!Pipeline)
{
	return pipeline.pipe!(DefaultPullPeekAdapter!Buffer)(buffer);
}

///
auto pullPeek(Pipeline)(auto ref Pipeline pipeline)
	if (isPullPipeline!Pipeline)
{
	import flod.buffer : movingBuffer;
	return pipeline.pullPeek(movingBuffer());
}

@implements!(PeekSink, PullSource, DefaultPeekPullAdapter)
private struct DefaultPeekPullAdapter(Source) {
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

///
auto peekPull(Pipeline)(auto ref Pipeline pipeline)
	if (isPeekPipeline!Pipeline)
{
	return pipeline.pipe!DefaultPeekPullAdapter();
}

@implements!(PullSink, PushSource, DefaultPullPushAdapter)
private struct DefaultPullPushAdapter(Source, Sink) {
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

@implements!(PushSink, YieldingPushSink)
private struct YieldingPushSink {
	const(void)[] pushed;
	private void stop() {
		pushed = (cast(const(void)*) null)[0xdead .. 0xdeaf];
		assert(pushed !is null);
	}

	private void more() { pushed = null; }

	size_t push(T)(const(T)[] buf)
	{
		import core.thread : Fiber;
		if (pushed !is null)
			return 0;
		pushed = cast(const(void)[]) buf;
		if (!__ctfe) // hack
			Fiber.yield();
		return buf.length;
	}
}

private struct DefaultPushPullAdapter(Source, Buffer) {
	enum _ok = implements!(PullSource, DefaultPushPullAdapter);

	import core.thread : Fiber;
	import flod.meta : NonCopyable, moveIfNonCopyable;

	Source source;
	Buffer buffer;
	Fiber fiber;

	this()(auto ref Source source, auto ref Buffer buffer) {
		this.source = moveIfNonCopyable(source);
		this.buffer = moveIfNonCopyable(buffer);
	}

	private T[] pullFromBuffer(T)(T[] dest)
	{
		import std.algorithm : min;
		auto src = buffer.peek!T();
		auto len = min(src.length, dest.length);
		if (len > 0) {
			dest[0 .. len] = src[0 .. len];
			buffer.consume!T(len);
			return dest[len .. $];
		}
		return dest;
	}

	private void runSource()
	{
		source.run();
	}

	// FIXME: unsafe, type information is lost between push and pull
	size_t pull(T)(T[] dest)
	{
		size_t requestedLength = dest.length;
		// first, give off whatever was left from this.pushed on previous pull();
		dest = pullFromBuffer(dest);
		if (dest.length == 0)
			return requestedLength;
		// if not satisfied yet, switch to source fiber till push() is called again
		// enough times to fill dest[]
		auto untyped = cast(void[]) dest;
		const(void)[] pushed;
		do {
			import std.algorithm : min;
			if (fiber is null) {
				// TODO: fiber is created here, and not in ctor, because the delegate context ptr
				// would point into the old location (possibly garbage) and it will
				// fail if "this" is moved or copied :/
				fiber = new Fiber(&this.runSource);
			}
			assert(fiber.state == Fiber.State.HOLD);
			source.sink.more();
			fiber.call();
			if (fiber.state == Fiber.State.TERM) {
				fiber = null;
				break;
			}
			pushed = source.sink.pushed;

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
		return requestedLength - untyped.length / T.sizeof;
	}

	~this()
	{
		if (fiber) {
			source.sink.stop;
			fiber.call();
			assert(fiber !is null);
			assert(fiber.state == Fiber.State.TERM);
			fiber = null;
		}
	}
}

///
auto pushPull(Pipeline, Buffer)(auto ref Pipeline pipeline, auto ref Buffer buffer)
{
	alias SP = typeof(pipeline.pipe!YieldingPushSink);
	alias PP = DefaultPushPullAdapter!(SP, Buffer);
	return PP(pipeline.pipe!YieldingPushSink, buffer);
}

///
auto pushPull(Pipeline)(auto ref Pipeline pipeline)
{
	import flod.buffer : movingBuffer;
	return pipeline.pushPull(movingBuffer());
}

unittest {
	static struct ArraySource(Sink) {
		mixin NonCopyable;
		int[] array;
		int counter = 1;

		private this(int[] arr) { array = arr; }
		Sink sink;

		this(Args...)(auto ref Sink sink, auto ref Args args)
		{
			this.sink = moveIfNonCopyable(sink);
			this(args);
		}

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
	static assert(!isCopyable!(ArraySource!YieldingPushSink));

	import std.range : iota, array;
	import flod.buffer;

	auto arr = iota(0, 1048576).array();
	int[] result;
	auto buffer = movingBuffer();
	auto pushsource = pipe!ArraySource(arr.dup);
	auto pushpipeline = pushsource.create(YieldingPushSink());
	auto pullsource = DefaultPushPullAdapter!(typeof(pushpipeline), typeof(buffer))(pushpipeline, buffer);
	auto n = 100;
	result.length = n;
	assert(pullsource.pull(result) == n);
	assert(result[0 .. n] == arr[0 .. n]);
	arr = arr[n .. $];

	n = 1337;
	result.length = n;
	assert(pullsource.pull(result) == n);
	assert(result[0 .. n] == arr[0 .. n]);
	arr = arr[n .. $];

	n = 4000000;
	result.length = n;
	assert(pullsource.pull(result) == arr.length);
	assert(result[0 .. arr.length] == arr[0 .. arr.length]);
}
