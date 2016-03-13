/** Adapters connecting stages with incompatible interfaces.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.adapter;

import flod.pipeline;
import flod.traits;
import flod.meta;

private template DefaultPullPeekAdapter(Buffer, E) {
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

private template DefaultPeekPullAdapter(E) {
	@peekSink!E @pullSource!E
	struct DefaultPeekPullAdapter(Source) {
		Source source;

		size_t pull(E[] buf)
		{
			import std.algorithm : min;
			auto inbuf = source.peek(buf.length);
			auto l = min(buf.length, inbuf.length);
			buf[0 .. l] = inbuf[0 .. l];
			source.consume(l);
			return l;
		}
	}
}

///
auto peekPull(Pipeline)(auto ref Pipeline pipeline)
	if (isPeekPipeline!Pipeline)
{
	return pipeline.pipe!(DefaultPeekPullAdapter!(Pipeline.ElementType));
}

private template DefaultPullPushAdapter(E) {
	@pullSink!E @pushSource!E
	struct DefaultPullPushAdapter(Source, Sink) {
		Source source;
		Sink sink;
		size_t chunkSize;

		this(size_t chunkSize)
		{
			this.chunkSize = chunkSize;
		}

		void run()()
		{
			import core.stdc.stdlib : alloca;
			auto buf = (cast(E*) alloca(E.sizeof * chunkSize))[0 .. chunkSize];
			for (;;) {
				size_t inp = source.pull(buf[]);
				if (inp == 0)
					break;
				if (sink.push(buf[0 .. inp]) < chunkSize)
					break;
			}
		}
	}
}

///
auto pullPush(Pipeline)(auto ref Pipeline pipeline, size_t chunkSize = 4096)
	if (isPullPipeline!Pipeline)
{
	return pipeline.pipe!(DefaultPullPushAdapter!(Pipeline.ElementType))(chunkSize);
}

private template DefaultPeekPushAdapter(E) {
	@peekSink!E @pushSource!E
	struct DefaultPeekPushAdapter(Source, Sink) {
		Source source;
		Sink sink;
		size_t minSliceSize;

		this(size_t minSliceSize)
		{
			this.minSliceSize = minSliceSize;
		}

		void run()()
		{
			for (;;) {
				auto buf = source.peek(minSliceSize);
				if (buf.length == 0)
					break;
				size_t w = sink.push(buf[]);
				if (w < minSliceSize)
					break;
				assert(w <= buf.length);
				source.consume(w);
			}
		}
	}
}

///
auto peekPush(Pipeline)(auto ref Pipeline pipeline, size_t minSliceSize = size_t.sizeof)
	if (isPeekPipeline!Pipeline)
{
	return pipeline.pipe!(DefaultPeekPushAdapter!(Pipeline.ElementType))(minSliceSize);
}

private template DefaultPushPullAdapter(Buffer, E) {
	@pushSink!E @pullSource!E
	struct DefaultPushPullAdapter(alias Scheduler) {
		import std.algorithm : min;

		mixin Scheduler;

		Buffer buffer;
		const(E)[] pushed;

		this()(auto ref Buffer buffer) {
			this.buffer = moveIfNonCopyable(buffer);
		}

		size_t push(const(E)[] buf)
		{
			if (pushed.length > 0)
				return 0;
			pushed = buf;
			yield();
			return buf.length;
		}

		private E[] pullFromBuffer(E[] dest)
		{
			auto src = buffer.peek!E();
			auto len = min(src.length, dest.length);
			if (len > 0) {
				dest[0 .. len] = src[0 .. len];
				buffer.consume!E(len);
				return dest[len .. $];
			}
			return dest;
		}

		size_t pull(E[] dest)
		{
			size_t requestedLength = dest.length;
			// first, give off whatever was left from this.pushed on previous pull();
			dest = pullFromBuffer(dest);
			if (dest.length == 0)
				return requestedLength;
			// if not satisfied yet, switch to source fiber till push() is called again
			// enough times to fill dest[]
			do {
				if (yield())
					break;
				// pushed is the slice of the original buffer passed to push() by the source.
				auto len = min(pushed.length, dest.length);
				assert(len > 0);
				dest[0 .. len] = pushed[0 .. len];
				dest = dest[len .. $];
				pushed = pushed[len .. $];
			} while (dest.length > 0);

			// whatever's left in pushed, keep it in buffer for the next time pull() is called
			while (pushed.length > 0) {
				auto b = buffer.alloc!E(pushed.length);
				if (b.length == 0) {
					import core.exception : OutOfMemoryError;
					throw new OutOfMemoryError();
				}
				auto len = (b.length, pushed.length);
				b[0 .. len] = pushed[0 .. len];
				buffer.commit!E(len);
				pushed = pushed[len .. $];
			}
			return requestedLength - dest.length;
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

private template ImplementPeekConsume(E) {
	const(E)[] peek(size_t n)
	{
		const(E)[] result;
		for (;;) {
			result = buffer.peek!E;
			if (result.length >= n)
				break;
			if (yield())
				break;
		}
		return result;
	}

	void consume(size_t n)
	{
		buffer.consume!E(n);
	}
}

private template ImplementAllocCommit(E) {
	bool alloc(ref E[] buf, size_t n)
	{
		buf = buffer.alloc!E(n);
		if (!buf || buf.length < n)
			return false;
		return true;
	}

	size_t commit(size_t n)
	{
		buffer.commit!E(n);
		if (yield())
			return 0;
		return n;
	}
}

private template DefaultPushPeekAdapter(Buffer, E) {
	@pushSink!E @peekSource!E
	struct DefaultPushPeekAdapter(alias Scheduler) {
		import std.algorithm : min;
		mixin Scheduler;
		Buffer buffer;

		this()(auto ref Buffer buffer)
		{
			this.buffer = buffer;
		}

		size_t push(const(E)[] buf)
		{
			size_t n = buf.length;
			auto ob = buffer.alloc!E(n);
			if (ob.length < n) {
				import core.exception : OutOfMemoryError;
				throw new OutOfMemoryError();
			}
			ob[0 .. n] = buf[0 .. n];
			buffer.commit!E(n);
			if (yield())
				return 0;
			return n;
		}

		mixin ImplementPeekConsume!E;
	}
}


///
auto pushPeek(Pipeline, Buffer)(auto ref Pipeline pipeline, auto ref Buffer buffer)
{
	alias E = Pipeline.ElementType;
	alias PP = DefaultPushPeekAdapter!(Buffer, E);
	return pipeline.pipe!PP(buffer);
}

///
auto pushPeek(Pipeline)(auto ref Pipeline pipeline)
{
	import flod.buffer : movingBuffer;
	return pipeline.pushPeek(movingBuffer());
}

private template DefaultAllocPeekAdapter(Buffer, E) {
	@allocSink!E @peekSource!E
	struct DefaultAllocPeekAdapter(alias Scheduler) {
		import std.algorithm : min;
		mixin Scheduler;
		Buffer buffer;

		this()(auto ref Buffer buffer)
		{
			this.buffer = buffer;
		}

		mixin ImplementAllocCommit!E;
		mixin ImplementPeekConsume!E;
	}
}

///
auto allocPeek(Pipeline, Buffer)(auto ref Pipeline pipeline, auto ref Buffer buffer)
	if (isAllocPipeline!Pipeline)
{
	alias E = Pipeline.ElementType;
	alias PP = DefaultAllocPeekAdapter!(Buffer, E);
	return pipeline.pipe!PP(buffer);
}

///
auto allocPeek(Pipeline)(auto ref Pipeline pipeline)
	if (isAllocPipeline!Pipeline)
{
	import flod.buffer : movingBuffer;
	return pipeline.allocPeek(movingBuffer());
}

private template DefaultAllocPullAdapter(Buffer, E) {
	@allocSink!E @pullSource!E
	struct DefaultAllocPullAdapter(alias Scheduler) {
		import std.algorithm : min;
		mixin Scheduler;
		Buffer buffer;

		this()(auto ref Buffer buffer)
		{
			this.buffer = buffer;
		}

		mixin ImplementAllocCommit!E;

		size_t pull(E[] buf)
		{
			const(E)[] ib;
			for (;;) {
				ib = buffer.peek!E;
				if (ib.length >= buf.length)
					break;
				if (yield())
					break;
			}
			auto len = min(ib.length, buf.length);
			buf[0 .. len] = ib[0 .. len];
			buffer.consume!E(len);
			return len;
		}
	}
}

///
auto allocPull(Pipeline, Buffer)(auto ref Pipeline pipeline, auto ref Buffer buffer)
	if (isAllocPipeline!Pipeline)
{
	alias E = Pipeline.ElementType;
	alias PP = DefaultAllocPullAdapter!(Buffer, E);
	return pipeline.pipe!PP(buffer);
}

///
auto allocPull(Pipeline)(auto ref Pipeline pipeline)
	if (isAllocPipeline!Pipeline)
{
	import flod.buffer : movingBuffer;
	return pipeline.allocPull(movingBuffer());
}

private template DefaultAllocPushAdapter(Buffer, E) {
	@allocSink!E @pushSource!E
	struct DefaultAllocPushAdapter(Sink) {
		Sink sink;
		Buffer buffer;

		this()(auto ref Buffer buffer)
		{
			this.buffer = buffer;
		}

		bool alloc(ref E[] buf, size_t n)
		{
			buf = buffer.alloc!E(n);
			if (!buf || buf.length < n)
				return false;
			return true;
		}

		size_t commit(size_t n)
		{
			buffer.commit!E(n);
			sink.push(buffer.peek!E[0 .. n]);
			buffer.consume!E(n);
			return n;
		}
	}
}

///
auto allocPush(Pipeline, Buffer)(auto ref Pipeline pipeline, auto ref Buffer buffer)
	if (isAllocPipeline!Pipeline)
{
	alias E = Pipeline.ElementType;
	alias PP = DefaultAllocPushAdapter!(Buffer, E);
	return pipeline.pipe!PP(buffer);
}

///
auto allocPush(Pipeline)(auto ref Pipeline pipeline)
	if (isAllocPipeline!Pipeline)
{
	import flod.buffer : movingBuffer;
	return pipeline.allocPush(movingBuffer());
}
