/** Adapters connecting stages with incompatible interfaces.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.adapter;

import flod.pipeline : pipe, isSchema, FiberSwitch;
import flod.traits : filter, Method;

private template DefaultPullPeekAdapter(Buffer) {
	@filter(Method.pull, Method.peek)
	struct DefaultPullPeekAdapter(alias Context, A...) {
		mixin Context!A;
		private Buffer buffer;

		static assert(is(InputElementType == OutputElementType));
		private alias E = InputElementType;

		this()(auto ref Buffer buffer)
		{
			this.buffer = buffer;
		}

		const(E)[] peek(size_t size)
		{
			auto ready = buffer.peek!E();
			if (ready.length >= size)
				return ready;
			auto chunk = buffer.alloc!E(size - ready.length);
			assert(chunk.length >= size - ready.length);
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
auto pullPeek(S, Buffer)(auto ref S schema, auto ref Buffer buffer)
	if (isSchema!S)
{
	return schema.pipe!(DefaultPullPeekAdapter!Buffer)(buffer);
}

///
auto pullPeek(S)(auto ref S schema)
	if (isSchema!S)
{
	import flod.buffer : movingBuffer;
	return schema.pullPeek(movingBuffer());
}

@filter(Method.peek, Method.pull)
struct DefaultPeekPullAdapter(alias Context, A...) {
	mixin Context!A;

	static assert(is(InputElementType == OutputElementType));
	private alias E = InputElementType;

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

///
auto peekPull(S)(auto ref S schema)
	if (isSchema!S)
{
	return schema.pipe!DefaultPeekPullAdapter;
}

@filter(Method.pull, Method.push)
struct DefaultPullPushAdapter(alias Context, A...) {
	mixin Context!A;
	import std.experimental.allocator.mallocator : Mallocator;
	import std.exception : enforce;
	static assert(is(InputElementType == OutputElementType));
	private alias E = InputElementType;

	private E[] buf;

	this(size_t chunkSize)
	{
		buf = cast(E[]) enforce(Mallocator.instance.allocate(chunkSize * E.sizeof));
	}

	~this()
	{
		Mallocator.instance.deallocate(buf);
	}

	void run()()
	{
		for (;;) {
			size_t inp = source.pull(buf[]);
			if (inp == 0)
				break;
			if (sink.push(buf[0 .. inp]) < inp)
				break;
		}
	}
}

///
auto pullPush(S)(auto ref S schema, size_t chunkSize = 4096)
	if (isSchema!S)
{
	return schema.pipe!DefaultPullPushAdapter(chunkSize);
}

@filter(Method.pull, Method.alloc)
struct DefaultPullAllocAdapter(alias Context, A...) {
	mixin Context!A;
	private size_t chunkSize;

	static assert(is(InputElementType == OutputElementType));
	private alias E = InputElementType;

	this(size_t chunkSize)
	{
		this.chunkSize = chunkSize;
	}

	void run()()
	{
		E[] buf;
		for (;;) {
			if (!sink.alloc(buf, chunkSize))
				assert(0);
			size_t inp = source.pull(buf[]);
			if (inp == 0)
				break;
			if (sink.commit(inp) < chunkSize)
				break;
		}
	}
}

///
auto pullAlloc(S)(auto ref S schema, size_t chunkSize = 4096)
	if (isSchema!S)
{
	return schema.pipe!DefaultPullAllocAdapter(chunkSize);
}

@filter(Method.peek, Method.push)
struct DefaultPeekPushAdapter(alias Context, A...) {
	mixin Context!A;
	private size_t minSliceSize;

	static assert(is(InputElementType == OutputElementType));
	private alias E = InputElementType;

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

///
auto peekPush(S)(auto ref S schema, size_t minSliceSize = size_t.sizeof)
	if (isSchema!S)
{
	return schema.pipe!DefaultPeekPushAdapter(minSliceSize);
}

@filter(Method.peek, Method.alloc)
struct DefaultPeekAllocAdapter(alias Context, A...) {
	mixin Context!A;

	private {
		static assert(is(InputElementType == OutputElementType));
		alias E = InputElementType;

		size_t minSliceSize;
		size_t maxSliceSize;
	}

	this(size_t minSliceSize, size_t maxSliceSize)
	{
		this.minSliceSize = minSliceSize;
		this.maxSliceSize = maxSliceSize;
	}

	void run()()
	{
		E[] ob;
		for (;;) {
			import std.algorithm : min;
			auto ib = source.peek(minSliceSize);
			if (ib.length == 0)
				break;
			auto len = min(ib.length, maxSliceSize);
			if (!sink.alloc(ob, len))
				assert(0);
			ob[0 .. len] = ib[0 .. len];
			size_t w = sink.commit(len);
			source.consume(w);
			if (w < minSliceSize)
				break;
		}
	}
}

///
auto peekAlloc(S)(auto ref S schema, size_t minSliceSize = size_t.sizeof, size_t maxSliceSize = 4096)
	if (isSchema!S)
{
	return schema.pipe!DefaultPeekAllocAdapter(minSliceSize, maxSliceSize);
}

@filter(Method.push, Method.alloc)
struct DefaultPushAllocAdapter(alias Context, A...) {
	mixin Context!A;

	static assert(is(InputElementType == OutputElementType));
	private alias E = InputElementType;

	size_t push(const(E)[] buf)
	{
		E[] ob;
		if (!sink.alloc(ob, buf.length))
			assert(0);
		ob[0 .. buf.length] = buf[];
		return sink.commit(buf.length);
	}
}

///
auto pushAlloc(S)(auto ref S schema)
	if (isSchema!S)
{
	return schema.pipe!DefaultPushAllocAdapter();
}

private template DefaultPushPullAdapter(Buffer) {
	@filter(Method.push, Method.pull)
	struct DefaultPushPullAdapter(alias Context, A...) {
		import std.algorithm : min;
		mixin Context!A;

		private {
			static assert(is(InputElementType == OutputElementType));
			alias E = InputElementType;

			Buffer buffer;
			const(E)[] pushed;
		}

		this()(auto ref Buffer buffer)
		{
			this.buffer = buffer;
		}

		mixin FiberSwitch;

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
				if (!pushed.length && yield())
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
				assert(b.length >= pushed.length);
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
auto pushPull(S, Buffer)(auto ref S schema, auto ref Buffer buffer)
{
	return schema.pipe!(DefaultPushPullAdapter!Buffer)(buffer);
}

///
auto pushPull(S)(auto ref S schema)
{
	import flod.buffer : movingBuffer;
	return schema.pushPull(movingBuffer());
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

private template DefaultPushPeekAdapter(Buffer) {
	@filter(Method.push, Method.peek)
	struct DefaultPushPeekAdapter(alias Context, A...) {
		import std.algorithm : min;
		mixin Context!A;

		private {
			static assert(is(InputElementType == OutputElementType));
			alias E = InputElementType;

			Buffer buffer;
		}

		this()(auto ref Buffer buffer)
		{
			this.buffer = buffer;
		}

		mixin FiberSwitch;

		size_t push(const(E)[] buf)
		{
			size_t n = buf.length;
			auto ob = buffer.alloc!E(n);
			assert(ob.length >= n);
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
auto pushPeek(S, Buffer)(auto ref S schema, auto ref Buffer buffer)
{
	return schema.pipe!(DefaultPushPeekAdapter!Buffer)(buffer);
}

///
auto pushPeek(S)(auto ref S schema)
{
	import flod.buffer : movingBuffer;
	return schema.pushPeek(movingBuffer());
}

private template DefaultAllocPeekAdapter(Buffer) {
	@filter(Method.alloc, Method.peek)
	struct DefaultAllocPeekAdapter(alias Context, A...) {
		import std.algorithm : min;
		mixin Context!A;

		private {
			static assert(is(InputElementType == OutputElementType));
			alias E = InputElementType;

			Buffer buffer;
		}

		this()(auto ref Buffer buffer)
		{
			this.buffer = buffer;
		}

		mixin FiberSwitch;
		mixin ImplementAllocCommit!E;
		mixin ImplementPeekConsume!E;
	}
}

///
auto allocPeek(S, Buffer)(auto ref S schema, auto ref Buffer buffer)
	if (isSchema!S)
{
	return schema.pipe!(DefaultAllocPeekAdapter!Buffer)(buffer);
}

///
auto allocPeek(S)(auto ref S schema)
	if (isSchema!S)
{
	import flod.buffer : movingBuffer;
	return schema.allocPeek(movingBuffer());
}

private template DefaultAllocPullAdapter(Buffer) {
	@filter(Method.alloc, Method.pull)
	struct DefaultAllocPullAdapter(alias Context, A...) {
		import std.algorithm : min;
		mixin Context!A;

		private {
			static assert(is(InputElementType == OutputElementType));
			alias E = InputElementType;

			Buffer buffer;
		}

		this()(auto ref Buffer buffer)
		{
			this.buffer = buffer;
		}

		mixin FiberSwitch;
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
auto allocPull(S, Buffer)(auto ref S schema, auto ref Buffer buffer)
	if (isSchema!S)
{
	return schema.pipe!(DefaultAllocPullAdapter!Buffer)(buffer);
}

///
auto allocPull(S)(auto ref S schema)
	if (isSchema!S)
{
	import flod.buffer : movingBuffer;
	return schema.allocPull(movingBuffer());
}

private template DefaultAllocPushAdapter(Buffer) {
	@filter(Method.alloc, Method.push)
	struct DefaultAllocPushAdapter(alias Context, A...) {
		mixin Context!A;
		private {
			static assert(is(InputElementType == OutputElementType));
			alias E = InputElementType;

			Buffer buffer;
		}

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
auto allocPush(S, Buffer)(auto ref S schema, auto ref Buffer buffer)
	if (isSchema!S)
{
	return schema.pipe!(DefaultAllocPushAdapter!Buffer)(buffer);
}

///
auto allocPush(S)(auto ref S schema)
	if (isSchema!S)
{
	import flod.buffer : movingBuffer;
	return schema.allocPush(movingBuffer());
}

@filter(Method.alloc, Method.peek)
private struct Parallel(alias Context, A...) {
	mixin Context!A;
private:
	import core.atomic : atomicLoad, atomicStore;
	import core.thread : Thread, Mutex;
	import core.sync.condition : Condition;
	import core.sync.semaphore : Semaphore;
	import flod.buffer : MmappedBuffer, mmappedBuffer;
	import flod.pipeline : DriveMode;

	alias E = InputElementType;
	static assert(is(E == OutputElementType));

	MmappedBuffer buffer;
	Mutex mutex;
	Semaphore peekSem;
	Semaphore allocSem;
	Thread secondary;

	// cache to avoid synchronized calls to buffer
	size_t cacheSize;
	const(E)[] cache;

	// FIXME: This kludge is used to know when the input stream ends.
	// Also, there's no way the sink can reliably tell the source it doesn't want more data.
	ulong writeCount;
	ulong readCount;
	shared(ulong) stopAt;
	size_t halfBufferSize;

	void finalize(bool secondary)()
	{
		static if (driveMode == (secondary ? DriveMode.sink : DriveMode.source)) {
			atomicStore(stopAt, writeCount);
			peekSem.notify();
		} else {
			allocSem.notify();
		}
	}

public:
	this(size_t buffer_size)
	{
		import std.typecons : No;
		halfBufferSize = buffer_size / 2;
		buffer = mmappedBuffer(buffer_size, No.grow);
		mutex = new Mutex;
		peekSem = new Semaphore;
		allocSem = new Semaphore;
		atomicStore(stopAt, ulong.max);
	}

	void spawn()()
	{
		assert(!secondary);
		secondary = new Thread(
			{
				nextDriver.run();
				finalize!true();
			}).start();
	}

	void stop()()
	{
		finalize!false();
		secondary.join();
	}

	bool alloc()(ref E[] buf, size_t n)
	{
		for (;;) {
			synchronized (mutex) {
				buf = buffer.alloc!E(n);
			}
			if (buf.length >= n)
				return true;
			allocSem.wait();
		}
	}

	size_t commit()(size_t n)
	{
		synchronized (mutex) {
			buffer.commit!E(n);
		}
		writeCount += n;
		peekSem.notify();
		return n;
	}

	const(E)[] peek()(size_t n)
	{
		if (n < cache.length)
			return cache;
		for (;;) {
			const(E)[] ib;
			synchronized (mutex) {
				if (cacheSize) {
					size_t read = cacheSize - cache.length;
					buffer.consume!E(read);
					readCount += read;
					cacheSize = 0;
					allocSem.notify();
				}
				ib = buffer.peek!E();
			}
			if (ib.length >= n || readCount + ib.length >= atomicLoad(stopAt)) {
				if (ib.length > halfBufferSize)
					ib = ib[0 .. halfBufferSize];
				cache = ib;
				cacheSize = ib.length;
				return cache;
			}
			peekSem.wait();
		}
	}

	void consume()(size_t n)
	{
		assert(n <= cache.length);
		cache = cache[n .. $];
	}
}

auto parallel(S)(S schema, size_t buffer_size = 65536)
{
	return schema.pipe!Parallel(buffer_size);
}

unittest {
	import std.algorithm : equal;
	import flod.range : byLine;
	"foo\nbar\nbaz\n".parallel.byLine.equal([ "foo", "bar", "baz" ]);
	"foo\nbar\nbaz\n".pushPeek.parallel.peekPush.byLine.equal([ "foo", "bar", "baz" ]);
}

unittest {
	import std.algorithm : equal, stdcopy = copy;
	import std.range : appender, iota, cycle, take;
	import flod.range : copy, pass;
	import flod.pipeline : pipe;

	foreach (size; [ 1, 1023, 1024, 4096, 4097, 128 * 1024 - 15, 128 * 1024 ].cycle.take(70)) {
		auto app = appender!(int[])();
		iota(size).parallel.copy(app);
		assert(iota(size).equal(app.data));
		app.clear();
		iota(size).parallel.parallel.peekPush.parallel.pushPeek.parallel.parallel.copy(app);
		assert(iota(size).equal(app.data));
		app.clear();
		{
			auto r = pass!int.parallel.copy(app);
			iota(size).stdcopy(r);
		}
		assert(iota(size).equal(app.data));
		app.clear();
		{
			auto r = pass!int.parallel.parallel.peekPush.parallel.pushPeek.parallel.parallel.copy(app);
			iota(size).stdcopy(r);
		}
		assert(iota(size).equal(app.data));
		app.clear();
		// ideally, size should be accessible inside the lambda template arg.
		// since it doesn't compile this way, we're passing it via a static var, which
		// also must be shared, because the source will actually run in a background thread.
		static shared(int) s_size;
		s_size = size;
		pipe!(int, (orange) {
				iota(s_size).stdcopy(orange);
			}).parallel.copy(app);
		assert(iota(size).equal(app.data));
	}
}
