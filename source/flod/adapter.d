/** Adapters connecting stages with incompatible interfaces.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.adapter;

import flod.pipeline: isPullPipeline, isPeekPipeline;
import flod.traits;

struct DefaultPullPeekAdapter(Source) {
	import std.experimental.allocator : IAllocator, processAllocator, expandArray, makeArray;
	import std.stdio;
	import flod.traits : FixedPullType;
	import flod.meta : moveIfNonCopyable;

	private {
	Source source;
	IAllocator allocator;

	void[] buf;
	size_t readOffset;
	size_t peekOffset;
	}

	this()(auto ref Source source, IAllocator allocator = processAllocator) {
		this.source = moveIfNonCopyable(source);
		this.allocator = allocator;
	}

	static if (is(FixedPullType!Source)) {
		alias PullType = DefaultPullType!Source;
		const(T)[] peek(T = PullType)(size_t size)
			if (is(T == PullType))
		{
			return doPeek!PullType(size);
		}
		void consume(T = PullType)(size_t size)
			if (is(T == PullType))
		{
			doConsume!PullType(size);
		}
	} else {
		// pull can work with any element type
		const(T)[] peek(T = ubyte)(size_t size) { return doPeek!T(size); }
		void consume(T = ubyte)(size_t size) { doConsume!T(size); }
	}

	const(T)[] doPeek(T)(size_t size)
	{
		T[] tbuf = cast(T[]) buf;
		if (peekOffset + size > tbuf.length) {
			writefln("PullBuffer expected %d < available %d", peekOffset + size, tbuf.length);
			if (!tbuf)
				tbuf = allocator.makeArray!T(4096);
			else
				allocator.expandArray(tbuf, ((peekOffset + size + 4095) & ~size_t(4095)) - tbuf.length);
			buf = tbuf;
			writefln("PullBuffer grow %d", buf.length);
		}
		if (peekOffset + size > readOffset) {
			size_t r = source.pull(tbuf[readOffset .. $]);
			readOffset += r;
		}
		return tbuf[peekOffset .. $];
	}

	void doConsume(T)(size_t size)
	{
		T[] tbuf = cast(T[]) buf;
		peekOffset += size;
		if (peekOffset == tbuf.length) {
			writefln("PullBuffer reset %d", buf.length);
			peekOffset = 0;
			readOffset = 0;
		}
	}
}
static assert(isPeekSource!DefaultPullPeekAdapter);
static assert(isPullSink!DefaultPullPeekAdapter);

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
	int step()()
	{
		import core.stdc.stdlib : alloca;
		auto buf = (cast(T*) alloca(T.sizeof * chunkSize))[0 .. chunkSize];
		size_t inp = source.pull(buf[]);
		if (inp == 0)
			return 1;
		return sink.push(buf[0 .. inp]) < chunkSize;
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
	int step()()
	{
		auto buf = source.peek!T(chunkSize);
		if (buf.length == 0)
			return 1;
		size_t w = sink.push(buf[]);
		source.consume(w);
		return w < chunkSize;
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
	import flod.pipeline : toOutputRange, run;
	auto app = appender!(uint[]);
	iota(0, 1048576).array().peekPush().toOutputRange(app).run();
	assert(app.data == iota(0, 1048576).array());
}

version(FlodBloat):

class PushPull(Sink)
{
	import core.thread;

	private {
		Sink sink;
		ubyte[] buffer;
		size_t peekOffset;
		size_t readOffset;
		void[__traits(classInstanceSize, Fiber)] fiberBuf;

		final @property Fiber sinkFiber() pure
		{
			return cast(Fiber) cast(void*) fiberBuf;
		}
	}

	this() @trusted
	{
		import std.conv : emplace;
		emplace!Fiber(fiberBuf[], &this.fiberFunc);
	}

	~this() @trusted
	{
		sinkFiber.__dtor();
	}

	private final void fiberFunc()
	{
		stderr.writefln("this in fiberFunc %d %d", peekOffset, readOffset);
		sink.pull();
	}

	// push sink interface
	size_t push(const(ubyte[]) data)
	{
		stderr.writefln("push %d bytes", data.length);
		if (readOffset + data.length > buffer.length)
			buffer.length = readOffset + data.length;
		buffer[readOffset .. readOffset + data.length] = data[];
		readOffset += data.length;
		stderr.writefln("%d bytes available", readOffset);
		sinkFiber.call();
		return data.length;
	}

	// TODO: alloc+commit

	// pull source interface
	const(ubyte)[] peek(size_t size)
	{
		stderr.writefln("peek %d, po %d, ro %d, available %d", size, peekOffset, readOffset, readOffset - peekOffset);
		while (peekOffset + size > readOffset)
			Fiber.yield();
		return buffer[peekOffset .. $];
	}

	void consume(size_t size)
	{
		peekOffset += size;
		if (peekOffset == readOffset) {
			peekOffset = 0;
			readOffset = 0;
		}
	}

	void pull(ubyte[] outbuf)
	{
		import std.algorithm : min;
		auto inbuf = peek(outbuf.length);
		auto l = min(inbuf.length, outbuf.length);
		outbuf[0 .. l] = inbuf[0 .. l];
		consume(l);
	}
}

struct CircularPullBuffer(Source)
{
	import std.stdio;
	import std.exception : enforce;
	import core.sys.posix.stdlib : mkstemp;
	import core.sys.posix.unistd : close, unlink, ftruncate;
	import core.sys.posix.sys.mman : mmap, munmap, MAP_ANON, MAP_PRIVATE, MAP_FIXED, MAP_SHARED, MAP_FAILED, PROT_WRITE, PROT_READ;

	import flod.traits : FixedPullType, hasGenericPull;

	Source source;
	private {
		void* buffer;
		size_t length;
		size_t peekOffset;
		size_t readOffset;
	}

	this(Source source)
	{
		enum order = 12;
		length = size_t(1) << order;
		int fd = createFile();
		scope(exit) close(fd);
		allocate(fd);
		this.source = source;
	}

	void dispose()
	{
		if (buffer) {
			munmap(buffer, length << 1);
			buffer = null;
		}
	}

	private int createFile()
	{
		static immutable path = "/dev/shm/flod-CompactPullBuffer-XXXXXX";
		char[path.length + 1] mutablePath = path.ptr[0 .. path.length + 1];
		int fd = mkstemp(mutablePath.ptr);
		enforce(fd >= 0, "Failed to create shm file");
		scope(failure) enforce(close(fd) == 0, "Failed to close shm file");
		enforce(unlink(mutablePath.ptr) == 0, "Failed to unlink shm file " ~ mutablePath);
		enforce(ftruncate(fd, length) == 0, "Failed to set shm file size");
		return fd;
	}

	private void allocate(int fd)
	{
		void* addr = mmap(null, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
		enforce(addr != MAP_FAILED, "Failed to mmap 1st part");
		scope(failure) munmap(addr, length);
		buffer = addr;
		addr = mmap(buffer + length, length, PROT_READ | PROT_WRITE, MAP_FIXED | MAP_SHARED, fd, 0);
		if (addr != buffer + length) {
			assert(addr == MAP_FAILED);
			addr = mmap(buffer - length, length, PROT_READ | PROT_WRITE, MAP_FIXED | MAP_SHARED, fd, 0);
			// TODO: if failed, try mmapping anonymous buf of 2 * length, then mmap the two copies inside it
			enforce(addr == buffer - length, "Failed to mmap 2nd part");
			buffer = addr;
		}
		import std.stdio;
		writefln("%016x,%08x", buffer, length * 2);
	}

	static if (is(FixedPullType!Source)) {
		alias PullType = FixedPullType!Source;
		const(PullType)[] peek()(size_t size) { return doPeek!PullType(size); }
		void consume()(size_t size) { doConsume!PullType(size); }
	}
	static if (hasGenericPull!Source) {
		const(T)[] peek(T)(size_t size) { return doPeek!T(size); }
		void consume(T)(size_t size) { doConsume!T(size); }
	}

	private const(T)[] doPeek(T)(size_t size)
	{
		enforce(size <= length, "Growing buffer not implemented");
		auto buf = cast(T*) buffer;
		assert(readOffset <= peekOffset + 4096);
		while (peekOffset + size > readOffset)
			readOffset += source.pull(buf[readOffset .. readOffset]); //peekOffset + 4096]);
		assert(buf[0 .. length] == buf[length .. 2 *length]);
		stderr.writefln("%08x,%08x", peekOffset, readOffset - peekOffset);
		return buf[peekOffset .. readOffset];
	}

	private void doConsume(T)(size_t size)
	{
		assert(peekOffset + size <= readOffset);
		peekOffset += size;
		if (peekOffset >= length) {
			peekOffset -= length;
			readOffset -= length;
		}
		writefln("%08x %08x", peekOffset, readOffset);
	}
}
