/** Adapters connecting stream components with incompatible interfaces.
 */
module flod.adapter;

// Convert buffered push source to unbuffered push source
struct BufferedToUnbufferedPushSource(Sink) {
	Sink sink;

	void open()
	{
		sink.open();
	}

	size_t push(const(ubyte)[] b)
	{
		auto buf = sink.alloc(b.length);
		buf[] = b[0 .. buf.length];
		sink.commit(buf.length);
		return buf.length;
	}
}

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

// Drive a stream which doesn't have any driving components
struct PullPush(Source, Sink) {
	Source source = void;
	Sink sink = void;

	bool step()()
	{
		ubyte[4096] buf;
		size_t n = source.pull(buf[]);
		if (n == 0)
			return false;
		if (sink.push(buf[0 .. n]) < n)
			return false;
		return true;
	}
}

struct PullBuffer(Source) {
	import std.experimental.allocator : IAllocator, expandArray, makeArray;
	import flod.traits : DefaultPullType, hasGenericPull;
	import std.stdio;

	Source source;
	IAllocator allocator;

	void[] buf;
	size_t readOffset;
	size_t peekOffset;

	static if (is(DefaultPullType!Source)) {
		alias PullType = DefaultPullType!Source;
		const(PullType)[] peek()(size_t size) { return doPeek!PullType(size); }
		void consume()(size_t size) { doConsume!PullType(size); }
	}
	static if (hasGenericPull!Source) {
		const(T)[] peek(T)(size_t size) { return doPeek!T(size); }
		void consume(T)(size_t size) { doConsume!T(size); }
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

struct PushBuffer(Sink, T)
{
	pragma(msg, "PushBuffer: ", typeof(Sink.init).stringof, ", ", T.stringof);
	Sink sink;

	T[] buf;
	size_t allocOffset;

	T[] alloc(size_t size)
	{
		if (allocOffset + size > buf.length) {
			buf.length = allocOffset + size;
			writefln("PushBuffer grow %d", buf.length);
		}
		return buf[allocOffset .. $];
	}

	void commit(size_t size)
	{
		sink.push(buf[allocOffset .. size]);
		allocOffset += size;
		if (allocOffset == buf.length)
			allocOffset = 0;
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

auto compactPullBuffer(Source, T = SourceDataType!Source)(Source source)
{
	return CompactPullBuffer!(Source, T)(source);
}

// TODO: detect type of sink and itsSink
auto pushToPullBuffer(Sink, ItsSink)(Sink sink, ItsSink itsSink)
{
	return new PushToPullBuffer!(Sink, ItsSink)(sink, itsSink);
}
