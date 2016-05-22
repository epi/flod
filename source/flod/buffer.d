/** Various buffer implementations.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.buffer;

import std.experimental.allocator.mallocator : Mallocator;
import std.typecons : Flag, Yes;

private size_t alignUp(size_t n, size_t al)
{
	return (n + al - 1) & ~(al - 1);
}
static assert(13.alignUp(4) == 16);
static assert(31337.alignUp(4096) == 32768);

private size_t goodSize(Allocator)(ref Allocator, size_t n)
{
	static if (is(typeof(allocator.goodAllocSize(n)) : size_t))
		return allocator.goodAllocSize(n);
	else
		return n.alignUp(size_t.sizeof);
}

/**
A trivial buffer that uses GC to allocate and extend the underlying storage.
*/
struct GCBuffer {
private:
    void[] storage;
	size_t allocOffset;
public:
	///
	const(void)[] peek() const pure nothrow @safe
	{
		return storage[0 .. allocOffset];
	}

	///
	void consume(size_t n) pure nothrow @safe
	{
		storage = storage[n .. $];
		allocOffset -= n;
	}

	///
	void[] alloc(size_t n) pure nothrow @safe
	{
		if (storage.length < allocOffset + n)
			storage.length = allocOffset + n;
		return storage[allocOffset .. $];
	}

	///
	void commit(size_t n) pure nothrow @safe
	{
		allocOffset += n;
	}
}

/**
Wrapper that provides a typed view over `Buffer`.

Params:
E = Element type.
Buffer = Underlying buffer implementation.
*/
struct TypedBuffer(E, Buffer) {
private:
	Buffer buffer;

public:
	///
	const(E)[] peek()()
	{
		auto s = buffer.peek();
		return (cast(const(E)*) s.ptr)[0 .. s.length / E.sizeof];
	}

	///
	void consume()(size_t n)
	{
		buffer.consume(n * E.sizeof);
	}

	///
	E[] alloc()(size_t n)
	{
		auto s = buffer.alloc(n * E.sizeof);
		return (cast(E*) s.ptr)[0 .. s.length / E.sizeof];
	}

	///
	void commit()(size_t n)
	{
		buffer.commit(n * E.sizeof);
	}
}

/// ditto
auto typedBuffer(E, Buffer)(Buffer buffer)
{
	import std.algorithm : move;
	return TypedBuffer!(E, Buffer)(move(buffer));
}

version(unittest) {
	private void testBuffer2(Buffer)(ref Buffer b)
	{
		import std.range : iota, array, put;
		import std.algorithm : copy;
		static assert(is(typeof(b)));
		assert(b.peek().length == 0);
		b.consume(0);
		auto chunk = b.alloc(1);
		assert(chunk.length >= 1);
		assert(b.peek().length == 0);
		chunk = b.alloc(31337);
		assert(chunk.length >= 31337);
		auto arr = iota!uint(0, chunk.length).array();
		iota!uint(0, cast(uint) chunk.length).copy(chunk[0 .. $]);
		b.commit(1312);
		assert(b.peek()[] == arr[0 .. 1312]);
		b.commit(chunk.length - 1312);
		assert(b.peek()[] == arr[]);
		b.consume(0);
		assert(b.peek()[] == arr[]);
		b.consume(15);
		assert(b.peek()[] == arr[15 .. $]);
		// TODO: put more stress on the buffer
	}
}

unittest {
	auto tb = typedBuffer!int(GCBuffer());
	testBuffer2(tb);
}

/**
A buffer that relies on moving chunks of data in memory
to ensure that contiguous slices of any requested size can always be provided.
Params:
 Allocator = _Allocator used for internal storage allocation.
*/
struct MovingBuffer(Allocator) {
private:
	import flod.meta : NonCopyable;
	mixin NonCopyable;

	void[] storage;
	size_t peekOffset;
	size_t allocOffset;
	Allocator allocator;

	invariant {
		assert(peekOffset <= allocOffset);
		assert(allocOffset <= storage.length);
	}

public:
	this(Allocator allocator, size_t initialSize = 0) @trusted
	{
		this.allocator = allocator;
		if (initialSize > 0)
			storage = allocator.allocate(allocator.goodSize(initialSize));
	}

	~this() @trusted
	{
		allocator.deallocate(storage);
		storage = null;
	}

	///
	void[] alloc()(size_t n) @trusted
	{
		import std.experimental.allocator : reallocate;
		import core.stdc.string : memmove;
		if (storage.length >= allocOffset + n)
			return storage[allocOffset .. $];
		memmove(storage.ptr, storage.ptr + peekOffset, allocOffset - peekOffset);
		allocOffset -= peekOffset;
		peekOffset = 0;
		size_t newSize = goodSize(allocator, allocOffset + n);
		if (storage.length < newSize)
			allocator.reallocate(storage, newSize);
		assert(storage.length >= allocOffset + n);
		return storage[allocOffset .. $];
	}

	///
	void commit()(size_t n) @trusted
	{
		allocOffset += n;
	}

	///
	const(void)[] peek()() const @trusted
	{
		return storage[peekOffset .. allocOffset];
	}

	///
	void consume()(size_t n) @trusted
	{
		peekOffset += n;
		assert(peekOffset <= allocOffset);
		if (peekOffset == storage.length) {
			peekOffset = 0;
			allocOffset = 0;
		}
	}
}

/// ditto
auto movingBuffer(Allocator)(Allocator allocator = Mallocator.instance)
{
	return MovingBuffer!Allocator(allocator);
}

version(unittest) {
	private void testBuffer(Buffer)(auto ref Buffer b)
	{
		import std.range : iota, array, put;
		import std.algorithm : copy;
		static assert(is(typeof(b)));
		assert(b.peek!uint().length == 0);
		b.consume!uint(0);
		auto chunk = b.alloc!uint(1);
		assert(chunk.length >= 1);
		assert(b.peek!uint().length == 0);
		chunk = b.alloc!uint(31337);
		assert(chunk.length >= 31337);
		auto arr = iota!uint(0, chunk.length).array();
		iota!uint(0, cast(uint) chunk.length).copy(chunk[0 .. $]);
		b.commit!uint(1312);
		assert(b.peek!uint()[] == arr[0 .. 1312]);
		b.commit!uint(chunk.length - 1312);
		assert(b.peek!uint()[] == arr[]);
		b.consume!uint(0);
		assert(b.peek!uint()[] == arr[]);
		b.consume!uint(15);
		assert(b.peek!uint()[] == arr[15 .. $]);
		// TODO: put more stress on the buffer
	}
}

unittest {
	auto b = typedBuffer!int(movingBuffer());
	testBuffer2(b);
	// consume everything and check if b will reset its pointers.
	b.consume(b.peek().length);
	assert(b.peek().length == 0);
	assert(b.buffer.allocOffset == 0);
	assert(b.buffer.peekOffset == 0);
}

/**
A circular buffer which avoids moving data around, but instead maps the same physical memory block twice
into two adjacent virtual memory blocks.
It $(U does) move data blocks when growing the buffer.
*/
struct MmappedBuffer {
private:
	enum pageSize = 4096;
	import flod.meta : NonCopyable;
	mixin NonCopyable;

	import core.sys.posix.stdlib : mkstemp;
	import core.sys.posix.unistd : close, unlink, ftruncate;
	import core.sys.posix.sys.mman : mmap, munmap,
		MAP_ANON, MAP_PRIVATE, MAP_FIXED, MAP_SHARED, MAP_FAILED, PROT_WRITE, PROT_READ;

	void[] buffer;
	size_t peekOffset;
	size_t peekableLength;
	int fd = -1;
	bool grow;

	@property size_t allocOffset() const pure nothrow
	{
		auto ao = peekOffset + peekableLength;
		if (ao <= buffer.length)
			return ao;
		return ao - buffer.length;
	}

	@property size_t allocableLength() const pure nothrow { return buffer.length - peekableLength; }

	invariant {
		assert(peekOffset <= buffer.length);
	}

	this(size_t initialSize, Flag!"grow" grow)
	{
		if (!createFile())
			return;
		if (initialSize)
			buffer = allocate(initialSize);
		this.grow = grow;
	}

	bool createFile()()
	{
		static immutable path = "/dev/shm/flod-buffer-XXXXXX";
		char[path.length + 1] mutablePath = path.ptr[0 .. path.length + 1];
		fd = mkstemp(mutablePath.ptr);
		if (fd < 0)
			return false;
		if (unlink(mutablePath.ptr) != 0) {
			close(fd);
			fd = -1;
			return false;
		}
		return true;
	}

	void[] allocate(size_t length)
	{
		length = length.alignUp(pageSize);
		if (fd < 0)
			return null;
		if (ftruncate(fd, length) != 0)
			return null;

		// first, make sure that a contiguous virtual memory range of 2 * length bytes is available
		void* anon = mmap(null, length * 2, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
		if (anon == MAP_FAILED)
			return null;

		// then map the 2 halves inside to the same range of physical memory
		void* p = mmap(anon, length, PROT_READ | PROT_WRITE, MAP_FIXED | MAP_SHARED, fd, 0);
		if (p == MAP_FAILED) {
			munmap(anon, length * 2);
			return null;
		}
		assert(p == anon);
		p = mmap(anon + length, length, PROT_READ | PROT_WRITE, MAP_FIXED | MAP_SHARED, fd, 0);
		if (p == MAP_FAILED) {
			munmap(anon, length * 2);
			return null;
		}
		assert(p == anon + length);
		return anon[0 .. length];
	}

	bool reallocate(size_t length)
	{
		if (length == buffer.length)
			return true;
		auto newbuf = allocate(length);
		if (!newbuf)
			return false;
		newbuf.ptr[peekOffset .. peekOffset + peekableLength] = buffer.ptr[peekOffset .. peekOffset + peekableLength];
		if (peekOffset > allocOffset) {
			auto po1 = peekOffset;
			auto po2 = newbuf.length - buffer.length;
			peekOffset += newbuf.length - buffer.length;
			if (peekOffset >= newbuf.length)
				peekOffset -= newbuf.length;
		}
		deallocate(buffer);
		buffer = newbuf;
		return true;
	}

	static void deallocate(ref void[] b)
	{
		if (b.ptr) {
			munmap(b.ptr, b.length << 1);
			b = null;
		}
	}

public:
	~this()
	{
		deallocate(buffer);
		if (fd >= 0) {
			close(fd);
			fd = -1;
		}
	}

	/// Returns a read-only slice, typed as `const(T)[]`, containing all data currently available in the buffer.
	const(T)[] peek(T)()
	{
		auto typed = cast(const(T*)) (buffer.ptr + peekOffset);
		auto count = peekableLength / T.sizeof;
		return typed[0 .. count];
	}

	/// Removes first `n` objects of type `T` from the buffer.
	void consume(T)(size_t n)
	{
		size_t tn = T.sizeof * n;
		assert(peekableLength >= tn);
		peekOffset += tn;
		peekableLength -= tn;
		if (peekOffset >= buffer.length)
			peekOffset -= buffer.length;
	}

	/// Allocates space for at least `n` new objects of type `T` to be written to the buffer.
	T[] alloc(T)(size_t n)
	{
		auto typed = cast(T*) (buffer.ptr + allocOffset);
		auto count = allocableLength / T.sizeof;
		if (grow && count < n) {
			// make sure at least T[n] will be available behind what's currently peekable
			reallocate(peekOffset + peekableLength + n * T.sizeof);
			typed = cast(T*) (buffer.ptr + allocOffset);
			count = allocableLength / T.sizeof;
			assert(count >= n); // TODO: let it return smaller chunk and the user will handle it
		}
		return typed[0 .. count];
	}

	/// Adds first `n` objects of type `T` stored in the slice previously obtained using `alloc`.
	/// Does not touch the remaining part of that slice.
	void commit(T)(size_t n)
	{
		size_t tn = T.sizeof * n;
		assert(tn <= allocableLength);
		peekableLength += tn;
	}
}

auto mmappedBuffer(size_t initialSize = 0, Flag!"grow" grow = Yes.grow)
{
	return MmappedBuffer(initialSize, grow);
}

unittest {
	testBuffer(mmappedBuffer());
}
