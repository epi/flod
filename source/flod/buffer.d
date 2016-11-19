/**
Various buffer implementations.

Buffer_interface:

In flod, a buffer is a FIFO with access to multiple elements at a time.

A compliant buffer must implement the following member functions.

---
void[] alloc(size_t n);
void commit(size_t n);
const(void)[] peek();
void consume(size_t n);
---

The semantics of these calls is as follows:
$(UL
 $(LI `alloc` extends the internal storage to accomodate at least `n` new bytes. It should return a slice where the
  new data are to be written. Returning `null` or a smaller slice means that the buffer will never be able to fulfill
  allocation request of that size.)
 $(LI `commit` commits the first `n` bytes written to the slice obtained from `alloc`. `n` must be positive and
  must never be greater than the length of the slice.)
 $(LI `peek` returns a read-only, contiguous view on all the data committed to the buffer, but not yet consumed.)
 $(LI `consume` removes `n` bytes from the front of the queue. `n` must be positive and must not be greater than
  the length of the slice returned by `peek`.)
)

This module offers a few readily available buffer implementations, such as `MovingBuffer`, `MmappedBuffer`
and `GCBuffer`.

Buffer_composition:

A buffer may impose a limit on the maximum length of contiguous slice that can be allocated. `FallbackBuffer`
provides a means to switch to another buffer implementation if the primary one has such a limit.
Extreme cases of such buffers are `NullBuffer` (always returns `null` slices) and `FailBuffer` (which always throws
from `alloc`). They are mainly useful as "terminators" for composite buffers such as `FallbackBuffer`.

Basic buffer implementation operates on raw slices of memory (`void[]`), but a safe, typed view can be
implemented on top of that using `TypedBuffer`.

Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
Copyright: © 2016 Adrian Matoga
License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
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
	private void testBuffer(Buffer)(auto ref Buffer b)
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
	testBuffer(tb);
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
	///
	this(Allocator allocator) @trusted
	{
		this.allocator = allocator;
		storage = allocator.allocate(allocator.goodSize(4096));
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
		size_t newSize = goodSize(allocator, allocOffset + n + 4096);
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

unittest {
	auto b = typedBuffer!int(movingBuffer());
	testBuffer(b);
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

	void[] storage;
	size_t peekOffset;
	size_t peekableLength;
	int fd = -1;

	@property size_t allocOffset() const pure nothrow
	{
		auto ao = peekOffset + peekableLength;
		if (ao <= storage.length)
			return ao;
		return ao - storage.length;
	}

	@property size_t allocableLength() const pure nothrow { return storage.length - peekableLength; }

	invariant {
		assert(peekOffset <= storage.length);
	}

	this(size_t initial_capacity)
	{
		if (!createFile())
			return;
		storage = allocate(initial_capacity);
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
		if (length == storage.length)
			return true;
		auto newbuf = allocate(length);
		if (!newbuf)
			return false;
		newbuf.ptr[peekOffset .. peekOffset + peekableLength] = storage.ptr[peekOffset .. peekOffset + peekableLength];
		if (peekOffset > allocOffset) {
			peekOffset += newbuf.length - storage.length;
			if (peekOffset >= newbuf.length)
				peekOffset -= newbuf.length;
		}
		deallocate(storage);
		storage = newbuf;
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
		deallocate(storage);
		if (fd >= 0) {
			close(fd);
			fd = -1;
		}
	}

	///
	const(void)[] peek()()
	{
		return storage.ptr[peekOffset .. peekOffset + peekableLength];
	}

	///
	void consume()(size_t n)
	{
		assert(peekableLength >= n);
		peekOffset += n;
		peekableLength -= n;
		if (peekOffset >= storage.length)
			peekOffset -= storage.length;
	}

	///
	void[] alloc()(size_t n)
	{
		if (allocableLength < n) {
			size_t newsize = peekableLength + n;
			if (newsize < n)
				return null;
			if (newsize > storage.length)
				reallocate(newsize);
		}
		return storage.ptr[allocOffset .. allocOffset + allocableLength];
	}

	///
	void commit()(size_t n)
	{
		assert(n <= allocableLength);
		peekableLength += n;
	}
}

/// ditto
auto mmappedBuffer(size_t initial_capacity)
{
	return MmappedBuffer(initial_capacity);
}

/// ditto
auto mmappedBuffer()
{
	return mmappedBuffer(4096);
}

unittest {
	// TODO: implement a non-growing mmappedBuffer
/+
	auto buf = typedBuffer!ubyte(mmappedBuffer());
	auto a = buf.alloc(2048);
	assert(a.length == 4096);
	buf.commit(2048);
	assert(buf.peek().length == 2048);
	buf.consume(2047);
	assert(buf.peek().length == 1);
	a = buf.alloc(4095);
	assert(a.length == 4095);
	a = buf.alloc(4097);
	assert(a is null);
	a = buf.alloc(4096);
	assert(a.length == 4095);
	buf.commit(4095);
	assert(buf.peek().length == 4096);
	buf.consume(2050);
	assert(buf.peek().length == 2046);
+/
}

unittest {
	testBuffer(typedBuffer!int(mmappedBuffer()));
}

/// A buffer that always returns empty slices.
struct NullBuffer {
	void[] alloc(size_t n) pure nothrow const @safe { return null; }
	const(void)[] peek() pure nothrow const @safe { return null; }
	void consume(size_t n) pure nothrow const @safe { assert(0); }
	void commit(size_t n) pure nothrow const @safe { assert(0); }
}

///
unittest {
	NullBuffer nb;
	assert(nb.peek() is null);
	assert(nb.alloc(1) is null);
}

/// A buffer that always throws on `alloc`.
struct FailBuffer {
	void[] alloc(size_t n)
	{
		import core.exception : OutOfMemoryError;
		static __gshared error = new OutOfMemoryError;
		throw error;
	}

	const(void)[] peek() { return null; }
	void consume(size_t n) { assert(0); }
	void commit(size_t n) { assert(0); }
}

///
unittest {
	import core.exception : OutOfMemoryError;
	import std.exception : assertThrown;
	FailBuffer fb;
	assert(fb.peek() is null);
	assertThrown!OutOfMemoryError(fb.alloc(1));
}

/**
A wrapper that forwards all calls to `Primary` and switches to `Fallback` as soon as
`Primary` fails to fulfill an `alloc` request.
*/
struct FallbackBuffer(Primary, Fallback) {
private:
	Primary primary;
	Fallback fallback;
	bool currentIsPrimary = true;

	void[] doAlloc(B1, B2)(ref B1 current, ref B2 other, size_t n)
	{
		auto result = current.alloc(n);
		if (result.length >= n)
			return result;
		currentIsPrimary = !currentIsPrimary;
		auto left = current.peek();
		if (left.length) {
			auto buf = other.alloc(left.length);
			assert(buf.length >= left.length);
			buf[0 .. left.length] = left[];
		}
		result = other.alloc(n);
		assert(result.length >= n);
		return result;
	}

public:
	const(void)[] peek()()
	{
		if (currentIsPrimary)
			return primary.peek();
		else
			return fallback.peek();
	}

	void[] alloc()(size_t n)
	{
		if (currentIsPrimary)
			return doAlloc(primary, fallback, n);
		else
			return doAlloc(fallback, primary, n);
	}

	void commit()(size_t n)
	{
		if (currentIsPrimary)
			primary.commit(n);
		else
			fallback.commit(n);
	}

	void consume()(size_t n)
	{
		if (currentIsPrimary)
			primary.consume(n);
		else
			fallback.consume(n);
	}
}

/// ditto
auto fallbackBuffer(Primary, Fallback)(Primary p, Fallback f)
{
	return FallbackBuffer!(Primary, Fallback)(p, f);
}

///
unittest {
	auto b = fallbackBuffer(NullBuffer(), GCBuffer());
	assert(b.currentIsPrimary);
	assert(b.peek().length == 0);
	assert(b.currentIsPrimary);
	auto v = b.alloc(1024);
	assert(!b.currentIsPrimary);
	b.commit(777);
	assert(!b.currentIsPrimary);
	assert(b.peek().length == 777);
	assert(!b.currentIsPrimary);
	b.consume(333);
	assert(!b.currentIsPrimary);
	assert(b.peek().length == 444);
}
