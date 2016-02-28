/** Various buffer implementations.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.buffer;

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

import std.experimental.allocator.mallocator : Mallocator;

/// A buffer that discards all data written to it and always returns empty slice.
struct NullBuffer {
private:
	import flod.meta : NonCopyable;
	mixin NonCopyable;
	void[] buffer;
public:
	~this() { Mallocator.instance.deallocate(buffer); }
	T[] alloc(T)(size_t n)
	{
		return new T[n];
	}
	void commit(T)(size_t n) {};
	const(T)[] peek(T)() { return null; }
	void consume(T)(size_t n) {};
}

/// A buffer that expands infinitely to ensure data peeked at are contiguous.
struct ExpandingBuffer(Allocator = Mallocator) {
private:
	import std.stdio;
	void[] buffer;
	size_t peekOffset;
	size_t allocOffset;
	Allocator allocator;

	this()(auto ref Allocator allocator) {
		import flod.meta : moveIfNonCopyable;
		this.allocator = moveIfNonCopyable(allocator);
	}

	invariant {
		assert(peekOffset <= allocOffset);
		assert(allocOffset <= buffer.length);
	}

public:
	/// Allocates space for at least `n` new objects of type `T` to be written to the buffer.
	T[] alloc(T)(size_t n)
	{
		import std.experimental.allocator : reallocate;
		size_t tn = T.sizeof * n;
		if (buffer.length - allocOffset >= tn)
			return cast(T[]) buffer[allocOffset .. $];
		size_t newSize = goodSize(allocator, allocOffset + tn);
		allocator.reallocate(buffer, newSize);
		assert(buffer.length - allocOffset >= n);
		return cast(T[]) buffer[allocOffset .. $];
	}

	/// Adds first `n` objects of type `T` stored in the slice previously obtained using `alloc`.
	/// Does not touch the remaining part of that slice.
	void commit(T)(size_t n)
	{
		size_t tn = T.sizeof * n;
		allocOffset += tn;
		assert(allocOffset <= buffer.length);
	}

	/// Return a read-only slice of the buffer, typed as `const(T)[]`, containing all data available in the buffer.
	const(T)[] peek(T)()
	{
		return cast(const(T)[]) buffer[peekOffset .. allocOffset];
	}

	/// Removes first `n` objects of type `T` from the buffer.
	void consume(T)(size_t n)
	{
		size_t tn = T.sizeof * n;
		peekOffset += tn;
		assert(peekOffset <= allocOffset);
		if (peekOffset == buffer.length) {
			peekOffset = 0;
			allocOffset = 0;
		}
	}
}

///
auto expandingBuffer(Allocator)(auto ref Allocator allocator)
{
	return ExpandingBuffer!Allocator(allocator);
}

///
auto expandingBuffer()
{
	import std.experimental.allocator.mallocator : Mallocator;
	return expandingBuffer(Mallocator.instance);
}

unittest {
	import std.range : iota, array, put;
	import std.algorithm : copy;
	auto b = expandingBuffer();
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
	// consume everything and check if b will reset its pointers.
	b.consume!uint(b.peek!uint().length);
	assert(b.peek!uint().length == 0);
	assert(b.allocOffset == 0);
	assert(b.peekOffset == 0);
}
