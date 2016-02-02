/** Construct stream objects out of pipelines or single stream components.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.stream;

import flod.pipeline : isPipeline;

// A stream based on pipeline P
struct Stream(P)
	if (isPipeline!P)
{
	import std.experimental.allocator : IAllocator;
	import flod.traits : isPeekSource, isPullSource;

	private P.Impl _impl;
	private IAllocator _allocator;

	this(P p)
	{
		_allocator = p.allocator;
		p.construct(_impl);
	}

	@disable this(this);
	@disable void opAssign(Stream!P);

	static if (isPeekSource!(P.LastStage)) {
		auto peek()(size_t n) {
			return _impl.peek(n);
		}
		auto peek(T)(size_t n) {
			return _impl.peek!T(n);
		}
		void consume()(size_t n) {
			return _impl.consume(n);
		}
		void consume(T)(size_t n) {
			return _impl.consume!T(n);
		}
	}

	auto pull(T)(T[] buf) {
		static if (isPullSource!(P.LastStage)) {
			return _impl.pull(buf);
		} else static if (isPeekSource!(P.LastStage)) {
			static if (is(typeof({ return _impl.peek!T(buf.length)[0]; }()) : const(T))) {
				import std.algorithm : min;
				auto inbuf = _impl.peek!T(buf.length);
				auto len = min(inbuf.length, buf.length);
				buf[0 .. len] = inbuf[0 .. len];
				_impl.consume!T(len);
				return len;
			} else static if (is(typeof({ return _impl.peek(buf.length)[0]; }()) : const(T))) {
				import std.algorithm : min;
				auto inbuf = _impl.peek(buf.length);
				auto len = min(inbuf.length, buf.length);
				buf[0 .. len] = inbuf[0 .. len];
				_impl.consume(len);
				return len;
			} else {
				static if (is(typeof({ return _impl.peek(buf.length)[0]; }()) T)) {
					pragma(msg, T.stringof);
				}
				static assert(false, P.lastStageName ~ " does not support data type " ~ T.stringof);
			}
		}
	}
}

auto stream(T)(auto ref T something)
{
	static if (isPipeline!T) {
		return Stream!T(something);
	} else {
		import flod.pipeline : pipe;
		auto pl = pipe(something);
		return Stream!(typeof(pl))(pl);
	}
}

unittest
{
	auto arr = [10, 20, 30, 40, 50, 60, 70, 80];
	auto stream = arr.stream();
	auto p1 = stream.peek(5);
	assert(p1.length >= 5);
	assert(p1[0 .. 5] == [10, 20, 30, 40, 50]);
	stream.consume(4);
	p1 = stream.peek(5);
	assert(p1.length == 4);
	assert(p1 == [50, 60, 70, 80]);
}

unittest
{
	auto arr = [10, 20, 30, 40, 50, 60, 70, 80];
	int[5] intbuf;
	ubyte[5] bytbuf;
	auto stream = arr.stream();
	static assert(!__traits(compiles, stream.pull(bytbuf[])));
	size_t r = stream.pull(intbuf[]);
	assert(intbuf[0 .. r] == [10, 20, 30, 40, 50]);
	r = stream.pull(intbuf[]);
	assert(intbuf[0 .. r] == [60, 70, 80]);
}

struct RefCountedStream(P)
	if (isPipeline!P)
{
	import std.experimental.allocator : IAllocator;

	private static struct Impl {
		Stream!P stream = void;
		IAllocator allocator;
		uint refs = 1;
	}

	private Impl* _impl;
	private this(P p) {
		import std.experimental.allocator : make;
		_impl = p.allocator.make!Impl;
		_impl.stream.__ctor(p);
		_impl.allocator = p.allocator;
		_impl.refs = 1;
	}

	this(this)
	{
		if (_impl)
			++_impl.refs;
	}
	void opAssign(RefCountedStream rhs)
	{
		import std.algorithm : swap;
		swap(this, rhs);
	}
	~this()
	{
		if (!_impl)
			return;
		if (_impl.refs == 1) {
			import std.experimental.allocator : dispose;
			_impl.allocator.dispose(_impl);
			_impl = null;
		} else {
			assert(_impl.refs > 1);
			--_impl.refs;
		}
	}

	auto pull(T)(T[] buf) {
		return _impl.stream.pull(buf);
	}

	auto peek()(size_t n) {
		return _impl.stream.peek(n);
	}
	auto peek(T)(size_t n) {
		return _impl.stream.peek!T(n);
	}
	void consume()(size_t n) {
		return _impl.stream.consume(n);
	}
	void consume(T)(size_t n) {
		return _impl.stream.consume!T(n);
	}
}

auto refCountedStream(T)(auto ref T something)
{
	static if (isPipeline!T) {
		return RefCountedStream!T(something);
	} else {
		import flod.pipeline : pipe;
		auto pl = pipe(something);
		return RefCountedStream!(typeof(pl))(pl);
	}
}
