/** Construct stream objects out of pipelines or single stream components.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.stream;

import flod.pipeline : isPipeline;

/** A _stream based on pipeline P.
 *
 * Instances of `Stream` are not copyable. If you need a _stream object that can be passed around,
 * for example to create range objects, see `RefCountedStream`.
 */
struct Stream(P)
	if (isPipeline!P)
{
	import std.experimental.allocator : IAllocator;
	import flod.traits : isPeekSource, isPullSource;
	import flod.adapter : PullBuffer;

	static if (isPeekSource!(P.LastStage)) {
		private P.Impl _impl;
	} else {
		PullBuffer!(P.Impl) _pullBuffer;
		private @property ref auto _impl() { return _pullBuffer.source; }
	}
	private IAllocator _allocator;

	this(P p)
	{
		_allocator = p.allocator;
		p.construct(_impl);
		static if (is(typeof(this._pullBuffer.allocator))) {
			this._pullBuffer.allocator = _allocator;
		}
	}

	@disable this(this);
	@disable void opAssign(Stream!P);

	version (D_Ddoc) {
		/** Source primitives.
		 *
		 * If the last stage of the pipeline supports these methods natively,
		 * the calls are simply forwarded.
		 * Otherwise, the appropriate copying/buffering is done here.
		 */
		auto peek()(size_t n);
		/// ditto
		auto peek(T)(size_t n);
		/// ditto
		void consume()(size_t n);
		/// ditto
		void consume(T)(size_t n);
		/// ditto
		size_t pull(T)(T[] buf);
	}

	static if (isPeekSource!(P.LastStage)) {
		auto peek()(size_t n) { return _impl.peek(n); }

		auto peek(T)(size_t n) {
			static if (is(typeof(_impl.peek!T(n)[0]) : const(T)))
				return _impl.peek!T(n);
			else static if (is(typeof(_impl.peek(n)[0]) : const(T)))
				return _impl.peek(n);
			else
				static assert(false, P.lastStageName ~ " does not support data type " ~ T.stringof);
		}

		void consume()(size_t n) { return _impl.consume(n); }

		void consume(T)(size_t n)
		{
			static if (is(typeof({_impl.consume!T(n);}())))
				return _impl.consume!T(n);
			else static if (is(typeof(_impl.peek(n)[0]) : const(T)))
				return _impl.consume(n);
			else
				static assert(false, P.lastStageName ~ " does not support data type " ~ T.stringof);
		}

		static if (!isPullSource!(P.LastStage)) {
			size_t pull(T)(T[] buf)
			{
				import std.algorithm : min;
				auto inbuf = peek!T(buf.length);
				auto len = min(inbuf.length, buf.length);
				buf[0 .. len] = inbuf[0 .. len];
				consume!T(len);
				return len;
			}
		}
	}

	static if (isPullSource!(P.LastStage)) {
		size_t pull(T)(T[] buf) { return _impl.pull(buf); }

		static if (!isPeekSource!(P.LastStage)) {
			auto peek()(size_t n) { return _pullBuffer.peek(n); }
			auto peek(T)(size_t n) { return _pullBuffer.peek!T(n); }
			void consume()(size_t n) { _pullBuffer.consume(n); }
			void consume(T)(size_t n) { _pullBuffer.consume!T(n); }
		}
	}
}

import flod.pipeline : isPipeline, pipe;
import flod.traits : isStreamComponent;

auto stream(alias FirstStage, Args...)(auto ref Args args)
	if (isStreamComponent!FirstStage)
{
	return pipe!FirstStage(args).stream();
}

auto stream(T)(auto ref T something)
	if (isPipeline!T)
{
	return Stream!T(something);
}


auto stream(T)(auto ref T something)
	if (is(Stream!(typeof(pipe(something)))))
{
	auto pl = pipe(something);
	return Stream!(typeof(pl))(pl);
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

unittest
{
	static struct CountingSource {
		uint cnt;
		size_t pull(uint[] buf)
		{
			foreach (ref n; buf) { n = cnt++; }
			return buf.length;
		}
	}
	auto stream = stream!CountingSource();
	//auto stream = p.stream();
	import std.range: iota, array;

	assert(stream.peek(10)[0 .. 10] == iota(0, 10).array);
	assert(stream.peek(5)[0 .. 5] == iota(0, 5).array);
	stream.consume(5);
	assert(stream.peek(4096)[0 .. 4096] == iota(5, 4101).array);
}

///
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

	@property bool isNull() const @safe @nogc { return _impl is null; }

	auto pull(T)(T[] buf) { return _impl.stream.pull(buf); }

	auto peek()(size_t n) { return _impl.stream.peek(n); }
	auto peek(T)(size_t n) { return _impl.stream.peek!T(n); }
	void consume()(size_t n) { return _impl.stream.consume(n); }
	void consume(T)(size_t n) { return _impl.stream.consume!T(n); }
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
