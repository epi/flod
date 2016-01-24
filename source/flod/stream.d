/** Templates which link the stream components together.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.stream;

import std.typecons : tuple, Tuple;
import std.stdio;

struct Stage(alias S, A...)
{
	alias Impl = S;
	alias Args = A;
	Tuple!Args args;
}

struct Stream(Stages...) {
	Tuple!Stages stages;

	private template Component(S) {
		static if (is(S == struct))
			alias Component = S;
	}

	private template StreamBuilder(int begin, int cur, int end) {
		static assert(begin <= end && begin <= cur && cur <= end && end <= Stages.length,
			"Invalid parameters: " ~
			Stages.stringof ~ "[" ~ begin.stringof ~ "," ~ cur.stringof ~ "," ~ end.stringof ~ "]");
		static if (cur < end) {
			alias Cur = Stages[cur].Impl;
			alias Lhs = StreamBuilder!(begin, begin, cur);
			alias Rhs = StreamBuilder!(cur + 1, cur + 1, end);
			static if (is(Component!(Lhs.Impl) _Li))
				alias LhsImpl = _Li;
			static if (is(Component!(Rhs.Impl) _Ri))
				alias RhsImpl = _Ri;
			static if (begin + 1 == end && is(Cur _Impl)) {
				alias Impl = _Impl;
				pragma(msg, "Match: ", Stages[begin]);
			} else static if (cur + 1 == end && is(Cur!LhsImpl _Impl)) {
				alias Impl = _Impl;
				pragma(msg, "Match: ", Stages[cur .. end]);
			} else static if (cur == begin && is(Cur!RhsImpl _Impl)) {
				alias Impl = _Impl;
				pragma(msg, "Match: ", Stages[begin .. cur + 1]);
			} else static if (is(Cur!(LhsImpl, RhsImpl) _Impl)) {
				alias Impl = _Impl;
				pragma(msg, "Match: ", Stages[begin .. end]);
			}
			static if (is(Impl)) {
				void construct(ref Impl impl) {
					static if (is(LhsImpl))
						Lhs.construct(impl.source);
					static if (is(RhsImpl))
						Rhs.construct(impl.sink);
					static if (Stages[cur].Args.length > 0) impl.__ctor(stages[cur].args.expand);
					writefln("%-30s %X[%d]: [%(%02x%|, %)]", Stages[cur].Impl.stringof, &impl, impl.sizeof, (cast(ubyte*) &impl)[0 .. impl.sizeof]);
				}
			} static if (cur + 1 < end) {
				alias Next = StreamBuilder!(begin, cur + 1, end);
				static if (is(Next.Impl))
					alias StreamBuilder = Next;
			}
		}
	}

	auto pipe(alias NextStage, Args...)(Args args)
	{
		auto stage = Stage!(NextStage, Args)(tuple(args));
		alias S = typeof(stage);
		return Stream!(Stages, S)(tuple(stages.expand, stage));
	}

	void run()()
	{
		import std.stdio;
		alias Builder = StreamBuilder!(0, 0, Stages.length);
		alias Impl = Builder.Impl;
		static assert(is(Impl), "Could not build stream out of the following list of stages: " ~ Stages.stringof);
		Impl impl;
		writefln("%X[%d]: [%(%02x%|, %)]", &impl, impl.sizeof, (cast(ubyte*) &impl)[0 .. impl.sizeof]);
		Builder.construct(impl);
		writeln(typeof(impl).stringof, ": ", impl.sizeof);
		impl.run();
	}
}

template isStage(Ss...) {
	static if (Ss.length == 1) {
		alias S = Ss[0];
		enum bool isStage = is(S == Stage!TL, TL...);
	} else {
		enum bool isStage = false;
	}
}

template isStream(Ss...) {
	static if (Ss.length == 1) {
		alias S = Ss[0];
		static if (is(S == Stream!TL, TL...)) {
			import std.meta : allSatisfy;
			enum isStream = allSatisfy!(isStage, TL);
		}
	} else {
		enum isStream = false;
	}
}

auto stream(alias Stage1, Args...)(Args args)
{
	auto tup = tuple(args);
	auto stage = Stage!(Stage1, Args)(tup);
	alias S = typeof(stage);
	return Stream!S(tuple(stage));
}

import flod.traits;

/// Convert buffered push source to unbuffered push source
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

/** Drive a stream which doesn't have any driving components
 */
struct PullPush(Source, Sink) {
	Source source = void;
	Sink sink = void;

	void run()
	{
		ubyte[4096] buf;
		for (;;) {
			size_t n = source.pull(buf[]);
			if (n == 0)
				break;
			if (sink.push(buf[0 .. n]) < n)
				break;
		}
	}
}

struct Test(Sink = void)
{
	import std.stdio : stderr;

	static if (!is(Sink == void)) {
		Sink sink;
		this(Sink sink, int a)
		{
			this.sink = sink;
			this.refs = new uint;
			this.a = a;
			*refs = 1;
			stderr.writefln("%d ctor refs=%d", a, *refs);
		}
	} else {
		this(int a)
		{
			this.refs = new uint;
			this.a = a;
			*refs = 1;
			stderr.writefln("%d ctor refs=%d", a, *refs);
		}
	}

	int a;
	uint* refs;


	auto opAssign(Test rhs)
	{
		import std.algorithm : swap;
		swap(this, rhs);
	}

	this(this)
	{
		++*refs;
		stderr.writefln("%d copy refs=%d", a, *refs);
	}

	~this()
	{
		if (!refs)
			return;
		--*refs;
		stderr.writefln("%d dtor refs=%d", a, *refs);
	}
}

auto test(T...)(T a)
{
	static if (T.length == 1)
		return Test!()(a[0]);
	else static if (T.length == 2)
		return Test!(T[0])(a[0], a[1]);
	else
		return 0;
}

unittest
{
	import flod.etc.ogg;
	import flod.common : discard;
	import std.stdio;
	{
		auto a = test(1337);
		writefln("\n{%d}", a.a);
	}
	{
		auto b = test(test(test(3), 13), 37);
	}

//	auto stream1 = stream!CurlReader("http://icecast.radiovox.org:8000/live.ogg").discard();
//	pragma(msg, typeof(stream1));
//	pragma(msg, stream1.sizeof);


//	auto stream2 = stream!CurlReader("http://icecast.radiovox.org:8000/live.ogg", Test!()(14)).pipe!BufferedToUnbufferedPushSource.pipe!OggReader.pipe!PushTee.pipe!VorbisDecoder.pipe!AlsaSink;
//	pragma(msg, typeof(stream2));
//	pragma(msg, stream2.sizeof);
	//stream0.run;
	//stream1.run();
	//stream2a.run();
	//stream2.run();
}
