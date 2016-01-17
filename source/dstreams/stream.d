/** Templates which link the stream components together.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module dstreams.stream;


import std.typecons : tuple, Tuple;

import dstreams : CurlReader, BufferedToUnbufferedPushSource;

struct Stage(alias S, Args...)
{
	alias Impl = S;
	Tuple!Args args;
}

template StreamObj(Stages...)
{
	static if (Stages.length == 0) {
		alias StreamObj = void;
	} else {
		alias _Impl = Stages[0].Impl;
		static if (__traits(isTemplate, _Impl)) {
			pragma(msg, "is template: ", _Impl.stringof, " L: ", Stages[1 .. $].length);
			static if (is(StreamObj!(Stages[1 .. $]) == void)) {
				alias StreamObj = void;
			} else static if (is(_Impl!(StreamObj!(Stages[1 .. $])) == struct)) {
				alias StreamObj = _Impl!(StreamObj!(Stages[1 .. $])*);
			} else static if (is(_Impl!(StreamObj!(Stages[1 .. $])) == class)) {
				alias StreamObj = _Impl!(StreamObj!(Stages[1 .. $]));
			}
		} else static if (Stages.length == 1 && is(_Impl)) {
			pragma(msg, "is type:     ", _Impl.stringof);
			alias StreamObj = _Impl;
		}
	}
}

struct Stream(Stages...)
{
	Tuple!Stages stages;

	auto pipe(alias NextStage, Args...)(Args args)
	{
		auto stage = Stage!(NextStage, Args)(tuple(args));
		alias S = typeof(stage);
		return Stream!(Stages, S)(tuple(stages.expand, stage));
	}

	static if (!is(SOT == void)) {
		void run()
		{
			import std.stdio;
			SOT sot;
			writeln(sot.sizeof);
			writeln(sot);
		}
	}
	alias SOT = StreamObj!Stages;
}

auto stream(alias Stage1, Args...)(Args args)
{
	auto tup = tuple(args);
	auto stage = Stage!(Stage1, Args)(tup);
	alias S = typeof(stage);
	return Stream!S(tuple(stage));
}

struct NullSink
{
	void open() {}
	void push(const(ubyte[])) {}
	void close() {}
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
	import dstreams.etc.ogg;
	import dstreams : AlsaSink;
	import std.stdio;

	auto stream0 = stream!NullSink;
	pragma(msg, typeof(stream0));
	pragma(msg, stream0.sizeof);
	pragma(msg, "STREAM OBJECT TYPE: ", stream0.SOT.stringof);

	auto stream1 = stream!CurlReader("http://icecast.radiovox.org:8000/live.ogg").pipe!NullSink;
	pragma(msg, typeof(stream1));
	pragma(msg, stream1.sizeof);
	pragma(msg, "STREAM OBJECT TYPE: ", stream1.SOT.stringof);

	auto stream2a = stream!VorbisDecoder.pipe!AlsaSink;
	pragma(msg, typeof(stream2a));
	pragma(msg, stream2a.sizeof);
	pragma(msg, "STREAM OBJECT TYPE: ", stream2a.SOT.stringof);

	auto stream2 = stream!CurlReader("http://icecast.radiovox.org:8000/live.ogg", Test!()(14)).pipe!BufferedToUnbufferedPushSource.pipe!OggReader.pipe!PushTee.pipe!VorbisDecoder.pipe!AlsaSink;
	pragma(msg, typeof(stream2));
	pragma(msg, stream2.sizeof);
	pragma(msg, "STREAM OBJECT TYPE: ", stream2.SOT.stringof);
	alias S = stream2.SOT;
	S obj;
	pragma(msg, typeof(obj), " SIZE=", obj.sizeof, " ", obj.init.stringof);
	stream0.run();
	stream1.run();
	stream2a.run();
	stream2.run();
}
