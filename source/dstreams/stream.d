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

struct Stream(Stages...)
{
	Tuple!Stages stages;

	auto pipe(alias NextStage, Args...)(Args args)
	{
		auto stage = Stage!(NextStage, Args)(tuple(args));
		alias S = typeof(stage);
		return Stream!(Stages, S)(tuple(stages.expand, stage));
	}
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
	void push(ubyte[]) {}
}

struct Test
{
	import std.stdio : stderr;

	int a;
	uint* refs;

	this(int a)
	{
		this.refs = new uint;
		this.a = a;
		*refs = 1;
		stderr.writefln("%d ctor refs=%d", a, *refs);
	}

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

unittest
{
	import dstreams.etc.ogg;
	import dstreams : AlsaSink;

	auto stream0 = stream!NullSink;
	pragma(msg, typeof(stream0));
	pragma(msg, stream0.sizeof);

	auto stream1 = stream!CurlReader("http://icecast.radiovox.org:8000/live.ogg").pipe!NullSink;
	pragma(msg, typeof(stream1));
	pragma(msg, stream1.sizeof);

	auto stream2 = stream!CurlReader("http://icecast.radiovox.org:8000/live.ogg", Test(14)).pipe!BufferedToUnbufferedPushSource.pipe!OggReader.pipe!VorbisDecoder.pipe!AlsaSink;
	pragma(msg, typeof(stream2));
	pragma(msg, stream2.sizeof);
}
