/**
High-level interface for $(B flod). Provides the most commonly used functions of the package.

Package_description:

$(B flod) is a library for processing streams of data using composable building blocks.

In $(B flod), a $(I pipeline) is a chain of one or more $(I stages), which communicate with
each other using the specified interfaces.

A stage can be either a $(I source), a $(I sink), or a $(I filter), which is
both a sink and a source. A source-only stage is one that only produces data, e.g.
reads a file or generates random bytes. A sink-only stage is one that only consumes
data from previous stages. For example, a sink may use the data to build an array or replay
audio samples. A filter stage receives data on its sink end,
and produces transformed data on its source end. Examples of such stages are media decoders
or cipher implementations.

There are four methods of passing data from a source to a sink:
$(TABLE
 $(TR $(TH method)     $(TH description)                                         $(TH buffer is owned by))
 $(TR $(TD $(I pull))  $(TD `sink` calls `source.pull()`)                        $(TD sink))
 $(TR $(TD $(I push))  $(TD `source` calls `sink.push()`)                        $(TD source))
 $(TR $(TD $(I peek))  $(TD `sink` calls `source.peek()` and `source.consume()`) $(TD source))
 $(TR $(TD $(I alloc)) $(TD `source` calls `sink.alloc()` and `sink.commit()`)   $(TD sink))
)

Note that a filter can use different methods for its sink and source ends. For example, it may
call `peek` and `consume` to access the input data, and output transformed data using `push`.

Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
Copyright: © 2016 Adrian Matoga
License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
*/
module flod;

import flod.pipeline : pipe, isSchema;
import flod.traits : Method, source, sink, filter;

public import flod.range : copy, pass, byLine, byChunk;
public import flod.file : read, write;

@sink(Method.push)
private struct NullSink(alias Context, A...) {
	mixin Context!A;

	size_t push(const(InputElementType)[] chunk)
	{
		return chunk.length;
	}
}

/**
A sink that discards all data written to it.
*/
void discard(S)(S schema)
	if (isSchema!S)
{
	return schema.pipe!NullSink;
}

///
unittest {
	import std.range : iota;
	"not important".discard();
	iota(31337).discard();
}

@sink(Method.pull)
private struct ArraySink(alias Context, A...) {
	mixin Context!A;
private:
	alias E = InputElementType;

public:
	@property E[] front()()
	{
		E[] array;
		size_t offset;
		for (;;) {
			array.length = array.length + 16384;
			offset += source.pull(array[offset .. $]);
			if (offset < array.length)
				return array[0 .. offset];
		}
	}
}

/**
A sink that reads the entire stream and stores its contents in a GC-allocated array.

`array` cannot be used for pipelines that start with an output range wrapper (`flod.range.pass(E)`).
If you need a pipeline with output range interface that stores the output in an array,
use `flod.range.pass` with delegate parameter or `flod.range.copy` with `std.array.Appender`.

Returns:
A GC-allocated array filled with the output of the previous stage.

See_Also:
`flod.range.copy`
*/
auto array(S)(S schema)
	if (isSchema!S)
{
	return schema.pipe!ArraySink.front;
}

///
unittest {
	import std.range : iota, stdarray = array;
	assert(iota(1048576).array == iota(1048576).stdarray);
}

@filter(Method.push)
@filter(Method.pull)
@filter(Method.peek)
@filter(Method.alloc)
private struct Take(alias Context, A...) {
	mixin Context!A;
private:
	ulong limit;
	alias E = InputElementType;
	static assert(is(E == OutputElementType));
	enum method = inputMethod;
	static assert(outputMethod == inputMethod);

public:
	this(ulong limit)
	{
		this.limit = limit;
	}

	static if (method == Method.pull) {
		size_t pull(E[] buf)
		{
			if (buf.length <= limit) {
				limit -= buf.length;
				return source.pull(buf);
			}
			auto result = source.pull(buf[0 .. limit]);
			limit = 0;
			return result;
		}
	} else static if (method == Method.peek) {
		const(E)[] peek(size_t n)
		out(result)
		{
			assert(result.length <= n);
		}
		body
		{
			import std.algorithm : min;
			auto l = min(n, limit);
			auto result = source.peek(l);
			return result[0 .. min(l, result.length)];
		}

		void consume(size_t n)
		{
			assert(n <= limit);
			source.consume(n);
			limit -= n;
		}
	} else static if (method == Method.push) {
		size_t push(const(E)[] buf)
		{
			if (buf.length <= limit) {
				limit -= buf.length;
				return sink.push(buf);
			}
			auto result = sink.push(buf[0 .. limit]);
			limit = 0;
			return result;
		}
	} else static if (method == Method.alloc) {
		bool alloc(ref E[] buf, size_t n)
		{
			return sink.alloc(buf, n);
		}

		size_t commit(size_t n)
		{
			if (n <= limit) {
				limit -= n;
				return sink.commit(n);
			}
			auto result = sink.commit(limit);
			limit = 0;
			return result;
		}
	}
}

/// Lazily takes only up to `n` elements of a stream.
auto take(S)(S schema, ulong n)
	if (isSchema!S)
{
	return schema.pipe!Take(n);
}

///
unittest {
	import std.algorithm : equal;
	import std.range : iota;
	assert([ 1, 2, 4, 8, 16 ].take(3)[].equal([ 1, 2, 4 ]));
	assert(iota(31337).take(1024)[].equal(iota(1024)));
}

unittest {
	import std.algorithm : min;
	import std.meta : AliasSeq;
	import std.range : iota;
	import flod.pipeline : pipe, Arg, TestPullSource, TestPullSink, TestPeekSource, TestPeekSink,
		TestPushSource, TestPushSink, TestAllocSource, TestAllocSink, inputArray, outputArray, outputIndex;

	auto ia = iota(10495832UL).array();
	foreach (m; AliasSeq!("Pull", "Peek", "Push", "Alloc")) {
		foreach (nt; [ 0, 1, 4095, 4096, 4097, 10495831, 10495832, 10495833, 999999999999 ]) {
			inputArray = ia;
			outputArray.length = ia.length;
			outputIndex = 0;
			mixin(`alias Source = Test` ~ m ~ `Source;`);
			mixin(`alias Sink = Test` ~ m ~ `Sink;`);
			pipe!Source(Arg!Source()).take(nt).pipe!Sink(Arg!Sink());
			assert(outputArray[0 .. min(outputIndex, ia.length)] == ia[0 .. min(nt, ia.length)]);
		}
	}
}
