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
auto discard(S)(S schema)
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
A sink that stores all the stream data in a GC-allocated array and returns the array.
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
