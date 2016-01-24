/** Commonly used stream sources, filters and sinks.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.common;

import flod.stream : isStream;

/** A filter which truncates the stream after reading the specified number elements
 *  or the entire stream, whichever comes first.
 */
struct Take(Sink) {
	Sink sink;
	ulong take;

	///
	this(ulong n)
	{
		this.take = n;
	}

	size_t push(T)(const(T[]) data)
	{
		if (data.length <= take) {
			take -= data.length;
			return sink.push(data);
		} else {
			import std.algorithm : min;
			auto len = min(take, data.length);
			if (len == 0)
				return 0;
			return sink.push(data[0 .. len]);
		}
	}
}

/// Truncate the stream to n elements if its length is greater than n.
auto take(S)(S stream, ulong n)
	if (isStream!S)
{
	return stream.pipe!Take(n);
}

/** A filter which drops the initial n elements from the stream and forwards the remaining part unchanged.
 */
struct Drop(Source) {
	Source source;
	ulong skip;

	/// Construct a Drop filter dropping n elements.
	this(ulong n)
	{
		this.skip = n;
	}

	size_t pull(T)(T[] data)
	{
		if (skip == 0)
			return source.pull(data);
		while (skip) {
			import std.algorithm : min;
			size_t l = min(skip, data.length);
			size_t r = source.pull(data[0 .. l]);
			if (r < l)
				return 0;
			skip -= r;
		}
		return source.pull(data);
	}

	auto peek()(size_t n)
	{
		while (skip) {
			import std.algorithm : min;
			auto data = source.peek(n);
			if (data.length == 0)
				return data;
			auto len = min(skip, data.length);
			source.consume(len);
			skip -= len;
		}
		return source.peek(n);
	}

	void consume()(size_t n)
	{
		source.consume(n);
	}
}

/// _Drop the initial n elements from stream and forward the remaining part unchanged.
auto drop(S)(S stream, ulong n)
	if (isStream!S)
{
	return stream.pipe!Drop(n);
}

/** A sink that discards all data written to it.
 */
struct NullSink {
	void open() {}
	size_t push(T)(const(T[]) buf) { return buf.length; }
	void close() {}
}

/** Read the stream discarding all data.
 */
auto discard(S)(S stream)
	if (isStream!S)
{
	return stream.pipe!NullSink;
}

/** An empty source.
 */
struct NullSource {
	size_t pull(T)(T[] buf) { return 0; }
	const(void)[] peek(size_t n) { return null; }
	void consume(size_t n) { assert(false); }
}

unittest
{
	import flod.stream : stream, PullPush;

	stream!NullSource.pipe!PullPush.discard().run(); // do nothing
	stream!NullSource.pipe!PullPush.pipe!NullSink().run(); // also do nothing
}
