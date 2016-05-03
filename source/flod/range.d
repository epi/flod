/** Convert ranges to pipelines and pipelines to ranges.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.range;

import std.range : isInputRange, isOutputRange;
import std.traits : isSomeChar;
import std.typecons : Flag, No;

import flod.pipeline : pipe, isSchema;
import flod.traits;

version(unittest) import std.algorithm : equal, map, filter;

private template ArraySource(E) {
	@source!E(Method.peek)
	struct ArraySource(alias Context, A...) {
		mixin Context!A;
		private const(E)[] array;
		this(const(E)* ptr, size_t length)
		{
			this.array = ptr[0 .. length];
		}

		const(E)[] peek()(size_t n) { return array; }
		void consume()(size_t n) { array = array[n .. $]; }
	}
}

package auto pipeFromArray(E)(const(E)[] array)
{
	static assert(isPeekSource!(ArraySource!E));
	static assert(isSource!(ArraySource!E));
	return .pipe!(ArraySource!E)(array.ptr, array.length);
}

unittest {
	auto arr = [ 1, 2, 37, 98, 123, 12313 ];
	auto pl = arr.pipeFromArray.instantiate();
	assert(pl.peek(1) == arr[]);
	assert(pl.peek(123) == arr[]);
	pl.consume(2);
	assert(pl.peek(23) == arr[2 .. $]);
	pl.consume(pl.peek(1).length);
	assert(pl.peek(1).length == 0);
}

private template RangeSource(R) {
	import std.range : ElementType;
	import std.traits;
	alias E = Unqual!(ElementType!R);

	@source!E(Method.pull)
	static struct RangeSource(alias Context, A...) {
		mixin Context!A;
		private R range;

		this(bool dummy, R range) { cast(void) dummy; this.range = range; }

		size_t pull()(E[] buf)
		{
			foreach (i, ref e; buf) {
				if (range.empty)
					return i;
				e = range.front;
				range.popFront();
			}
			return buf.length;
		}
	}
}

package auto pipeFromInputRange(R)(R r)
	if (isInputRange!R)
{
	return .pipe!(RangeSource!R)(false, r);
}

unittest {
	import std.range : iota, hasSlicing, hasLength, isInfinite;

	auto r = iota(6, 12);
	static assert( hasSlicing!(typeof(r)));
	static assert( hasLength!(typeof(r)));
	static assert(!isInfinite!(typeof(r)));
	auto p = r.pipeFromInputRange;
	static assert(isSchema!(typeof(p)));
	static assert(is(p.ElementType == int));
	auto pl = p.instantiate();
	int[4] buf;
	assert(pl.pull(buf[]) == 4);
	assert(buf[] == [6, 7, 8, 9]);
	assert(pl.pull(buf[]) == 2);
	assert(buf[0 .. 2] == [10, 11]);
}

unittest {
	import std.range : repeat, hasSlicing, hasLength, isInfinite;

	auto r = repeat(0xdead);
	static assert( hasSlicing!(typeof(r)));
	static assert(!hasLength!(typeof(r)));
	static assert( isInfinite!(typeof(r)));
	auto pl = r.pipeFromInputRange.instantiate();
	int[5] buf;
	assert(pl.pull(buf[]) == 5);
	assert(buf[] == [0xdead, 0xdead, 0xdead, 0xdead, 0xdead]);
	assert(pl.pull(new int[1234567]) == 1234567);
}

unittest {
	import std.range : generate, take, hasSlicing;

	auto r = generate({ int i = 0; return (){ return i++; }; }()).take(104);
	static assert(!hasSlicing!(typeof(r)));
	auto pl = r.pipeFromInputRange.instantiate();
	int[5] buf;
	assert(pl.pull(buf[]) == 5);
	assert(buf[] == [0, 1, 2, 3, 4]);
	assert(pl.pull(new int[1234567]) == 99);
}

private template RangeSink(R) {
	@sink(Method.push)
	static struct RangeSink(alias Context, A...) {
		mixin Context!A;
		private R range;

		alias E = InputElementType;
		static assert(isOutputRange!(R, E));

		this()(R range) { this.range = range; }

		size_t push()(const(E)[] buf)
		{
			import std.range : put;
			put(range, buf);
			return buf.length;
		}
	}
}

public auto copy(S, R)(auto ref S schema, R outputRange)
	if (isSchema!S && isOutputRange!(R, S.ElementType))
{
	return schema.pipe!(RangeSink!R)(outputRange);
}

unittest {
	import std.array : appender;
	import std.range : iota;

	auto app = appender!(int[]);
	iota(89, 94).pipeFromInputRange.copy(app);
	assert(app.data[] == [89, 90, 91, 92, 93]);
}

template DelegateSource(alias fun, E) {
	@source!E(Method.push)
	struct DelegateSource(alias Context, A...) {
		mixin Context!A;

		void put()(const(E)[] b)
		{
			sink.push(b);
		}

		void run()()
		{
			fun(&this);
		}
	}
}

package auto pipeFromDelegate(E, alias fun)()
{
	return pipe!(DelegateSource!(fun, E));
}

unittest {
	int z = 2;
	auto x = [10, 20, 30].map!(n => n + z);
}

unittest {
	import std.format : formattedWrite;
	import std.array : appender;

	auto app = appender!string;
	/* FIXME:
	Fails if the delegate literal passed to pipeFromDelegate accesses the calling function's context.
	Error: function flod.range.__unittestL186_57.DelegateSource!(__lambda1, char).Stage!(...).DelegateSource
		.run!().run cannot access frame of function flod.range.__unittestL186_57
	*/
	static int a = 42;
	pipeFromDelegate!(char, (orange)
		{
			orange.formattedWrite("first line %d\n", a);
			orange.formattedWrite("formatted %012x line\n", 0xdeadbeef);
		})
		.copy(app);
	assert(app.data == "first line 42\nformatted 0000deadbeef line\n");
}

@sink(Method.pull)
@sink(Method.peek)
package struct ByElement(alias Context, A...) {
	mixin Context!A;
	private alias E = OutputElementType;

	static if (inputMethod == Method.peek) {
		void popFront()() { source.consume(1); }
		@property E front()() { return source.peek(1)[0]; }
		@property bool empty()() { return source.peek(1).length == 0; }
	} else static if (inputMethod == Method.pull) {
		private E[1] current_;
		private bool empty_ = true;

		@property bool empty()()
		{
			if (!empty_)
				return false;
			popFront();
			return empty_;
		}

		@property E front()() { return current_[0]; }

		void popFront()()
		{
			empty_ = source.pull(current_[]) != 1;
		}
	} else {
		static assert(0);
	}
}

unittest {
	auto p = [10, 20, 30].pipe!ByElement;
	assert(!p.empty);
	assert(p.front == 10);
	p.popFront();
	assert(p.front == 20);
}

unittest {
	import std.range : iota;
	auto p = iota(42, 50).pipe!ByElement;
	assert(!p.empty);
	assert(p.front == 42);
	p.popFront();
	assert(p.front == 43);
}

package template Splitter(Separator, size_t peekStep = 128) {
	@sink(Method.peek)
	struct Splitter(alias Context, A...) {
		mixin Context!A;
	private:
		import std.meta : AliasSeq;
		import std.traits : isSomeChar, isIntegral, Unqual;

		static if (isSomeChar!InputElementType)
			alias Char = Unqual!InputElementType;
		else static if (isIntegral!InputElementType && InputElementType.sizeof <= 4)
			alias Char = AliasSeq!(void, char, wchar, void, dchar)[InputElementType.sizeof];
		static assert(is(Char),
			"Only streams of chars or bytes can be read by line, not " ~ InputElementType.stringof);

		const(Char)[] line;
		bool keepSeparator;
		static if (is(Separator : Char)) {
			Char separator;
			bool done;
		}
		else {
			immutable(Char)[] separator;
		}

		public this(Separator)(typeof(null) dummy, Separator separator, bool keep)
		{
			// TODO: convert separator to array without GC allocation
			// TODO: optimize for single-char separator
			import std.conv : to;
			this.keepSeparator = keep;
			static if (is(Separator : Char))
				this.separator = separator;
			else
				this.separator = separator.to!(typeof(this.separator));
			next();
		}

		void next()()
		{
			if (line.length)
				source.consume(line.length);
			line = cast(typeof(line)) source.peek(peekStep);
			static if (is(Separator : Char)) {
				size_t start = 0;
				for (;;) {
					import std.string : indexOf;
					auto i = line[start .. $].indexOf(separator);
					if (i >= 0) {
						line = line[0 .. start + i + 1];
						return;
					}
					start = line.length;
					line = cast(const(Char)[]) source.peek(start + peekStep);
					if (line.length == start) {
						done = true;
						if (line.length == 0)
							line = null;
						return;
					}
				}
			} else {
				for (size_t i = 0; ; i++) {
					if (line.length - i < separator.length) {
						line = cast(typeof(line)) source.peek(line.length + peekStep);
						if (line.length - i < separator.length) {
							separator = null;
							if (line.length == 0)
								line = null;
							return; // we've read everything, and haven't found separator
						}
					}
					if (line[i .. i + separator.length] == separator[]) {
						line = line[0 .. i + separator.length];
						return;
					}
				}
			}
		}

	public:
		@property bool empty()()
		{
			return line is null;
		}

		@property const(Char)[] front()()
		{
			static if (is(Separator : Char))
				return keepSeparator ? line : line[0 .. $ - (done ? 0 : 1)];
			else
				return keepSeparator ? line : line[0 .. $ - separator.length];
		}

		void popFront()()
		{
			static if (is(Separator : Char)) {
				if (done)
					line = null;
				else
					next();
			} else {
				if (separator.length)
					next();
				else
					line = null;
			}
		}
	}
}

unittest {
	import std.string : representation;
	assert("Zażółć gęślą jaźń".pipe!(Splitter!(char, 3))(null, ' ', true)
		.equal(["Zażółć ", "gęślą ", "jaźń"]));
	assert("Zażółć gęślą jaźń".representation.pipe!(Splitter!(char, 3))(null, ' ', true)
		.equal(["Zażółć ", "gęślą ", "jaźń"]));
	assert("Zażółć gęślą jaźń "w.pipe!(Splitter!(dstring, 5))(null, " "d, true)
		.equal(["Zażółć "w, "gęślą "w, "jaźń "w]));
	// map and filter decode the string into a sequence of dchars
	assert("여보세요 세계".map!"a".filter!(a => true).pipe!(Splitter!(string, 2))(null, " ", false)
		.equal(["여보세요"d, "세계"d]));
	assert("Foo\r\nBar\r\nBaz\r\r\n\r\n".pipe!(Splitter!(wstring, 4))(null, "\r\n"w, false)
		.equal(["Foo", "Bar", "Baz\r", ""]));
}

/**
Returns a range that reads from the pipeline one line at a time.

Allowed input element types are built-in character and integer types. The stream is interpreted
as UTF-8, UTF-16 or UTF-32 according to the input element size.
Range elements are arrays of respective built-in character types.

Each `front` is valid only until `popFront` is called. If retention is needed,
a copy must be made using e.g. `idup` or `to!string`.
*/
auto byLine(S, Terminator)(S schema, Terminator terminator = '\n',
	Flag!"keepTerminator" keep_terminator = No.keepTerminator)
	if (isSchema!S && isSomeChar!Terminator)
{
	return schema.pipe!(Splitter!Terminator)(null, terminator, keep_terminator);
}

auto byLine(S, Terminator)(S schema, Terminator terminator,
	Flag!"keepTerminator" keep_terminator = No.keepTerminator)
	if (isSchema!S && isInputRange!Terminator)
{
	return schema.pipe!(Splitter!Terminator)(null, terminator, keep_terminator);
}

///
unittest {
	import flod.adapter : peekPush;
	assert("first\nsecond\nthird\n".byLine.equal(["first", "second", "third"]));
	assert("first\nsecond\nthird".peekPush.byLine.equal(["first", "second", "third"]));
}

unittest {
	// For arrays of chars byLine should just give slices of the original array.
	// This is not a part of the API, but an implementation detail with positive effect
	// on performance.
	auto line = "just one line";
	assert(line.byLine.front is line);

}

unittest {
	import std.conv : to;
	import std.meta : AliasSeq;
	foreach (T; AliasSeq!(string, wstring, dstring)) {
		assert(q"EOF
Prześliczna dzieweczka na spacer raz szła
Gdy noc ją złapała wietrzysta i zła
Być może przestraszył by ziąb i mrok ją
Lecz miałą wszak mufkę prześliczną swą
EOF".to!T.byLine.equal([
				"Prześliczna dzieweczka na spacer raz szła",
				"Gdy noc ją złapała wietrzysta i zła",
				"Być może przestraszył by ziąb i mrok ją",
				"Lecz miałą wszak mufkę prześliczną swą",
			].map!(to!T)));
	}
}

@sink(Method.peek)
private struct PeekByChunk(alias Context, A...) {
	mixin Context!A;
private:
	alias E = InputElementType;

	const(E)[] chunk;
	size_t chunkSize;

public:
	this(size_t chunk_size)
	{
		chunkSize = chunk_size;
		popFront();
	}

	@property bool empty()() { return chunk.length == 0; }

	@property const(E)[] front()() { return chunk; }

	void popFront()()
	{
		if (chunk.length)
			source.consume(chunk.length);
		if (chunkSize) {
			chunk = source.peek(chunkSize);
			if (chunk.length > chunkSize)
				chunk.length = chunkSize;
		} else {
			chunk = source.peek(4096);
		}
	}
}

private template PullByChunk(E) {
	@sink!E(Method.pull)
	struct PullByChunk(alias Context, A...) {
		mixin Context!A;
	private:
		alias E = InputElementType;

		E[] chunk;

	public:
		this(E[] buffer)
		{
			chunk = buffer;
			popFront();
		}

		@property bool empty()() { return chunk.length == 0; }

		@property const(E)[] front()() { return chunk; }

		void popFront()()
		{
			chunk.length = source.pull(chunk);
		}
	}
}

/**
Returns a range that reads from the pipeline one chunk at a time.
*/
auto byChunk(S)(S schema, size_t chunk_size = 0)
	if (isSchema!S)
{
	return schema.pipe!PeekByChunk(chunk_size);
}

/// ditto
auto byChunk(S, E)(S schema, E[] buf)
	if (isSchema!S)
{
	return schema.pipe!(PullByChunk!E)(buf);
}

///
unittest {
	auto arr = [ 42, 41, 40, 39, 38, 37, 36 ];
	assert(arr.byChunk(2).equal([[ 42, 41 ], [ 40, 39 ], [ 38, 37 ], [ 36 ]]));
	int[3] buf;
	assert(arr.byChunk(buf[]).equal([[ 42, 41, 40 ], [ 39, 38, 37 ], [ 36 ]]));
}

template OutputRangeSource(El = void) {
	@source!El(Method.push)
	package struct OutputRangeSource(alias Context, A...) {
		mixin Context!A;

		alias E = OutputElementType;

		void put(const(E)[] elements)
		{
			sink.push(elements);
		}
	}
}

/// A pipe used to start a pipeline to be used as an output range.
@property auto pass(E = void)()
{
	import flod.pipeline : DriveMode;
	return .pipe!(OutputRangeSource!E, DriveMode.source);
}

/// ditto
alias _ = pass;

version(unittest) {
	void testOutputRange(string r)()
	{
		import std.array : appender;
		import std.format : formattedWrite;
		import flod.adapter;
		auto app = appender!string();
		{
			auto or = mixin(r);
			or.formattedWrite("test %d\n", 42);
			or.formattedWrite("%s line\n", "second");
		}
		assert(app.data == "test 42\nsecond line\n");
	}
}

unittest {
	testOutputRange!q{ pass!char.copy(app) };
}

unittest {
	// test if this works also with more drivers
	testOutputRange!q{ pass!char.peekAlloc.pullPush.copy(app) };
}
