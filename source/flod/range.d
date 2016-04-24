/** Convert ranges to pipelines and pipelines to ranges.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.range;

import std.range : isInputRange, isOutputRange;

import flod.pipeline : pipe, isSchema;
import flod.traits;

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
	auto pl = arr.pipeFromArray.create();
	assert(pl.peek(1) == arr[]);
	assert(pl.peek(123) == arr[]);
	pl.consume(2);
	assert(pl.peek(23) == arr[2 .. $]);
	pl.consume(pl.peek(1).length);
	assert(pl.peek(1).length == 0);
}

private template RangeSource(R) {
	import std.range : ElementType;
	alias E = ElementType!R;

	@source!E(Method.pull)
	struct RangeSource(alias Context, A...) {
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
	auto pl = p.create();
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
	auto pl = r.pipeFromInputRange.create();
	int[5] buf;
	assert(pl.pull(buf[]) == 5);
	assert(buf[] == [0xdead, 0xdead, 0xdead, 0xdead, 0xdead]);
	assert(pl.pull(new int[1234567]) == 1234567);
}

unittest {
	import std.range : generate, take, hasSlicing;

	auto r = generate({ int i = 0; return (){ return i++; }; }()).take(104);
	static assert(!hasSlicing!(typeof(r)));
	auto pl = r.pipeFromInputRange.create();
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
	import std.algorithm : map;
	import std.stdio : writeln;
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
