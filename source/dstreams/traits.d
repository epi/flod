/** Templates for determining characteristics of types and symbols at compile-time.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://boost.org/LICENSE_1_0.txt, Boost License 1.0).
 */
module dstreams.traits;

version(unittest) private {
	struct Foo {}
	struct TestBufferedPullSource(T) {
		const(T)[] peek(size_t n) { return new T[](n); }
		void consume(size_t n) {}
	}
	class CTestBufferedPullSource(T) {
		const(T)[] peek(size_t n) { return new T[](n); }
		void consume(size_t n) {}
	}
	struct TestUnbufferedPullSource(T) {
		void pull(T[] b) {}
	}
	class CTestUnbufferedPullSource(T) {
		void pull(T[] b) {}
	}
	struct TestBufferedPushSink(T) {
		T[] alloc(size_t n) { return new T[](n); }
		void commit(size_t n) {}
	}
	class CTestBufferedPushSink(T) {
		T[] alloc(size_t n) { return new T[](n); }
		void commit(size_t n) {}
	}
	struct TestUnbufferedPushSink(T) {
		void push(T[] buf) {}
	}
	class CTestUnbufferedPushSink(T) {
		void push(T[] buf) {}
	}
}

///
template isCopyable(S)
{
	enum bool isCopyable = is(typeof({ foreach (a; [S.init]) {} }()));
}

unittest
{
	static struct B {
		@disable this(this);
	}
	static assert( isCopyable!Foo);
	static assert(!isCopyable!B);
}

///
template isBufferedPullSource(S...)
{
	enum isBufferedPullSource =
		   S.length == 1
		&& is(S[0])
		&& is(typeof({
			S[0] s;
			auto b = s.peek(size_t(1));
			auto c = b[0];
			auto d = b[$ - 1];
			s.consume(b.length); }()));
}

unittest
{
	static assert(!isBufferedPullSource!());
	static assert(!isBufferedPullSource!int);
	static assert(!isBufferedPullSource!12);
	static assert(!isBufferedPullSource!Foo);
	static assert(!isBufferedPullSource!(TestBufferedPullSource!ubyte, ""));
	static assert( isBufferedPullSource!(TestBufferedPullSource!ubyte));
	static assert(!isBufferedPullSource!(CTestBufferedPullSource!ubyte, ""));
	static assert( isBufferedPullSource!(CTestBufferedPullSource!ubyte));
}

/** Extracts the element type from a buffered pull source S
 *
 * Example:
 * ----
 * BufPullSrc s;
 * alias T = ElementType!(typeof(s));
 * const(T)[] buf = s.peek(4096);
 * ----
 */
template ElementType(S...)
	if (isBufferedPullSource!S)
{
	import std.traits : Unqual;
	alias ElementType = Unqual!(typeof({ S[0] s; return s.peek(size_t(1))[0]; }()));
}

unittest
{
	static assert(is(ElementType!(TestBufferedPullSource!int) == int));
	static assert(is(ElementType!(TestBufferedPullSource!ubyte) == ubyte));
	static assert(is(ElementType!(TestBufferedPullSource!Foo) == Foo));
	static assert(is(ElementType!(CTestBufferedPullSource!int) == int));
	static assert(is(ElementType!(CTestBufferedPullSource!ubyte) == ubyte));
	static assert(is(ElementType!(CTestBufferedPullSource!Foo) == Foo));
}

///
template isUnbufferedPullSource(S...)
{
	private template impl() {
		import std.traits : isCallable, ReturnType, ParameterTypeTuple, isDynamicArray, isMutable, hasMember;
		static if (S.length != 1 || !is(S[0]) || !__traits(hasMember, S[0], "pull") || !isCallable!(S[0].init.pull)) {
			enum bool impl = false;
		} else {
			alias Params = ParameterTypeTuple!(S[0].init.pull);
			static if (Params.length != 1)
				enum bool impl = false;
			else static if (is(Params[0] T : U[], U))
				enum bool impl = isMutable!U && is(ReturnType!(S[0].pull) == void);
			else
				enum bool impl = false;
		}
	}
	enum bool isUnbufferedPullSource = impl!();
}

unittest
{
	static struct Foo {}
	static assert(!isUnbufferedPullSource!());
	static assert(!isUnbufferedPullSource!int);
	static assert(!isUnbufferedPullSource!Foo);
	static assert(!isUnbufferedPullSource!12);
	static assert(!isUnbufferedPullSource!(TestUnbufferedPullSource!int, TestUnbufferedPullSource!int));
	static assert(!isUnbufferedPullSource!TestUnbufferedPullSource);
	static assert( isUnbufferedPullSource!(TestUnbufferedPullSource!ubyte));
	static assert( isUnbufferedPullSource!(TestUnbufferedPullSource!Foo));
	static assert(!isUnbufferedPullSource!CTestUnbufferedPullSource);
	static assert( isUnbufferedPullSource!(CTestUnbufferedPullSource!ubyte));
	static assert( isUnbufferedPullSource!(CTestUnbufferedPullSource!Foo));
}

///
template ElementType(S...)
	if (isUnbufferedPullSource!S)
{
	private template Impl() {
		import std.traits : ParameterTypeTuple, Unqual;
		static if (is(ParameterTypeTuple!(S[0].init.pull)[0] T : U[], U))
			alias Impl = U;
		else
			static assert(false);
	}
	alias ElementType = Impl!();
}

unittest
{
	static assert( is(ElementType!(TestUnbufferedPullSource!int) == int));
	static assert( is(ElementType!(TestUnbufferedPullSource!ubyte) == ubyte));
	static assert( is(ElementType!(TestUnbufferedPullSource!Foo) == Foo));
	static assert(!is(ElementType!(TestUnbufferedPullSource!ubyte) == byte));
	static assert( is(ElementType!(CTestUnbufferedPullSource!int) == int));
	static assert( is(ElementType!(CTestUnbufferedPullSource!ubyte) == ubyte));
	static assert( is(ElementType!(CTestUnbufferedPullSource!Foo) == Foo));
	static assert(!is(ElementType!(CTestUnbufferedPullSource!ubyte) == byte));
}

///
template isBufferedPushSink(S...)
{
	import std.range : ElementType;
	enum bool isBufferedPushSink = S.length == 1 && is(S[0])
		&& is(typeof({
				S[0] s;
				auto b = s.alloc(size_t(1));
				b[$ - 1] = ElementType!(typeof(b)).init;
				s.commit(b.length);
			}()));
}

unittest
{
	static assert(!isBufferedPushSink!());
	static assert(!isBufferedPushSink!Foo);
	static assert( isBufferedPushSink!(TestBufferedPushSink!int));
	static assert( isBufferedPushSink!(CTestBufferedPushSink!uint));
	static assert(!isBufferedPushSink!(CTestBufferedPushSink!(const(uint))));
}

///
template ElementType(S...)
	if (isBufferedPushSink!S)
{
	import std.range : ET = ElementType;
	alias ElementType = ET!(typeof(S[0].init.alloc(size_t(1))));
}

unittest
{
	static assert(is(ElementType!(TestBufferedPushSink!int) == int));
	static assert(is(ElementType!(CTestBufferedPushSink!ubyte) == ubyte));
	static assert(is(ElementType!(CTestBufferedPushSink!Foo) == Foo));
}

///
template isUnbufferedPushSink(S...)
{
	private template impl() {
		import std.traits : isCallable, ReturnType, ParameterTypeTuple, isDynamicArray, isMutable, hasMember;
		static if (S.length != 1 || !is(S[0]) || !__traits(hasMember, S[0], "push") || !isCallable!(S[0].init.push)) {
			enum bool impl = false;
		} else {
			alias Params = ParameterTypeTuple!(S[0].init.push);
			static if (Params.length != 1)
				enum bool impl = false;
			else static if (is(Params[0] T : U[], U))
				enum bool impl = !isMutable!U && is(ReturnType!(S[0].init.push) == void);
			else
				enum bool impl = false;
		}
	}
	enum bool isUnbufferedPushSink = impl!();
}

unittest
{
	static assert(!isUnbufferedPushSink!());
	static assert(!isUnbufferedPushSink!Foo);
	static assert( isUnbufferedPushSink!(TestUnbufferedPushSink!(const(int))));
	static assert( isUnbufferedPushSink!(CTestUnbufferedPushSink!(const(uint))));
	static assert(!isUnbufferedPushSink!(TestUnbufferedPushSink!uint));
	static assert(!isUnbufferedPushSink!(CTestUnbufferedPushSink!ubyte));
}

///
template ElementType(S...)
	if (isUnbufferedPushSink!S)
{
	import std.traits : ParameterTypeTuple, Unqual;
	import std.range : ET = ElementType;
	alias ElementType = Unqual!(ET!(ParameterTypeTuple!(S[0].init.push)[0]));
}

unittest
{
	static assert(is(ElementType!(TestUnbufferedPushSink!(const(int))) == int));
	static assert(is(ElementType!(CTestUnbufferedPushSink!(const(Foo))) == Foo));
}

///
template isStreamComponent(S...)
{
	enum bool isStreamComponent =
		isBufferedPushSink!S || isUnbufferedPushSink!S || isBufferedPullSource!S || isUnbufferedPullSource!S;
}

///
template isBufferedPushSource(S...)
{
	enum bool isBufferedPushSource = false;
}

///
template isBufferedPullSink(S...)
{
	enum bool isBufferedPullSink = false;
}

///
template isUnbufferedPushSource(S...)
{
	enum isUnbufferedPushSource = false;
}

///
template isUnbufferedPullSink(S...)
{
	enum isUnbufferedPullSink = false;
}

///
template isSource(S...)
{
	enum bool isSource =
		   isBufferedPullSource!S
		|| isBufferedPushSource!S
		|| isUnbufferedPullSource!S
		|| isUnbufferedPushSource!S;
}

///
template isSink(S...)
{
	enum bool isSink =
		   isBufferedPullSink!S
		|| isBufferedPushSink!S
		|| isUnbufferedPullSink!S
		|| isUnbufferedPushSink!S;
}
