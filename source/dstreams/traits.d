/** Templates for determining characteristics of types and symbols at compile-time.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://boost.org/LICENSE_1_0.txt, Boost License 1.0).
 */
module dstreams.traits;


/*
 * Terminals:
 * - Active (1 template argument)
 *   push source  (must know the sink it pushes to)
 *   pull sink    (must know the source it pulls from)
 * - Passive (no template arguments)
 *   pull source  (doesn't need to know anything about the sink that pulls from it)
 *   push sink    (doesn't need to know anything about the source that pushes to it)
 */
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
		size_t pull(T[] b) { return b.length; }
	}
	class CTestUnbufferedPullSource(T) {
		size_t pull(T[] b) { return b.length; }
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

struct PullSinkPushSource(Source, Sink) {
	Source source;
	Sink sink;

	void run()
	{
		ubyte[4096] buf;
		for (;;) {
			size_t n = source.pull(buf[]);
			if (n < buf.length)
				break;
			sink.push(buf[0 .. n]);
		}
	}
}

struct PeekSinkPushSource(Source, Sink) {
	Source source;
	Sink sink;

	void run()
	{
		const(ubyte)[] inbuf;
		for (;;) {
			source.peek(inbuf, 1);
			if (inbuf.length == 0)
				break;
			sink.push(inbuf[]);
			source.consume(inbuf.length);
		}
	}
}

struct PullSinkAllocSource(Source, Sink) {
	Source source;
	Sink sink;

	void run()
	{
		ubyte[4096] buf;
		ubyte[] outbuf;
		for (;;) {
			size_t n = source.pull(buf[]);
			sink.alloc(outbuf, n);
			outbuf[0 .. n] = buf[0 .. n];
			sink.commit(n);
		}
	}
}

struct PeekSinkAllocSource(Source, Sink) {
	Source source;
	Sink sink;

	void run()
	{
		const(ubyte)[] inbuf;
		ubyte[] outbuf;
		for (;;) {
			source.peek(inbuf, 1);
			if (inbuf.length == 0)
				break;
			while (inbuf.length) {
				import std.algorithm : min;
				auto n = min(4096, inbuf.length);
				sink.alloc(outbuf, n);
				outbuf[0 .. n] = inbuf[0 .. n];
				sink.commit(n);
				inbuf = inbuf[n .. $];
			}
		}
	}
}

struct AllocSink
{
	ubyte[] alloc(size_t n)
	{
		ubyte[] a;
		return a;
	}

	void commit(size_t n)
	{
	}
}

struct PullSource
{
	size_t pull(ubyte[])
	{
		return 0;
	}
}

struct PeekSource
{
	const(ubyte)[] peek(size_t n)
	{
		ubyte[] a;
		return a;
	}

	void consume(size_t n)
	{
	}
}

private template isReadOnlyBuffer(T)
{
	import std.traits : isDynamicArray;
	import std.range.primitives : ElementEncodingType;
	enum bool isReadOnlyBuffer =
		   is(T)
		&& isDynamicArray!T
		&& is(typeof({ T t; return t[$ - 1] + t.ptr[t.length - 1]; }()))
		&& is(ElementEncodingType!T)
		&& !is(typeof({ T t; t[0] = ElementEncodingType!T.init; }()))
		&& !is(typeof({ T t; *t.ptr = ElementEncodingType!T.init; }()))
		&& !is(typeof({ T t; t.ptr[0] = ElementEncodingType!T.init; }()));
}

private template isWritableBuffer(T)
{
	import std.traits : isDynamicArray;
	import std.range.primitives : ElementEncodingType;
	enum bool isWritableBuffer =
		is(T)
			&& isDynamicArray!T
			&& is(ElementEncodingType!T)
			&& is(typeof({ T t; ElementEncodingType!T e; t[0] = e; t[$ - 1] = e; *t.ptr = e; t.ptr[t.length - 1] = e; }()));
}

unittest
{
	static assert( isReadOnlyBuffer!(const(ubyte[])));
	static assert(!isReadOnlyBuffer!(const(void[])));
	static assert(!isReadOnlyBuffer!(ubyte[]));
	static assert( isWritableBuffer!(ubyte[]));
	static assert(!isWritableBuffer!(void[]));
	static assert(!isWritableBuffer!(const(ubyte)[]));
}

private template checkParameters(alias Func, size_t index, ParameterRequirements...)
{
	static if (index >= ParameterRequirements.length) {
		enum checkParameters = true;
	} else static if (is(ParameterRequirements[index])) {
		import std.traits : ParameterTypeTuple;
		static if (!is(ParameterRequirements[index] : ParameterTypeTuple!Func[index]))
			pragma(msg, ParameterRequirements[index].stringof, " is not convertible to ", ParameterTypeTuple!Func[index]);
		enum checkParameters =
			is(ParameterRequirements[index] : ParameterTypeTuple!Func[index])
				&& checkParameters!(Func, index + 1, ParameterRequirements);
	} else {
		import std.traits : ParameterTypeTuple;
		alias Req = ParameterRequirements[index];
		static if (!Req!(ParameterTypeTuple!Func[index]))
			pragma(msg, ParameterTypeTuple!Func[index], " does not match ", Req.stringof);
		enum checkParameters =
			Req!(ParameterTypeTuple!Func[index])
				&& checkParameters!(Func, index + 1, ParameterRequirements);
	}
}

private template checkSignature(alias Func, Requirements...) {
	import std.traits : isCallable, arity;
	static if (!isCallable!Func) {
		pragma(msg, Func.stringof, " is not callable");
		enum checkSignature = false;
	} else static if (Requirements.length - 1 != arity!Func) {
		pragma(msg, Func.stringof, " has different # of parameters than expected");
		enum checkSignature = false;
	} else static if (!checkParameters!(Func, 0, Requirements[1 .. $])) {
		pragma(msg, " parameter types do not match");
		enum checkSignature = false;
	} else static if (is(Requirements[0])) {
		import std.traits : ReturnType;
		enum checkSignature = is(ReturnType!Func : Requirements[0]);
		static if (!checkSignature)
			pragma(msg, (&Func).stringof, " return type does not match: ", ReturnType!Func.stringof, " != ", Requirements[0].stringof);
	} else {
		import std.traits : ReturnType;
		alias ReturnTypeRequirement = Requirements[0];
		enum checkSignature = ReturnTypeRequirement!(ReturnType!Func);
		static if (!checkSignature)
			pragma(msg, (&Func).stringof, " return type does not match: ", ReturnType!Func.stringof, ", ", ReturnTypeRequirement.stringof);
	}
}

private template checkSignature(T, string name, Requirements...) {
	import std.traits : hasMember;
	static if (!hasMember!(T, name)) {
		enum checkSignature = false;
	} else {
		enum checkSignature = checkSignature!(__traits(getMember, T.init, name), Requirements);
	}
}

private class GenericNullPullSource {
	size_t pull(T)(T[] buf) { return 0; }
}
private class GenericNullPeekSource {
	void peek(T)(ref const(T)[] buf, size_t n) { buf = null; }
	void consume(size_t n) {}
}
private class GenericNullPushSink {
	void push(T)(const(T)[] buf) {}
}
private class GenericNullAllocSink {
	void alloc(T)(ref T[] buf, size_t n) { buf.length = n; }
	void commit(size_t n) {}
}
private class Empty {}

private template canBeInstantiatedWith(S...) {
	alias T = S[0];
	enum bool canBeInstantiatedWith = !is(T!Empty) && is(T!(S[1 .. $])) && isAggregateType!(T!(S[1 .. $]));
}

template isAllocSource(S...) {
	enum bool isAllocSource = S.length == 1 && (
		   canBeInstantiatedWith!(S[0], GenericNullAllocSink)
		|| canBeInstantiatedWith!(S[0], GenericNullPeekSource, GenericNullAllocSink)
		|| canBeInstantiatedWith!(S[0], GenericNullPullSource, GenericNullAllocSink));
}

template isPushSource(S...) {
	enum bool isPushSource = S.length == 1 && (
		   canBeInstantiatedWith!(S[0], GenericNullPushSink)
		|| canBeInstantiatedWith!(S[0], GenericNullPeekSource, GenericNullPushSink)
		|| canBeInstantiatedWith!(S[0], GenericNullPullSource, GenericNullPushSink));
}

template isPeekSink(S...) {
	enum bool isPeekSink = S.length == 1 && (
		   canBeInstantiatedWith!(S[0], GenericNullPeekSource)
		|| canBeInstantiatedWith!(S[0], GenericNullPeekSource, GenericNullAllocSink)
		|| canBeInstantiatedWith!(S[0], GenericNullPeekSource, GenericNullPushSink));
}

template isPullSink(S...) {
	enum bool isPullSink = S.length == 1 && (
		   canBeInstantiatedWith!(S[0], GenericNullPullSource)
		|| canBeInstantiatedWith!(S[0], GenericNullPullSource, GenericNullAllocSink)
		|| canBeInstantiatedWith!(S[0], GenericNullPullSource, GenericNullPushSink));
}

template PushSinkElementType(Ss...) {
	template PushArgElementType(T) {
		import std.traits : isAggregateType;
		static if (isAggregateType!S && checkSignature!(T, "push", void, isReadOnlyBuffer)) {
			import std.traits : Unqual, ParameterTypeTuple;
			import std.range.primitives : ElementEncodingType;
			alias Impl = Unqual!(ElementEncodingType!(ParameterTypeTuple!(__traits(getMember, T.init, "push"))));
		} else {
			alias Impl = void;
		}
	}
	template Impl() {
		static if (Ss.length = 1) {
			alias S = Ss[0];
			static if (is(S)) {
				alias Impl = PushArgElementType!S;
			} else static if (isPushSource!S) {
				alias Impl = PushArgElementType!(S!GenericPushSink);
			}
		}
		static if (!is(Impl))
			alias Impl = void;
	}
	alias PushSinkElementType = Impl!();
}

template isPushSink(Ss...) {
	enum bool isPushSink = !is(PushSinkElementType!(Ss) == void);
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
				enum bool impl = isMutable!U && is(ReturnType!(S[0].pull) : size_t);
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
	private template impl() {
		static if (S.length != 1) {
			enum bool impl = false;
		} else {
			alias Source = S[0];
			static struct Null {}
			static if (is(Source!Null)) {
				enum bool impl = false;
			} else {
				static struct Sink {
					void push(T)(const(T)[]) {}
				}
				Source!Sink src;
				static if (is(typeof({ src.push(); }())))
					enum bool impl = true;
				else static if (isUnbufferedPushSink!Source && is(typeof(
							{
								alias T = ElementType!Source;
								T[] buf;
								src.push(buf);
							}())))
					enum bool impl = true;
				else static if (isBufferedPushSink!Source && is(typeof(
							{
								auto buf = src.alloc(1);
								src.commit();
							}())))
					enum bool impl = true;
				else
					enum bool impl = false;
			}
		}
	}
	enum bool isBufferedPushSource = impl!();
}

///
template isBufferedPullSink(S...)
{
	// TODO
	enum bool isBufferedPullSink = __traits(isTemplate, S[0]);
}

///
template isUnbufferedPushSource(S...)
{
	// TODO
	enum isUnbufferedPushSource = __traits(isTemplate, S[0]);
}

///
template isUnbufferedPullSink(S...)
{
	// TODO
	enum isUnbufferedPullSink = __traits(isTemplate, S[0]);
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

///
template Why(S...)
{
	import std.traits : isCallable, ParameterTypeTuple, isMutable, ReturnType;
	template _isNotUnbufferedPullSource() {
		static if (S.length != 1) {
			enum string _isNotUnbufferedPullSource = S[0].stringof ~ " is not a type";
		} else static if (!is(S[0] == struct) && !is(S[0] == class) && !is(S[0] == interface)) {
			enum string _isNotUnbufferedPullSource = S[0].stringof ~ " is not a struct, a class nor an interface";
		} else static if (!__traits(hasMember, S[0], "pull")) {
			enum string _isNotUnbufferedPullSource = S[0].stringof ~ " does not have a member named 'pull'";
		} else static if (!isCallable!(S[0].init.pull)) {
			enum string _isNotUnbufferedPullSource = "member 'pull' of type " ~ S[0].stringof ~ " is not callable";
		} else {
			alias Params = ParameterTypeTuple!(S[0].init.pull);
			static if (Params.length != 1) {
				enum string _isNotUnbufferedPullSource = "member 'pull' should accept exactly one argument";
			} else static if (is(Params[0] T : U[], U)) {
				static if (!isMutable!U) {
					enum string _isNotUnbufferedPullSource = "type of array element is not mutable";
				} else static if (!is(ReturnType!(S[0].pull) : size_t)) {
					enum string _isNotUnbufferedPullSource = "return type of member 'pull' is not implicitly convertible to size_t";
				} else {
					enum string _isNotUnbufferedPullSource = "oh wait, it is"; // apparently S is an unbuffered pull sink
				}
			} else {
				enum string _isNotUnbufferedPullSource = "member 'pull''s argument is not an array type";
			}
		}
	}
	///
	enum string isNotUnbufferedPullSource = _isNotUnbufferedPullSource!();
}
