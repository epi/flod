/** This module defines templates for determining characteristics of stream components at compile time.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://boost.org/LICENSE_1_0.txt, Boost License 1.0).
 */
module flod.traits;

private struct DummyPullSource {
	size_t pull(T)(T[] buf) { return buf.length; }
}

private struct DummyPeekSource {
	const(T)[] peek(T = ubyte)(size_t n) { return new T[n]; };
	void consume(T = ubyte)(size_t n) {}
}

private struct DummyPushSink {
	size_t push(T)(const(T)[] buf) { return buf.length; }
}

private struct DummyAllocSink {
	T[] alloc(T = ubyte)(size_t n) { return new T[n]; };
	void commit(T = ubyte)(size_t n) {}
}

// Workaround for https://issues.dlang.org/show_bug.cgi?id=15623
private bool canInstantiate(alias Template, Parameters...)() {
	return is(Template!Parameters);
//private template canInstantiate(alias Template, Parameters...) {
//	enum canInstantiate = is(Template!Parameters);
}

unittest
{
	static struct HasFoo { void foo() {} }
	static struct NoFoo {}
	static struct CallsFoo(S) {
		S s;
		void bar() { s.foo(); }
	}
	static assert( canInstantiate!(CallsFoo, HasFoo));
	static assert(!canInstantiate!(CallsFoo, NoFoo));
}


private template test(alias req, alias S, Params...) {
	static if (!canInstantiate!(S, Params))
		enum bool test = false;
	else
		enum bool test = req!(S!Params);
}

private template Subs(ulong mask, ReplacementForZeros, Types...) {
	alias What = ReplacementForZeros;
	import std.meta : AliasSeq;
	static if (Types.length == 0)
		alias Subs = AliasSeq!();
	else {
		static if (mask & 1)
			alias Subs = AliasSeq!(Subs!(mask >> 1, What, Types[0 .. $ - 1]), Types[$ - 1]);
		else
			alias Subs = AliasSeq!(Subs!(mask >> 1, What, Types[0 .. $ - 1]), What);
	}
}

unittest
{
	static struct Empty {}
	struct Z(Params...) {}
	alias List = Subs!(0b011011, Empty, int, bool, float, uint, ulong, double);
	static assert(is(Z!List == Z!(Empty, bool, float, Empty, ulong, double)));
}

// test if S!Types can be instantiated and fullfills req
// but fails to do so if any of Types is substituted with empty struct
private template onlyValidFor(alias req, alias S, Types...) {
	static struct Empty {}
	template sub(ulong mask) {
		static if (test!(req, S, Subs!(mask, Empty, Types))) {
			// pragma(msg, req.stringof, " was true also for ", S.stringof, " with ", Subs!(mask, Empty, Types).stringof);
			enum bool sub = false;
		}
		else static if (mask == 0) {
			// pragma(msg, req.stringof, " was true for ", S.stringof, " only with ", Types.stringof);
			enum bool sub = true;
		}
		else {
			// pragma(msg, "ok, cannot ", req.stringof, " for ", S.stringof, " ! ", Subs!(mask, Empty, Types).stringof);
			enum bool sub = sub!(mask - 1);
		}
	}

	enum onlyValidFor = test!(req, S, Types) && sub!((1UL << Types.length) - 2);
}

private template WriteBufferType(alias buf) {
	alias WriteBufferType = typeof({
			alias T = typeof(buf[0]);
			buf[0] = T.init;
			buf[$ - 1] = T.init;
			buf[] = T.init;
			buf[0 .. $] = T.init;
			buf[0 .. $] = new T[buf.length];
			return T.init;
		}());
}

private bool canRun(S)()
{
	return __traits(compiles,
		{
			S x;
			x.run();
		});
}

private bool canStep(S)()
{
	return __traits(compiles,
		{
			S x;
			while (x.step()) {}
		});
}

private bool canPush(S)()
{
	static struct CanPushPOD { uint meaningless; }
	auto getPushPtr(ref S s)
	{
		static if (is(typeof(&s.push!())))
			return &s.push!();
		else static if (is(typeof(&s.push!CanPushPOD)))
			return &s.push!CanPushPOD;
		else static if (is(typeof(&s.push)))
			return &s.push;
		else
			return null;
	}
	return is(typeof(
			{
				S x;
				auto f = getPushPtr(x);
				import std.traits : ParameterTypeTuple;
				alias B = ParameterTypeTuple!(typeof(f))[0];
				alias T = typeof({ B b; return b[0]; }());
				T[] buf;
				return x.push(buf);
			}()) : size_t);
}

private bool canPull(S)()
{
	static struct CanPullPOD { bool dummy; float justForTest; }
	auto getPullPtr(ref S s)
	{
		static if (is(typeof(&s.pull!())))
			return &s.pull!();
		else static if (is(typeof(&s.pull!CanPullPOD)))
			return &s.pull!CanPullPOD;
		else static if (is(typeof(&s.pull)))
			return &s.pull;
		else
			return null;
	}
	return is(typeof(
			{
				S s;
				auto f = getPullPtr(s);
				import std.traits : ParameterTypeTuple;
				alias B = ParameterTypeTuple!(typeof(f))[0];
				alias T = typeof({ B b; return b[0].init; }());
				T[] buf;
				return s.pull(buf);
			}()) : size_t);
}

private bool canAlloc(S)()
{
	static struct CanAllocPOD { uint meaningless; long justForTest; }
	return is(typeof(
			{
				S s;
				auto buf = s.alloc(size_t(1));
				alias T = WriteBufferType!buf;
				s.commit(buf.length);
			}()))
		|| is(typeof(
			{
				S s;
				auto buf = s.alloc(size_t(1));
				alias T = WriteBufferType!buf;
				s.commit!T(buf.length);
			}()))
		|| is(typeof(
			{
				S s;
				auto buf = s.alloc!CanAllocPOD(size_t(1));
				alias T = WriteBufferType!buf;
				s.commit!T(buf.length);
			}()));
}

private bool canPeek(S)()
{
	static struct CanPeekPOD { uint meaningless; short dummy; real justForTest; }
	return is(typeof(
			{
				S s;
				auto buf = s.peek(size_t(1));
				auto el1 = buf[0];
				auto el2 = buf[$ - 1];
				s.consume(buf.length);
			}()))
		|| is(typeof(
			{
				S s;
				auto buf = s.peek(size_t(1));
				auto el1 = buf[0];
				auto el2 = buf[$ - 1];
				alias T = typeof(buf[0]);
				s.consume!T(buf.length);
			}()))
		|| is(typeof({
				S s;
				auto buf = s.peek!CanPeekPOD(size_t(1));
				auto el1 = buf[0];
				auto el2 = buf[$ - 1];
				s.consume!(typeof(el2))(buf.length);
			}()));
}

/// Returns `true` if `Ss` is a source from which data can be read by calling `pull()`.
template isPullSource(Ss...) {
	template impl() {
		static if (Ss.length != 1) {
			enum bool impl = false;
		} else {
			alias S = Ss[0];
			static if (is(S))
				enum bool impl = canPull!S;
			else
				enum bool impl =
					   onlyValidFor!(canPull, S, DummyPullSource)
					|| onlyValidFor!(canPull, S, DummyPeekSource);
		}
	}
	enum bool isPullSource = impl!();
}

unittest
{
	static struct ZeroSource {
		size_t pull(T)(T[] buf)
		{
			T t;
			buf[] = t;
			return buf.length;
		}
	}
	static assert(isPullSource!ZeroSource);
	static struct ZeroUbyteSource {
		size_t pull()(ubyte[] buf)
		{
			buf[] = 0;
			return buf.length;
		}
	}
	static assert(isPullSource!ZeroUbyteSource);
	static struct Forward(S) {
		S source;
		size_t pull(T)(T[] buf) { return source.pull(buf); }
	}
	static assert(isPullSource!Forward);
	static struct PeekForward(S) {
		S source;
		size_t pull(T)(T[] buf)
		{ // not a correct code, just to test the primitives
			auto inbuf = source.peek!T(buf.length); source.consume!T(inbuf.length); return buf.length;
		}
	}
	static assert(isPullSource!PeekForward);
}

/// Returns `true` if `Ss` is a source from which data can be read by calling `peek()` and `consume()`.
template isPeekSource(Ss...) {
	template impl() {
		static if (Ss.length != 1) {
			enum bool impl = false;
		} else {
			alias S = Ss[0];
			static if (is(S))
				enum bool impl = canPeek!S;
			else
				enum bool impl =
					   onlyValidFor!(canPeek, S, DummyPullSource)
					|| onlyValidFor!(canPeek, S, DummyPeekSource);
		}
	}
	enum bool isPeekSource = impl!();
}

unittest
{
	static struct ZeroSource {
		const(ubyte)[] peek(size_t n) { return new ubyte[n]; }
		void consume(size_t n) {};
	}
	static assert(isPeekSource!ZeroSource);
	static struct GenericZeroSource {
		const(T)[] peek(T = ubyte)(size_t n) { return new T[n]; }
		void consume(T)(size_t n) {};
	}
	static assert(isPeekSource!GenericZeroSource);
	static struct TemplateZeroSource {
		const(ubyte)[] peek()(size_t n) { return new ubyte[n]; }
		void consume()(size_t n) {};
	}
	static assert(isPeekSource!TemplateZeroSource);
	static struct PeekSinkSource(Source) {
		Source source;
		auto peek(T)(size_t n) { return source.peek!T(n); }
		void consume(T)(size_t n) { source.consume!T(n); }
	}
	static assert(isPeekSource!PeekSinkSource);
	static struct PullSinkSource(Source) {
		Source source;
		auto peek(T)(size_t n) { auto buf = new T[n]; source.pull!T(buf); return buf; }
		void consume(T)(size_t n) {}
	}
	static assert(isPeekSource!PullSinkSource);
}

/// Returns `true` if `Ss` is a source which writes data by calling `push()`.
template isPushSource(Ss...) {
	template impl() {
		static if (Ss.length != 1) {
			enum bool impl = false;
		} else {
			alias S = Ss[0];
			enum bool impl =
				   onlyValidFor!(canRun,   S, DummyPushSink)
				|| onlyValidFor!(canStep,  S, DummyPushSink)
				|| onlyValidFor!(canPush,  S, DummyPushSink)
				|| onlyValidFor!(canAlloc, S, DummyPushSink)
				|| onlyValidFor!(canRun,   S, DummyPeekSource, DummyPushSink)
				|| onlyValidFor!(canStep,  S, DummyPeekSource, DummyPushSink)
				|| onlyValidFor!(canRun,   S, DummyPullSource, DummyPushSink)
				|| onlyValidFor!(canStep,  S, DummyPullSource, DummyPushSink);
		}
	}
	enum bool isPushSource = impl!();
}

unittest
{
	static struct NullSource(Sink) {
		Sink sink;
		void run()() {
			ubyte[] buf;
			sink.push(buf);
		}
	}
	static assert(isPushSource!NullSource);
}

unittest
{
	static struct NullSource(Sink) {
		Sink sink;
		void run() {
			ubyte[] buf;
			sink.push(buf);
		}
	}
	static assert(isPushSource!NullSource);
}

unittest
{
	static struct NullSource(Sink) {
		Sink sink;
		bool step() {
			ubyte[] buf;
			sink.push(buf);
			return false;
		}
	}
	static assert(isPushSource!NullSource);
}

unittest
{
	static struct Forward(Sink) {
		Sink sink;
		auto push(T)(const(T)[] buf) {
			return sink.push(buf);
		}
	}
	static assert(isPushSource!Forward);
}

unittest
{
	struct MyOwnPOD { double x; }
	static struct Forward(Sink) {
		Sink sink;
		auto push()(const(MyOwnPOD)[] buf) {
			return sink.push(buf);
		}
	}
	static assert(isPushSource!Forward);
}

unittest
{
	struct MyOwnPOD { double x; }
	static struct Forward(Sink) {
		Sink sink;
		auto push(const(MyOwnPOD)[] buf) {
			return sink.push(buf);
		}
	}
	static assert(isPushSource!Forward);
}

unittest
{
	struct MyOwnPOD { double x; }
	static struct Forward(Sink) {
		Sink sink;
		MyOwnPOD[] buf;
		auto alloc(size_t n) {
			buf = new MyOwnPOD[n];
			return buf;
		}
		void commit(size_t s) {
			sink.push(buf[0 .. s]);
		}
	}
	static assert(isPushSource!Forward);
}

unittest
{
	static struct NotASource(Sink) {
		Sink sink;
		void run()() {}
		bool step()() { return false; }
		size_t push(T = ubyte)(const(T)[] buf) { return buf.length; }
		size_t push(const(ulong)[] buf) { return buf.length; }
	}
	static assert(!isPushSource!NotASource);
}

unittest
{
	static struct Driver(Source, Sink) {
		Source source;
		Sink sink;
		void run()() {
			auto buf = source.peek(1);
			sink.push(buf);
			source.consume(buf.length);
		}
	}
	static assert(isPushSource!Driver);
}

unittest
{
	static struct Driver(Source, Sink) {
		Source source;
		Sink sink;
		void run()() {
			ubyte[] buf;
			source.pull(buf);
			sink.push(buf);
		}
	}
	static assert(isPushSource!Driver);
}

unittest
{
	static struct NotADriver(Source, Sink) {
		Source source;
		Sink sink;
		void run()() {
			ubyte[] buf;
			sink.push(buf);
		}
	}
	static assert(!isPushSource!NotADriver);
}

/// Returns `true` if `Ss` is a source which writes data by calling `alloc()` and `commit()`.
template isAllocSource(Ss...) {
	template impl() {
		static if (Ss.length != 1) {
			enum bool isAllocSource = false;
		} else {
			alias S = Ss[0];
			enum bool impl =
				   onlyValidFor!(canRun,   S, DummyAllocSink)
				|| onlyValidFor!(canStep,  S, DummyAllocSink)
				|| onlyValidFor!(canPush,  S, DummyAllocSink)
				|| onlyValidFor!(canAlloc, S, DummyAllocSink)
				|| onlyValidFor!(canRun,   S, DummyPeekSource, DummyAllocSink)
				|| onlyValidFor!(canStep,  S, DummyPeekSource, DummyAllocSink)
				|| onlyValidFor!(canRun,   S, DummyPullSource, DummyAllocSink)
				|| onlyValidFor!(canStep,  S, DummyPullSource, DummyAllocSink);
		}
	}
	enum bool isAllocSource = impl!();
}

/// Returns `true` if `Ss` is a sink to which data can be written by calling `push()`.
template isPushSink(Ss...) {
	template impl() {
		static if (Ss.length != 1) {
			enum bool impl = false;
		} else {
			alias S = Ss[0];
			static if (is(S))
				enum bool impl = canPush!S;
			else
				enum bool impl =
					   onlyValidFor!(canPush, S, DummyPushSink)
					|| onlyValidFor!(canPush, S, DummyAllocSink);
		}
	}
	enum bool isPushSink = impl!();
}

/// Returns `true` if `Ss` is a sink to which data can be written by calling `alloc()` and `commit()`.
template isAllocSink(Ss...) {
	template impl() {
		static if (Ss.length != 1) {
			enum bool impl = false;
		} else {
			alias S = Ss[0];
			static if (is(S))
				enum bool impl = canAlloc!S;
			else
				enum bool impl =
					   onlyValidFor!(canAlloc, S, DummyPushSink)
					|| onlyValidFor!(canAlloc, S, DummyAllocSink);
		}
	}
	enum bool isAllocSink = impl!();
}

unittest
{
	static struct NullSink {
		ubyte[] alloc(size_t n) { return new ubyte[n]; }
		void commit(size_t n) {}
	}
	static assert(isAllocSink!NullSink);
	static struct GenericNullSink {
		T[] alloc(T = ubyte)(size_t n) { return new T[n]; }
		void commit(T)(size_t n) {}
	}
	static assert(isAllocSink!GenericNullSink);
	static struct TemplateAllocSink {
		ubyte[] alloc()(size_t n) { return new ubyte[n]; }
		void commit()(size_t n) {}
	}
	static assert(isAllocSink!TemplateAllocSink);
	static struct AllocSourceSink(Sink) {
		Sink sink;
		auto alloc(T)(size_t n) { return sink.alloc!T(n); }
		void commit(T)(size_t n) { sink.commit!T(n); }
	}
	static assert(isAllocSink!AllocSourceSink);
	static struct PushSourceSink(Sink) {
		Sink sink;
		void[] buf;
		auto alloc(T)(size_t n) { auto b = new T[n]; buf = cast(void[]) b; return b; }
		void commit(T)(size_t n) { sink.push!T(cast(T[]) buf); }
	}
	static assert(isAllocSink!PushSourceSink);
}

/// Returns `true` if `Ss` is a sink which reads data by calling `pull()`.
template isPullSink(Ss...) {
	template impl() {
		static if (Ss.length != 1) {
			enum bool impl = false;
		} else {
			alias S = Ss[0];
			enum bool impl =
				   onlyValidFor!(canRun,  S, DummyPullSource)
				|| onlyValidFor!(canStep, S, DummyPullSource)
				|| onlyValidFor!(canPull, S, DummyPullSource)
				|| onlyValidFor!(canPeek, S, DummyPullSource)
				|| onlyValidFor!(canRun,  S, DummyPullSource, DummyPushSink)
				|| onlyValidFor!(canStep, S, DummyPullSource, DummyPushSink)
				|| onlyValidFor!(canRun,  S, DummyPullSource, DummyAllocSink)
				|| onlyValidFor!(canStep, S, DummyPullSource, DummyAllocSink);
		}
	}
	enum bool isPullSink = impl!();
}

unittest
{
	struct PullSink(Source) {
		Source source;
		bool step()()
		{
			struct A { bool foo; }
			auto buf = new A[100];
			auto n = source.pull(buf);
			return false;
		}
	}
	static assert(isPullSink!PullSink);
	struct PullFilter(Source) {
		Source source;
		auto pull(T)(T[] b) { return source.pull(b); }
	}
	static assert(isPullSink!PullSink);
	//TODO: more tests
}

/// Returns `true` if `Ss` is a sink which reads data by calling `peek()` and `consume()`.
template isPeekSink(Ss...) {
	template impl() {
		static if (Ss.length != 1) {
			enum bool impl = false;
		} else {
			alias S = Ss[0];
			enum bool impl =
				   onlyValidFor!(canRun,  S, DummyPeekSource)
				|| onlyValidFor!(canStep, S, DummyPeekSource)
				|| onlyValidFor!(canPull, S, DummyPeekSource)
				|| onlyValidFor!(canPeek, S, DummyPeekSource)
				|| onlyValidFor!(canRun,  S, DummyPeekSource, DummyPushSink)
				|| onlyValidFor!(canStep, S, DummyPeekSource, DummyPushSink)
				|| onlyValidFor!(canRun,  S, DummyPeekSource, DummyAllocSink)
				|| onlyValidFor!(canStep, S, DummyPeekSource, DummyAllocSink);
		}
	}
	enum bool isPeekSink = impl!();
}

unittest
{
	struct PeekSink(Source) {
		Source source;
		void run()() { auto b = source.peek(10); source.consume(b.length); }
	}
	static assert(isPeekSink!PeekSink);
	struct PeekFilter(Source) {
		Source source;
		auto peek(T)(size_t n) { return source.peek(n); }
		auto consume(T)(size_t n) { return source.consume!T(n); }
	}
	static assert(isPeekSink!PeekFilter);
	//TODO: more tests
}

/** Returns `true` if `Ss[0]` is a source and `Ss[1]` is a sink and they both use the same
 *  method of passing data.
 */
template areCompatible(Ss...) if (Ss.length == 2) {
	enum areCompatible =
		   (isPeekSource!(Ss[0]) && isPeekSink!(Ss[1]))
		|| (isPullSource!(Ss[0]) && isPullSink!(Ss[1]))
		|| (isAllocSource!(Ss[0]) && isAllocSink!(Ss[1]))
		|| (isPushSource!(Ss[0]) && isPushSink!(Ss[1]));
}

/// Returns `true` if `Ss` is a source of any kind.
template isSource(Ss...) {
	enum isSource = isPeekSource!Ss || isPullSource!Ss || isAllocSource!Ss || isPushSource!Ss;
}

/// Returns `true` if `Ss` is a sink of any kind.
template isSink(Ss...) {
	enum isSink = isPeekSink!Ss || isPullSink!Ss || isAllocSink!Ss || isPushSink!Ss;
}

/// Returns `true` if `Ss` is a source but not a sink.
template isSourceOnly(Ss...) {
	enum isSourceOnly = isSource!Ss && !isSink!Ss;
}

/// Returns `true` if `Ss` is a sink but not a source.
template isSinkOnly(Ss...) {
	enum isSinkOnly = !isSource!Ss && isSink!Ss;
}

template isStreamComponent(Ss...) {
	enum isStreamComponent = isSource!Ss || isSink!Ss;
}