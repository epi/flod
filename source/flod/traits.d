/** This module defines templates for determining characteristics of stream components at compile time.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://boost.org/LICENSE_1_0.txt, Boost License 1.0).
 */
module flod.traits;

import flod.meta : isType, ReplaceWithMask, str;

// test if S!Types can be instantiated and satisfies req
// but fails to do so if any of Types is substituted with empty struct
private template requiresSpecificTypes(alias req, alias S, Types...) {
	template test(Params...) {
		static if (!isType!(S, Params))
			enum bool test = false;
		else
			enum bool test = req!(S!Params);
	}

	static struct Empty {}
	template sub(ulong mask) {
		static if (test!(ReplaceWithMask!(mask, Empty, Types))) {
			//pragma(msg, req.stringof, " was true also for ", S.stringof, " with ", ReplaceWithMask!(mask, Empty, Types).stringof);
			enum bool sub = false;
		}
		else static if (mask == 0) {
			//pragma(msg, req.stringof, " was true for ", S.stringof, " only with ", Types.stringof);
			enum bool sub = true;
		}
		else {
			//pragma(msg, "ok, cannot ", req.stringof, " for ", S.stringof, "!", ReplaceWithMask!(mask, Empty, Types).stringof);
			enum bool sub = sub!(mask - 1);
		}
	}

	static if (!test!Types)
		enum requiresSpecificTypes = false;
	else
		enum requiresSpecificTypes = sub!((1UL << Types.length) - 2);
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

// Defines a unique POD struct type
private struct SomePOD(string cookie = __FILE__ ~ ":" ~ __LINE__.stringof) { string meaningless = cookie; }

///
template isRunnable(S) {
	enum bool isRunnable = __traits(compiles, checkRunnable!S);
}

bool checkRunnable(S)()
{
	S s;
	s.run();
	return true;
}

bool checkRunnable(alias S, Args...)()
	if (__traits(isTemplate, S))
{
	alias T = S!Args;
	T t;
	t.run();
	assert(requiresSpecificTypes!(isRunnable, S, Args), str!S ~ " doesn't depend on " ~ str!Args);
	return true;
}

private template PushElementType(S, Args...) {
	template ElementType(Func) {
		import std.traits : Unqual;
		alias ElementType = Unqual!(typeof({
				S x;
				import std.traits : ParameterTypeTuple;
				alias B = ParameterTypeTuple!Func[0];
				alias T = typeof({ B b; return b[0]; }());
				const(T)[] buf;
				size_t s = x.push(buf);
				return buf[0];
			}()));
	}
	static if (is(typeof(Args[0]) == typeof(null))) {
		alias PushElementType = ElementType!(typeof({ S s; return &s.push; }()));
	} else {
		alias PushElementType = ElementType!(typeof({ S s; return &s.push!Args; }()));
	}
}

///
template FixedPushType(S) {
	static if (!is(PushElementType!(S, SomePOD!"FixedPushType"))) {
		static if (is(PushElementType!(S) T))
			alias FixedPushType = T;
		else static if (is(PushElementType!(S, null) U))
			alias FixedPushType = U;
	}
}

private template PullElementType(S, Args...) {
	template ElementType(Func) {
		alias ElementType = typeof({
				S s;
				import std.traits : ParameterTypeTuple;
				alias B = ParameterTypeTuple!Func[$ - 1];
				alias T = typeof({ B b; return b[0].init; }());
				T[] buf;
				size_t n = s.pull(buf);
				return buf[0];
			}());
	}
	static if (is(typeof(Args[0]) == typeof(null))) {
		alias PullElementType = ElementType!(typeof({ S s; return &s.pull; }));
	} else {
		alias PullElementType = ElementType!(typeof({ S s; return &s.pull!Args; }()));
	}
}

///
template FixedPullType(S) {
	static if (!is(PullElementType!(S, SomePOD!"FixedPullType"))) {
		static if (is(PullElementType!(S) T))
			alias FixedPullType = T;
		else static if (is(PullElementType!(S, null) U))
			alias FixedPullType = U;
	}
}

private template AllocElementType(S, bool templateCommit, AllocArgs...) {
	alias AllocElementType = typeof({
			S s;
			static if (AllocArgs.length)
				auto buf = s.alloc!AllocArgs(size_t(1));
			else
				auto buf = s.alloc(size_t(1));
			alias T = WriteBufferType!buf;
			static if (templateCommit)
				size_t n = s.commit!T(buf.length);
			else
				size_t n = s.commit(buf.length);
			return T.init;
		}());
}

///
template DefaultAllocType(S) {
	import std.traits : Unqual;
	static if (is(AllocElementType!(S, false) T))
		alias DefaultAllocType = Unqual!T;
	else static if (is(AllocElementType!(S, true) U))
		alias DefaultAllocType = Unqual!U;
}

///
template FixedAllocType(S) {
	static if (!is(AllocElementType!(S, true, SomePOD!())))
		alias FixedAllocType = DefaultAllocType;
}

private template PeekElementType(S, bool templateConsume, PeekArgs...) {
	alias PeekElementType = typeof({
			S s;
			static if (PeekArgs.length)
				auto buf = s.peek!PeekArgs(size_t(1));
			else
				auto buf = s.peek(size_t(1));
			auto el1 = buf[0];
			auto el2 = buf[$ - 1];
			alias T = typeof(buf[0]);
			static if (templateConsume)
				s.consume!T(buf.length);
			else
				s.consume(buf.length);
			return T.init;
		}());
}

///
template DefaultPeekType(S) {
	import std.traits : Unqual;
	static if (is(PeekElementType!(S, false) T))
		alias DefaultPeekType = Unqual!T;
	else static if (is(PeekElementType!(S, true) U))
		alias DefaultPeekType = Unqual!U;
}

///
template FixedPeekType(S) {
	static if (!is(PeekElementType!(S, true, SomePOD!"FixedPeekType")))
		alias FixedPeekType = DefaultPeekType;
}

private template testStage(alias testType, alias testTemplate, S...) {
	static if (S.length != 1) {
		enum testStage = false;
	} else {
		alias Z = S[0];
		static if (is(Z) && !is(typeof(testType) : typeof(null)))
			enum bool testStage = testType!Z;
		else static if (__traits(isTemplate, Z) && !is(typeof(testTemplate) : typeof(null)))
			enum bool testStage = testTemplate!Z;
		else
			enum bool testStage = false;
	}
}
/// Returns `true` if `S` is a source from which data can be read by calling `pull()`.
template isPullSource(S...) {
	template templ(alias Z) {
		enum bool templ =
			   requiresSpecificTypes!(isPullable, Z, PullSource)
			|| requiresSpecificTypes!(isPullable, Z, PeekSource);
	}
	enum bool isPullSource = testStage!(isPullable, templ, S);;
}

unittest {
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

/// Returns `true` if `S` is a source from which data can be read by calling `peek()` and `consume()`.
template isPeekSource(S...) {
	template templ(alias Z) {
	enum bool templ =
		   requiresSpecificTypes!(isPeekable, Z, PullSource)
		|| requiresSpecificTypes!(isPeekable, Z, PeekSource);
	}
	enum bool isPeekSource = testStage!(isPeekable, templ, S);
}

unittest {
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

/**
Returns `true` if `S` is a source which writes data by calling `push()`.
Bugs: Always returns `false` for a non-copyable nested `struct`.
*/
template isPushSource(S...) {
	template isPushSourceTempl(alias Z) {
		enum isPushSourceTempl =
			   requiresSpecificTypes!(isRunnable,  Z, PushSink)
			|| requiresSpecificTypes!(isPushable,  Z, PushSink)
			|| requiresSpecificTypes!(isAllocable, Z, PushSink)
			|| requiresSpecificTypes!(isRunnable,  Z, PeekSource, PushSink)
			|| requiresSpecificTypes!(isRunnable,  Z, PullSource, PushSink);
	}
	enum bool isPushSource = testStage!(null, isPushSourceTempl, S);
}

unittest {
	static struct NullSource(Sink) {
		Sink sink;
		void run()() {
			ubyte[] buf;
			sink.push(buf);
		}
	}
	static assert(isPushSource!NullSource);
}

unittest {
	static struct NullSource(Sink) {
		Sink sink;
		void run() {
			ubyte[] buf;
			sink.push(buf);
		}
	}
	static assert(isPushSource!NullSource);
}

unittest {
	static struct Forward(Sink) {
		Sink sink;
		auto push(T)(const(T)[] buf) {
			return sink.push(buf);
		}
	}
	static assert(isPushSource!Forward);
}

unittest {
	static struct Forward(Sink) {
		Sink sink;
		auto push()(const(SomePOD!())[] buf) {
			return sink.push(buf);
		}
	}
	static assert(isPushSource!Forward);
}

unittest {
	static struct Forward(Sink) {
		Sink sink;
		auto push(const(SomePOD!())[] buf) {
			return sink.push(buf);
		}
	}
	static assert(isPushSource!Forward);
}

unittest {
	alias P = SomePOD!();
	static struct Forward(Sink) {
		Sink sink;
		P[] buf;
		auto alloc(size_t n) {
			buf = new P[n];
			return buf;
		}
		size_t commit(size_t s) {
			return sink.push(buf[0 .. s]);
		}
	}
	static assert(isPushSource!Forward);
}

unittest {
	static struct NotASource(Sink) {
		Sink sink;
		void run()() {}
		size_t push(T = ubyte)(const(T)[] buf) { return buf.length; }
		size_t push(const(ulong)[] buf) { return buf.length; }
	}
	static assert(!isPushSource!NotASource);
}

unittest {
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

unittest {
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

unittest {
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

/// Returns `true` if `S` is a source which writes data by calling `alloc()` and `commit()`.
template isAllocSource(S...) {
	template isAllocSourceTempl(alias Z) {
		enum isAllocSourceTempl =
			   requiresSpecificTypes!(isRunnable,  Z, AllocSink)
			|| requiresSpecificTypes!(isPushable,  Z, AllocSink)
			|| requiresSpecificTypes!(isAllocable, Z, AllocSink)
			|| requiresSpecificTypes!(isRunnable,  Z, PeekSource, AllocSink)
			|| requiresSpecificTypes!(isRunnable,  Z, PullSource, AllocSink);
	}
	enum isAllocSource = testStage!(null, isAllocSourceTempl, S);
}

unittest {
	static assert(!isAllocSource!int);
}

/// Returns `true` if `S` is a sink to which data can be written by calling `push()`.
template isPushSink(S...) {
	template templ(alias Z) {
		enum bool templ =
			   requiresSpecificTypes!(isPushable, Z, PushSink)
			|| requiresSpecificTypes!(isPushable, Z, AllocSink);
	}
	enum bool isPushSink = testStage!(isPushable, templ, S);
}

unittest {
	static assert(!isPushSink!int);
}

/// Returns `true` if `S` is a sink to which data can be written by calling `alloc()` and `commit()`.
template isAllocSink(S...) {
	private template templ(alias S) {
		enum bool templ =
			   requiresSpecificTypes!(isAllocable, S, PushSink)
			|| requiresSpecificTypes!(isAllocable, S, AllocSink);
	}
	enum bool isAllocSink = testStage!(isAllocable, templ, S);
}

unittest {
	static assert(!isAllocSink!int);
	static struct NullSink {
		ubyte[] alloc(size_t n) { return new ubyte[n]; }
		size_t commit(size_t n) { return n; }
	}
	static assert(isAllocSink!NullSink);
	static struct GenericNullSink {
		T[] alloc(T = ubyte)(size_t n) { return new T[n]; }
		size_t commit(T)(size_t n) { return n; }
	}
	static assert(isAllocSink!GenericNullSink);
	static struct TemplateAllocSink {
		ubyte[] alloc()(size_t n) { return new ubyte[n]; }
		size_t commit()(size_t n) { return n; }
	}
	static assert(isAllocSink!TemplateAllocSink);
	static struct AllocSourceSink(Sink) {
		Sink sink;
		auto alloc(T)(size_t n) { return sink.alloc!T(n); }
		size_t commit(T)(size_t n) { return sink.commit!T(n); }
	}
	static assert(isAllocSink!AllocSourceSink);
	static struct PushSourceSink(Sink) {
		Sink sink;
		void[] buf;
		auto alloc(T)(size_t n) { auto b = new T[n]; buf = cast(void[]) b; return b; }
		size_t commit(T)(size_t n) { return sink.push!T(cast(T[]) buf); }
	}
	static assert(isAllocSink!PushSourceSink);
}

/// Returns `true` if `S` is a sink which reads data by calling `pull()`.
template isPullSink(S...) {
	private template isPushSinkTempl(alias Z) {
		enum isPushSinkTempl =
			   requiresSpecificTypes!(isRunnable, Z, PullSource)
			|| requiresSpecificTypes!(isPullable, Z, PullSource)
			|| requiresSpecificTypes!(isPeekable, Z, PullSource)
			|| requiresSpecificTypes!(isRunnable, Z, PullSource, PushSink)
			|| requiresSpecificTypes!(isRunnable, Z, PullSource, AllocSink);
	}
	enum isPullSink = testStage!(null, isPushSinkTempl, S);
}

unittest {
	struct PullSink(Source) {
		Source source;
		void run()()
		{
			struct A { bool foo; }
			auto buf = new A[100];
			auto n = source.pull(buf);
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

/// Returns `true` if `S` is a sink which reads data by calling `peek()` and `consume()`.
template isPeekSink(S...) {
	private template isPeekSinkTempl(alias Z) {
		import std.range : isInputRange;
		enum isPeekSinkTempl =
			   requiresSpecificTypes!(isInputRange, Z, PeekSource)
			|| requiresSpecificTypes!(isRunnable,   Z, PeekSource)
			|| requiresSpecificTypes!(isPullable,   Z, PeekSource)
			|| requiresSpecificTypes!(isPeekable,   Z, PeekSource)
			|| requiresSpecificTypes!(isRunnable,   Z, PeekSource, PushSink)
			|| requiresSpecificTypes!(isRunnable,   Z, PeekSource, AllocSink);
	}
	enum bool isPeekSink = testStage!(null, isPeekSinkTempl, S);
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

/** Returns `true` if `S[0]` is a source and `S[1]` is a sink and they both use the same
 *  method of passing data.
 */
template areCompatible(S...) if (S.length == 2) {
	enum areCompatible =
		   (isPeekSource!(S[0]) && isPeekSink!(S[1]))
		|| (isPullSource!(S[0]) && isPullSink!(S[1]))
		|| (isAllocSource!(S[0]) && isAllocSink!(S[1]))
		|| (isPushSource!(S[0]) && isPushSink!(S[1]));
}

///
template isPassiveSource(S...) {
	enum isPassiveSource = isPeekSource!S || isPullSource!S;
}

///
template isActiveSource(S...) {
	enum isActiveSource = isPushSource!S || isAllocSource!S;
}

///
template isPassiveSink(S...) {
	enum isPassiveSink = isPushSink!S || isAllocSink!S;
}

///
template isActiveSink(S...) {
	enum isActiveSink = isPullSink!S || isPeekSink!S;
}

/// Returns `true` if `S` is a source of any kind.
template isSource(S...) {
	enum isSource = isPassiveSource!S || isActiveSource!S;
}

/// Returns `true` if `S` is a sink of any kind.
template isSink(alias S) {
	enum isSink = isPassiveSink!S || isActiveSink!S;
}

/// Returns `true` if `S` is a source but not a sink.
template isSourceOnly(S...) {
	enum isSourceOnly = isSource!S && !isSink!S;
}

/// Returns `true` if `S` is a sink but not a source.
template isSinkOnly(S...) {
	enum isSinkOnly = !isSource!S && isSink!S;
}

/// Returns `true` if `S` is a source or a sink.
template isStage(S...) {
	enum isStage = isSource!S || isSink!S;
}

/**
Aliases to the element type can be accepted by both `Source` and `Sink` stages.
If both can work on any types and do not specify defaults, `CommonType` aliases to `DefaultType`.
*/
template CommonType(alias Source, alias Sink, DefaultType = ubyte)
	if (isPassiveSource!Source || isPassiveSink!Sink)
{
	static if (is(FixedPullType!Source F)) {
		alias FixedSourceType = F;
	} else static if (is(FixedPeekType!Source G)) {
		alias FixedSourceType = G;
	}
	static if (is(FixedPushType!Source F)) {
		alias FixedSinkType = F;
	} else static if (is(FixedAllocType!Source G)) {
		alias FixedSinkType = G;
	}
	static if (is(DefaultPeekType!Source F))
		alias DefaultSourceType = F;
	static if (is(DefaultPeekType!Sink F))
		alias DefaultSinkType = F;

	static if (is(FixedSourceType)) {
		static if (!is(FixedSinkType))
			alias CommonType = FixedSourceType;
		else static if (is(FixedSourceType : FixedSinkType))
			alias CommonType = FixedSourceType;
		else
			static assert(false, "No common type for " ~ str!Source ~ " (" ~ str!FixedSourceType
				~ ") and " ~ str!Sink ~ " (" ~ str!FixedSinkType ~ ")");
	} else {
		static if (is(FixedSinkType))
			alias CommonType = FixedSinkType;
		else static if (is(DefaultSourceType))
			alias CommonType = DefaultSourceType;
		else static if (is(DefaultSinkType))
			alias CommonType = DefaultSinkType;
		else
			alias CommonType = DefaultType;
	}
}

// used in 2-argument `implements` to mark a lack of source end or sink end in the tested filter
private template None() {}

/** Defines the interface of a peek-sink.
 *
 * Note that the interface in this case includes
 * not only the public members `source` and `run`, but also that a peek-sink must call
 * its source's member functions `peek` and `consume`.
 */
@implements!(PeekSink, PeekSink) // sanity check
struct PeekSink(Source) {
	Source source;
	void run()() {
		auto buf = source.peek(1);
		source.consume(buf.length);
	}
}

/** Defines the interface of a pull-sink.
 *
 * Note that the interface in this case includes
 * not only the public members `source` and `run`, but also that a pull-sink must call
 * its source's member function `pull`.
 */
@implements!(PullSink, PullSink) // sanity check
struct PullSink(Source) {
	Source source;
	void run()() {
		alias T = SourceElementType!Source;
		T[1] buf;
		size_t n = source.pull(buf[]);
	}
}

/** Defines the interface of a push-source.
 *
 * Note that the interface in this case includes
 * not only the public members `sink` and `run`, but also that a push-source must call
 * its sink's member function `push`.
 */
@implements!(PushSource, PushSource) // sanity check
struct PushSource(Sink) {
	Sink sink;
	void run()() {
		alias T = SinkElementType!Sink;
		T[1] buf;
		size_t n = sink.push(buf[]);
	}
}

/** Defines the interface of an alloc-source.
 *
 * Note that the interface in this case includes
 * not only the public members `sink` and `run`, but also that an alloc-source must call
 * its sink's member functions `alloc` and `commit`.
 */
@implements!(AllocSource, AllocSource) // sanity check
struct AllocSource(Sink) {
	Sink sink;
	void run()() {
		auto buf = sink.alloc(10);
		buf[] = buf[0].init;
		size_t n = sink.commit(buf.length);
	}
}

/// Defines the inteface of a pull-source.
@implements!(PullSource, PullSource)
struct PullSource {
	size_t pull(T)(T[] buf) { return buf.length; }
}

/// Defines the interface of a peek-source.
@implements!(PeekSource, PeekSource)
struct PeekSource {
	const(T)[] peek(T = ubyte)(size_t n) { return new T[n]; };
	void consume(T = ubyte)(size_t n) {}
}

/// Defines the interface of a push-sink.
@implements!(PushSink, PushSink)
struct PushSink {
	size_t push(T)(const(T)[] buf) { return buf.length; }
}

/// Defines the interface of an alloc-sink.
@implements!(AllocSink, AllocSink)
struct AllocSink {
	T[] alloc(T = ubyte)(size_t n) { return new T[n]; };
	size_t commit(T = ubyte)(size_t n) { return n; }
}

private auto getMemberFunction(string name, S)(ref S s)
{
	mixin(`
		static if (is(typeof(&s.` ~ name ~ `!(SomePOD!name))))
			return __ctfe ? &s.` ~ name ~ `!(SomePOD!name) : null;
		else static if (is(typeof(&s.` ~ name ~ `!())))
			return __ctfe ? &s.` ~ name ~ `!() : null;
		else static if (is(typeof(&s.` ~ name ~ `)))
			return __ctfe ? &s.` ~ name ~ ` : null;
		else static assert(0);`);
}

unittest {
	static struct Bar {
		void foo();
		void bar()(size_t n);
		int baz(T)(T a);
	}
	Bar bar;
	import std.traits : isCallable;
	static assert(isCallable!(typeof(getMemberFunction!"foo"(bar))));
	static assert(isCallable!(typeof(getMemberFunction!"bar"(bar))));
	static assert(isCallable!(typeof(getMemberFunction!"baz"(bar))));
}

private bool checkPeekable(alias S)() {
	S s;
	auto peek = getMemberFunction!"peek"(s);
	auto consume = getMemberFunction!"consume"(s);
	auto buf = peek(1);
	consume(1);
	size_t n = buf.length;
	auto x = buf[0];
	auto y = buf[$ - 1];
	return true;
}

/// Tests if type `S` implements member functions `peek` and `consume`.
template isPeekable(S) {
	enum bool isPeekable = /+
		   is(PeekElementType!(S, false))
		|| is(PeekElementType!(S, true))
		|| is(PeekElementType!(S, true, SomePOD!"isPeekable")); +/
	__traits(compiles, checkPeekable!S);
}

private bool checkPullable(alias S)() {
	S s;
	static assert(__traits(hasMember, S, "pull"));
	auto func = getMemberFunction!"pull"(s);
	import std.traits : ParameterTypeTuple, Unqual;
	import std.range : ElementEncodingType;
	alias T = Unqual!(ElementEncodingType!(ParameterTypeTuple!(typeof(func))[0]));
	auto buf = new T[1024];
	size_t n = s.pull(buf);
	return true;
}

/// Tests if type `S` implements member function `pull`.
template isPullable(S) {
	enum bool isPullable = /+
		   is(PullElementType!(S))
		|| is(PullElementType!(S, SomePOD!"isPullable"))
		|| is(PullElementType!(S, null));+/
	__traits(compiles, checkPullable!S);

}

private bool checkPushable(alias S)() {
	S s;
	static assert(__traits(hasMember, S, "push"));
	auto func = getMemberFunction!"push"(s);
	import std.traits : ParameterTypeTuple, Unqual;
	import std.range : ElementEncodingType;
	alias T = Unqual!(ElementEncodingType!(ParameterTypeTuple!(typeof(func))[0]));
	auto buf = new const(T)[1024];
	size_t n = s.push(buf);
	return true;
}

/// Tests if type `S` implements member function `push`.
template isPushable(S) {
	enum bool isPushable = /+
		   is(PushElementType!(S))
		|| is(PushElementType!(S, SomePOD!"isPushable"))
		|| is(PushElementType!(S, null)); +/
	__traits(compiles, checkPushable!S);
}

private bool checkAllocable(alias S)() {
	S s;
	auto alloc = getMemberFunction!"alloc"(s);
	auto commit = getMemberFunction!"commit"(s);
	auto buf = alloc(1);
	size_t n = buf.length;
	typeof(*buf.ptr) dataEl;
	buf[0] = dataEl;
	buf[$ - 1] = dataEl;
	size_t m = commit(1);
	return true;
}

/// Tests if type `S` implements member functions `alloc` and `commit`.
template isAllocable(S) {
	enum isAllocable = /+
		   is(AllocElementType!(S, false))
		|| is(AllocElementType!(S, true))
		|| is(AllocElementType!(S, true, SomePOD!"isAllocable"));+/
	__traits(compiles, checkAllocable!S);
}

private bool check(alias test, alias S)()
{
	// this idiom is used to avoid check failures due to correct,
	// but non-ctfeable code in stages
	static if (!__traits(compiles, test!S))
		return test!S;
	return true;
}

private bool check(alias test, alias req, alias S, Args...)()
{
	alias T = S!Args;
	T t;
	static if (!__traits(compiles, test!T))
		return test!T;
	static assert(Args.length == 1 || Args.length == 2);
	static assert(requiresSpecificTypes!(req, S, Args));
	return true;
}

/** Tests if `S` _implements the specified sink and source interfaces.

It is meant to be used as an attribute with the definition of `S`.
If `S` fails to comply with any of the specified interfaces,
a compilation error occurs.

Params:
sinkInterface   = One of: PushSink, AllocSink, PullSink, PeekSink.
sourceInterface = One of: PullSource, PeekSource, PushSource, AllocSource.
interface_ = One of: PullSource, PeekSource, PushSource, AllocSource, PushSink, AllocSink, PullSink, PeekSink
S = struct or struct template to test

Example:
---
@implements!(PushSink, NullSink) // fails to compile if there are errors in NullSink
struct NullSink {
	size_t push(T)(const(T)[] buf) { return buf.length; }
}
---

*/
bool implements(alias sinkInterface, alias sourceInterface, alias S)() {
	if (__ctfe) {
		struct Id(S...) {}
		static if (is(Id!sinkInterface == Id!None)) {
			static assert(!isPeekSink!S);
			static assert(!isPullSink!S);
			static assert(!isPushSink!S);
			static assert(!isAllocSink!S);
			static if (is(Id!sourceInterface == Id!PeekSource)) {
				return check!(checkPeekable, S);
			} else static if (is(Id!sourceInterface == Id!PullSource)) {
				return check!(checkPullable, S);
			} else static if (is(Id!sourceInterface == Id!PushSource)) {
				return check!(checkRunnable, isRunnable, S, PushSink);
			} else static if (is(Id!sourceInterface == Id!AllocSource)) {
				return check!(checkRunnable, isRunnable, S, AllocSink);
			} else {
				static assert(0, "Invalid arguments: " ~ str!sinkInterface ~ ", " ~ str!sourceInterface ~ ", " ~ str!S);
			}
		} else static if (is(Id!sourceInterface == Id!None)) {
			static assert(!isPeekSource!S);
			static assert(!isPullSource!S);
			static assert(!isPushSource!S);
			static assert(!isAllocSource!S);
			static if (is(Id!sinkInterface == Id!PushSink)) {
				return check!(checkPushable, S);
			} else static if (is(Id!sinkInterface == Id!AllocSink)) {
				return check!(checkAllocable, S);
			} else static if (is(Id!sinkInterface == Id!PullSink)) {
				return check!(checkRunnable, isRunnable, S, PullSource);
			} else static if (is(Id!sinkInterface == Id!PeekSink)) {
				return check!(checkRunnable, isRunnable, S, PeekSource);
			} else {
				static assert(0, "Invalid arguments: " ~ str!sinkInterface ~ ", " ~ str!sourceInterface ~ ", " ~ str!S);
			}
		} else static if (is(Id!sinkInterface == Id!PeekSink)) {
			static if (is(Id!sourceInterface == Id!PeekSource)) {
				return check!(checkPeekable, isPeekable, S, PeekSource);
			} else static if (is(Id!sourceInterface == Id!PullSource)) {
				return check!(checkPullable, isPullable, S, PeekSource);
			} else static if (is(Id!sourceInterface == Id!PushSource)) {
				return check!(checkRunnable, isRunnable, S, PeekSource, PushSink);
			} else static if (is(Id!sourceInterface == Id!AllocSource)) {
				return check!(checkRunnable, isRunnable, S, PeekSource, AllocSink);
			} else {
				static assert(0, "Invalid arguments: " ~ str!sinkInterface ~ ", " ~ str!sourceInterface ~ ", " ~ str!S);
			}
		} else static if (is(Id!sinkInterface == Id!PullSink)) {
			static if (is(Id!sourceInterface == Id!PeekSource)) {
				return check!(checkPeekable, isPeekable, S, PullSource);
			} else static if (is(Id!sourceInterface == Id!PullSource)) {
				return check!(checkPullable, isPullable, S, PullSource);
			} else static if (is(Id!sourceInterface == Id!PushSource)) {
				return check!(checkRunnable, isRunnable, S, PullSource, PushSink);
			} else static if (is(Id!sourceInterface == Id!AllocSource)) {
				return check!(checkRunnable, isRunnable, S, PullSource, AllocSink);
			} else {
				static assert(0, "Invalid arguments: " ~ str!sinkInterface ~ ", " ~ str!sourceInterface ~ ", " ~ str!S);
			}
		} else static if (is(Id!sinkInterface == Id!PushSink)) {
			static if (is(Id!sourceInterface == Id!PeekSource)) {
				static if (!__traits(compiles, checkPushable!S))
					return checkPushable!S;
				else static if (!__traits(compiles, checkPeekable!S))
					return checkPeekable!S;
			} else static if (is(Id!sinkInterface == Id!PushSink) && is(Id!sourceInterface == Id!PullSource)) {
				static if (!__traits(compiles, checkPushable!S))
					return checkPushable!S;
				else static if (!__traits(compiles, checkPullable!S))
					return checkPullable!S;
			} else static if (is(Id!sinkInterface == Id!PushSink) && is(Id!sourceInterface == Id!PushSource)) {
				return check!(checkPushable, isPushable, S, PushSink);
			} else static if (is(Id!sinkInterface == Id!PushSink) && is(Id!sourceInterface == Id!AllocSource)) {
				return check!(checkPushable, isPushable, S, AllocSink);
			} else {
				static assert(0, "Invalid arguments: " ~ str!sinkInterface ~ ", " ~ str!sourceInterface ~ ", " ~ str!S);
			}
		} else static if (is(Id!sinkInterface == Id!AllocSink)) {
			static if (is(Id!sourceInterface == Id!PeekSource)) {
				static if (!__traits(compiles, checkPushable!S))
					return checkPushable!S;
				else static if (!__traits(compiles, checkPeekable!S))
					return checkPeekable!S;
			} else static if (is(Id!sinkInterface == Id!PushSink) && is(Id!sourceInterface == Id!PullSource)) {
				static if (!__traits(compiles, checkPushable!S))
					return checkPushable!S;
				else static if (!__traits(compiles, checkPullable!S))
					return checkPullable!S;
			} else static if (is(Id!sinkInterface == Id!PushSink) && is(Id!sourceInterface == Id!PushSource)) {
				return check!(checkPushable, isPushable, S, PushSink);
			} else static if (is(Id!sinkInterface == Id!PushSink) && is(Id!sourceInterface == Id!AllocSource)) {
				return check!(checkPushable, isPushable, S, AllocSink);
			} else {
				static assert(0, "Invalid arguments: " ~ str!sinkInterface ~ ", " ~ str!sourceInterface ~ ", " ~ str!S);
			}
		} else {
			static assert(0, "Invalid arguments: " ~ str!sinkInterface ~ ", " ~ str!sourceInterface ~ ", " ~ str!S);
		}
	}
	return true;
}

/// ditto
bool implements(alias interface_, alias S)() pure {
	if (__ctfe) {
		struct Id(S...) {}
		static if (is(Id!interface_ == Id!PeekSink)
			|| is(Id!interface_ == Id!PullSink)
			|| is(Id!interface_ == Id!PushSink)
			|| is(Id!interface_ == Id!AllocSink))
			return implements!(interface_, None, S);
		else
			return implements!(None, interface_, S);
	}
	return true;
}

///
template SourceElementType(alias S, Default = ubyte) {
	static if (isPullSource!S) {
		static if (is(FixedPullType!S F))
			alias SourceElementType = F;
		else
			alias SourceElementType = Default;
	}
	else static assert(0, "not implemented");

}

///
template SinkElementType(alias S, Default = ubyte) {
	static if (isPushSink!S) {
		static if (is(FixedPushType!S F))
			alias SinkElementType = F;
		else
			alias SinkElementType = Default;
	}
	else static assert(0, "not implemented");
}
