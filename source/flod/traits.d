/** This module defines templates for determining characteristics of stream components at compile time.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://boost.org/LICENSE_1_0.txt, Boost License 1.0).
 */
module flod.traits;

struct PullSource(E) {
	size_t pull(E[] buf) { return buf.length; }
}

struct PeekSource(E) {
	const(E)[] peek(size_t n) { return new E[n]; }
	void consume(size_t n) {}
}

struct PushSource(E) {}

struct AllocSource(E) {}

struct PullSink(E) {}

struct PeekSink(E) {}

struct PushSink(E) {
	size_t push(const(E)[] buf) { return buf.length; }
}

struct AllocSink(E) {
	bool alloc(ref E[] buf, size_t n) { buf = new E[n]; return true; }
	void consume(size_t n) {}
}

enum pullSource(E) = PullSource!E();
enum peekSource(E) = PeekSource!E();
enum pushSource(E) = PushSource!E();
enum allocSource(E) = AllocSource!E();

enum pullSink(E) = PullSink!E();
enum peekSink(E) = PeekSink!E();
enum pushSink(E) = PushSink!E();
enum allocSink(E) = AllocSink!E();

private struct None {}

private struct Id(S...) {}

private template isSame(alias S) {
	template isSame(alias Z) {
		enum isSame = is(Id!S == Id!Z);
	}
}

private template areSame(W...) {
	enum areSame = is(Id!(W[0 .. $ / 2]) == Id!(W[$ / 2 .. $]));
}

package template Traits(alias Src = None, alias Snk = None, SrcE = void, SnkE = void, UDAs...)
{
	static if (UDAs.length == 0) {
		alias Source = Src;
		alias Sink = Snk;
		alias SourceElementType = SrcE;
		alias SinkElementType = SnkE;
	} else {
		import std.meta : anySatisfy;

		alias T = typeof(UDAs[0]);
		static if (is(T == S!E, alias S, E)) {
			static if (anySatisfy!(isSame!S, PullSource, PeekSource, PushSource, AllocSource)) {
				static assert(is(Id!Src == Id!None), "Source interface declared more than once");
				alias Traits = .Traits!(S, Snk, E, SnkE, UDAs[1 .. $]);
			} else static if (anySatisfy!(isSame!S, PullSink, PeekSink, PushSink, AllocSink)) {
				static assert(is(Id!Snk == Id!None), "Sink interface declared more than once");
				alias Traits = .Traits!(Src, S, SrcE, E, UDAs[1 .. $]);
			} else {
				alias Traits = .Traits!(Src, Snk, SrcE, SnkE, UDAs[1 .. $]);
			}
		} else {
			alias Traits = .Traits!(Src, Snk, SrcE, SnkE, UDAs[1 .. $]);
		}
	}
}

package template getTraits(alias S) {
	alias getTraits = Traits!(None, None, void, void, __traits(getAttributes, S));
}

unittest {
	@peekSource!int @(100) @Id!"zombie"() @allocSink!(Id!1)
	struct Foo {}
	alias Tr = getTraits!Foo;
	static assert(areSame!(Tr.Source, PeekSource));
	static assert(areSame!(Tr.SourceElementType, int));
	static assert(areSame!(Tr.Sink, AllocSink));
	static assert(areSame!(Tr.SinkElementType, Id!1));
}

unittest {
	@pullSource!int @pushSource!ubyte
	struct Bar {}
	static assert(!__traits(compiles, getTraits!Bar)); // source interface specified twice
}

unittest {
	@pullSink!double @pushSink!string @peekSink!void
	struct Baz {}
	static assert(!__traits(compiles, getTraits!Baz)); // sink interface specified 3x
}

enum isPullSource(alias S) = areSame!(getTraits!S.Source, PullSource);
enum isPeekSource(alias S) = areSame!(getTraits!S.Source, PeekSource);
enum isPushSource(alias S) = areSame!(getTraits!S.Source, PushSource);
enum isAllocSource(alias S) = areSame!(getTraits!S.Source, AllocSource);

enum isPullSink(alias S) = areSame!(getTraits!S.Sink, PullSink);
enum isPeekSink(alias S) = areSame!(getTraits!S.Sink, PeekSink);
enum isPushSink(alias S) = areSame!(getTraits!S.Sink, PushSink);
enum isAllocSink(alias S) = areSame!(getTraits!S.Sink, AllocSink);

unittest {
	@pullSource!int @pullSink!bool
	struct Foo {}
	static assert( isPullSource!Foo);
	static assert(!isPeekSource!Foo);
	static assert(!isPushSource!Foo);
	static assert(!isAllocSource!Foo);
	static assert( isPullSink!Foo);
	static assert(!isPeekSink!Foo);
	static assert(!isPushSink!Foo);
	static assert(!isAllocSink!Foo);
}

enum isPassiveSource(alias S) = isPeekSource!S || isPullSource!S;
enum isActiveSource(alias S) = isPushSource!S || isAllocSource!S;
enum isSource(alias S) = isPassiveSource!S || isActiveSource!S;

enum isPassiveSink(alias S) = isPushSink!S || isAllocSink!S;
enum isActiveSink(alias S) = isPeekSink!S || isPullSink!S;
enum isSink(alias S) = isPassiveSink!S || isActiveSink!S;

version(none):

import flod.meta : isType, ReplaceWithMask, str;

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

// test if S!Types can be instantiated and fullfills req
// but fails to do so if any of Types is substituted with empty struct
private template onlyValidFor(alias req, alias S, Types...) {
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
		enum onlyValidFor = false;
	else
		enum onlyValidFor = sub!((1UL << Types.length) - 2);
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
template isRunnable(P) {
	enum bool isRunnable = __traits(compiles,
		{
			P x;
			x.run();
		}());
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
template isPushable(S) {
	enum bool isPushable =
		   is(PushElementType!(S))
		|| is(PushElementType!(S, SomePOD!"isPushable"))
		|| is(PushElementType!(S, null));
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
template isPullable(S) {
	enum bool isPullable =
		   is(PullElementType!(S))
		|| is(PullElementType!(S, SomePOD!"isPullable"))
		|| is(PullElementType!(S, null));
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
				s.commit!T(buf.length);
			else
				s.commit(buf.length);
			return T.init;
		}());
}

///
template isAllocable(S) {
	enum isAllocable =
		   is(AllocElementType!(S, false))
		|| is(AllocElementType!(S, true))
		|| is(AllocElementType!(S, true, SomePOD!"isAllocable"));
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
template isPeekable(S) {
	enum bool isPeekable =
		   is(PeekElementType!(S, false))
		|| is(PeekElementType!(S, true))
		|| is(PeekElementType!(S, true, SomePOD!"isPeekable"));
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
			   onlyValidFor!(isPullable, Z, DummyPullSource)
			|| onlyValidFor!(isPullable, Z, DummyPeekSource);
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
		   onlyValidFor!(isPeekable, Z, DummyPullSource)
		|| onlyValidFor!(isPeekable, Z, DummyPeekSource);
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
			   onlyValidFor!(isRunnable,  Z, DummyPushSink)
			|| onlyValidFor!(isPushable,  Z, DummyPushSink)
			|| onlyValidFor!(isAllocable, Z, DummyPushSink)
			|| onlyValidFor!(isRunnable,  Z, DummyPeekSource, DummyPushSink)
			|| onlyValidFor!(isRunnable,  Z, DummyPullSource, DummyPushSink);
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
		void commit(size_t s) {
			sink.push(buf[0 .. s]);
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
			   onlyValidFor!(isRunnable,  Z, DummyAllocSink)
			|| onlyValidFor!(isPushable,  Z, DummyAllocSink)
			|| onlyValidFor!(isAllocable, Z, DummyAllocSink)
			|| onlyValidFor!(isRunnable,  Z, DummyPeekSource, DummyAllocSink)
			|| onlyValidFor!(isRunnable,  Z, DummyPullSource, DummyAllocSink);
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
			   onlyValidFor!(isPushable, Z, DummyPushSink)
			|| onlyValidFor!(isPushable, Z, DummyAllocSink);
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
			   onlyValidFor!(isAllocable, S, DummyPushSink)
			|| onlyValidFor!(isAllocable, S, DummyAllocSink);
	}
	enum bool isAllocSink = testStage!(isAllocable, templ, S);
}

unittest {
	static assert(!isAllocSink!int);
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

/// Returns `true` if `S` is a sink which reads data by calling `pull()`.
template isPullSink(S...) {
	private template isPushSinkTempl(alias Z) {
		enum isPushSinkTempl =
			   onlyValidFor!(isRunnable, Z, DummyPullSource)
			|| onlyValidFor!(isPullable, Z, DummyPullSource)
			|| onlyValidFor!(isPeekable, Z, DummyPullSource)
			|| onlyValidFor!(isRunnable, Z, DummyPullSource, DummyPushSink)
			|| onlyValidFor!(isRunnable, Z, DummyPullSource, DummyAllocSink);
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
			   onlyValidFor!(isInputRange, Z, DummyPeekSource)
			|| onlyValidFor!(isRunnable,   Z, DummyPeekSource)
			|| onlyValidFor!(isPullable,   Z, DummyPeekSource)
			|| onlyValidFor!(isPeekable,   Z, DummyPeekSource)
			|| onlyValidFor!(isRunnable,   Z, DummyPeekSource, DummyPushSink)
			|| onlyValidFor!(isRunnable,   Z, DummyPeekSource, DummyAllocSink);
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

template check(string sinkMethod, string sourceMethod, S...)
	if (S.length == 1)
{
	static if (sinkMethod == "peek" && sourceMethod == "push") {
		alias __S = S[0];
		alias __check = __S!(DummyPeekSource, DummyPushSink);
		static assert(isRunnable!__check);
		enum check = true;
	}
	else {
		static assert(0, "not implemented");
	}
}

template satisfies(alias Constraint, S...) {
	enum id = __traits(identifier, Constraint);
	static assert(id != "Constraint", "Undefined constraint: " ~ Constraint.stringof);
	enum check = "check" ~ id[2 .. $];
	enum assert_ = "static assert(" ~ check ~ "!S);";
	mixin(assert_);
}

bool checkPushSink(S...)()
{
	if (!__ctfe) {
		return true;
	} else {
		static if (S.length != 1) {
			return false;
		} else static if (is(S[0])) {
			alias P = S[0];
			P s;
			static if (is(FixedPushType!P T))
				size_t result = s.push(new T[1]);
			else
				size_t result = s.push(new int[1]);
			return true;
		} else {
			return false;
		}
	}
}

bool checkPushSource(S...)()
{
	if (!__ctfe) {
		return true;
	} else {
		static if (S.length != 1) {
			return false;
		} else static if (is(S[0])) {
			static assert(0, S[0].stringof ~ " must be a struct template, not a " ~ str!S[0]);
		} else {
			alias Templ = S[0];
			static if (isType!(Templ, DummyPushSink)) {
				alias Type = Templ!DummyPushSink;
				static if (isRunnable!Type) {
					return true;
				} else if (isPushable!Type) {
					return true;
				} else if (isAllocable!Type) {
					return true;
				} else {
					Type t;
					t.run();
					size_t a = t.push(new void[1]);
					return true;
				}
			} else {
				alias Type = Templ!DummyPushSink;
				Type t;
				t.run();
				return false;
			}
		}
	}
}

bool checkAllocSource(S...)()
{
	if (!__ctfe) {
		return true;
	} else {
		assert(S.length == 1);
		assert(__traits(isTemplate, S[0]));
		alias Templ = S[0];
		assert(isType!(Templ, DummyAllocSink));
		alias Type = Templ!DummyAllocSink;
		Type t;
		static if (isRunnable!Type || isPushable!Type || isAllocable!Type) {
			return true;
		} else {
			t.run();
			t.alloc(15);
			t.push();
		}
	}
}
