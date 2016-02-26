/** This module defines templates for determining characteristics of stream components at compile time.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://boost.org/LICENSE_1_0.txt, Boost License 1.0).
 */
module flod.traits;

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

template hasRun(S) {
	enum bool hasRun = __traits(compiles,
		{
			S x;
			x.run();
		}());
}

template hasStep(S) {
	enum bool hasStep = __traits(compiles,
		{
			S x;
			while (x.step() == 0) {}
		});
}

// Defines a unique POD struct type
private struct SomePOD(string cookie = __FILE__ ~ ":" ~ __LINE__.stringof) { string meaningless = cookie; }

///
template isRunnable(P) {
	enum bool isRunnable = hasRun!P || hasStep!P;
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
				alias B = ParameterTypeTuple!Func[0];
				alias T = typeof({ B b; return b[0].init; }());
				T[] buf;
				size_t n = s.pull(buf);
				return buf[0];
			}());
	}
	static if (is(typeof(Args[0]) == typeof(null))) {
		alias PullElementType = ElementType!(typeof({ S s; return &s.pull; }()));
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
	static if (!is(PushElementType!(S, SomePOD!"FixedPushType"))) {
		static if (is(PushElementType!(S) T))
			alias FixedPushType = T;
		else static if (is(PushElementType!(S, null) U))
			alias FixedPushType = U;
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
		bool step()() { return false; }
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

/// Returns `true` if `S` is a sink which reads data by calling `peek()` and `consume()`.
template isPeekSink(S...) {
	private template isPeekSinkTempl(alias Z) {
		enum isPeekSinkTempl =
			   onlyValidFor!(isRunnable, Z, DummyPeekSource)
			|| onlyValidFor!(isPullable, Z, DummyPeekSource)
			|| onlyValidFor!(isPeekable, Z, DummyPeekSource)
			|| onlyValidFor!(isRunnable, Z, DummyPeekSource, DummyPushSink)
			|| onlyValidFor!(isRunnable, Z, DummyPeekSource, DummyAllocSink);
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
