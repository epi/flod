module flod.npl;
import std.stdio;
import std.string : format;

import std.meta : staticMap;

template inst(alias S, A...)
{
	bool impl() { return is(S!A); }
	enum inst = impl();
}

struct NoFoo
{
}

struct HasFoo
{
	void foo() {}
}

struct CallsFoo(T)
{
	T t;
	void run()
	{
		t.foo();
	}
}

static assert(!inst!(CallsFoo, NoFoo));
static assert( inst!(CallsFoo, HasFoo));

/// Returns an `AliasSeq` where `S` is repeated `count` times.
template Repeat(int count, S...) {
	import std.meta : AliasSeq;
	static if (count == 0)
		alias Repeat = AliasSeq!();
	else static if (count == 1)
		alias Repeat = AliasSeq!S;
	else
		alias Repeat = AliasSeq!(.Repeat!(count - 1, S), S);
}

unittest
{
	import std.meta : AliasSeq;
	template Z(S...) {}
	alias I = int;
	static struct X(S...) {}
	static assert(is(Repeat!(5, void) == AliasSeq!(void, void, void, void, void)));
	alias Repeated = Repeat!(3, 5, Z, I, bool);
	static assert(is(X!Repeated == X!(5, Z, I, bool, 5, Z, I, bool, 5, Z, I, bool)));
}

/// Returns an `AliasSeq` where `S[index + 1]` is replaced with `S[0]`.
template ReplaceAt(int index, S...) if (index < S.length - 1) {
	import std.meta : AliasSeq;
	alias ReplaceAt = AliasSeq!(S[1 .. index + 1], S[0], S[index + 2 .. $]);
}

unittest
{
	import std.meta : AliasSeq;
	static struct X(S...) {}
	struct Z;
	static assert(is(X!(ReplaceAt!(4, Z, Repeat!(6, void))) == X!(void, void, void, void, Z, void)));
}

private struct TypeList(S...) {}

template Overlay(X, Y) {
	import std.meta : AliasSeq;
	static if (is(X == TypeList!XL, XL...)) {
		static if (is(Y == TypeList!YL, YL...)) {
			static if (XL.length == YL.length) {
				static if (XL.length > 1) {
					alias Remain = Overlay!(TypeList!(XL[1 .. $]), TypeList!(YL[1 .. $]));
				} else {
					alias Remain = AliasSeq!();
				}
				static assert(is(XL[0] == void) || is(YL[0] == void));
				static if (is(XL[0] == void))
					alias Overlay = AliasSeq!(YL[0], Remain);
				else
					alias Overlay = AliasSeq!(XL[0], Remain);
			}
		}
	}
}

unittest
{
	struct W;
	struct C;
	alias List1 = TypeList!(void, int, W, void, void, C);
	alias List2 = TypeList!(double, void, void, void, C, void);
	pragma(msg, TypeList!(Overlay!(List1, List2)).stringof);
	static assert(is(TypeList!(Overlay!(List1, List2)) == TypeList!(double, int, W, void, C, C)));
}

template VectorAdd(alias X, alias Y) if (X.length == Y.length) {
	static if (X.length > 1)
		enum VectorAdd = [ X[0] + Y[0] ] ~ VectorAdd!(X[1 .. $], Y[1 .. $]);
	else
		enum VectorAdd = [ X[0] + Y[0] ];
}

template VectorZero(int length) {
	alias Arr = int[length];
	enum VectorZero = Arr.init[];
}

template VectorSet(int index, alias Value, alias X) {
	enum VectorSet = X[0 .. index] ~ Value ~ X[index + 1 .. $];
}

unittest
{
	enum of1 = [ 0, 5, 10, 20, 35 ];
	enum of2 = [ 4, 1, 0, 0, 0 ];
	static assert(VectorZero!5 == [0, 0, 0, 0, 0]);
	static assert(VectorAdd!(of1, of2) == [4, 6, 10, 20, 35]);
	static assert(VectorAdd!(VectorZero!5, of2) == [4, 1, 0, 0, 0]);
}

template Builder(int begin, int cur, int end, Stages...) {
	static assert(begin <= end && begin <= cur && cur <= end && end <= Stages.length,
		"Invalid parameters: " ~
		Stages.stringof ~ "[" ~ begin.stringof ~ "," ~ cur.stringof ~ "," ~ end.stringof ~ "]");
	static if (cur < end) {
		alias Pl = Pipeline!Stages;
		enum index = cur;
		static assert(is(Pl));
		alias Cur = Stages[cur]; // template
		alias Lhs = Builder!(begin, begin, cur, Stages);
		alias Rhs = Builder!(cur + 1, cur + 1, end, Stages);
		static if (is(Lhs.Impl L)) {
			alias LhsImpl = L;
		} else { alias LhsImpl = void; }
		static if (is(Rhs.Impl R)) {
			alias RhsImpl = R;
		} else { alias RhsImpl = void; }
		static if (begin + 1 == end && inst!(Cur, Pl)) {
			alias Impl = Cur!Pl;
			alias ImplSeq = ReplaceAt!(cur, Impl, Repeat!(Stages.length, void));
		} else static if (cur + 1 == end && inst!(Cur, Pl, LhsImpl)) {
			alias Impl = Cur!(Pl, LhsImpl);
			alias ImplSeq =
				Overlay!(
					TypeList!(Lhs.ImplSeq),
					TypeList!(ReplaceAt!(cur, Impl, Repeat!(Stages.length, void))));
			alias offsetSeq = VectorZero!(Stages.length);
			pragma(msg, format("%d.%d.%d %s %s", begin, cur, end, Stages.stringof, Impl.stringof));
		} else static if (cur == begin && inst!(Cur, Pl, RhsImpl)) {
			alias Impl = Cur!(Pl, RhsImpl);
			alias ImplSeq =
				Overlay!(
					TypeList!(Rhs.ImplSeq),
					TypeList!(ReplaceAt!(cur, Impl, Repeat!(Stages.length, void))));
		} else static if (inst!(Cur, Pl, LhsImpl, RhsImpl)) {
			alias Impl = Cur!(Pl, LhsImpl, RhsImpl);
			alias ImplSeq =
				Overlay!(
					TypeList!(Overlay!(
						TypeList!(Lhs.ImplSeq),
						TypeList!(Rhs.ImplSeq))),
					TypeList!(ReplaceAt!(cur, Impl, Repeat!(Stages.length, void))));
			pragma(msg, format("%d.%d.%d %s %s", begin, cur, end, Stages.stringof, Impl.stringof));
		}
		static if (is(Impl)) {
			template offsetOf(S) {
				static if (is(S == Impl)) {
					enum offsetOf = 0;
				} else static if (is(typeof(Impl.init.source)) && is(typeof(Impl.init.source) == S)) {
					enum offsetOf = Impl.init.source.offsetof;
				} else static if (is(typeof(Impl.init.sink)) && is(typeof(Impl.init.sink) == S)) {
					enum offsetOf = Impl.init.sink.offsetof;
				} else static if (is(typeof(Lhs.offsetOf!S))) {
					static if (is(Lhs.Impl == typeof(Impl.source))) {
						enum offsetOf = Impl.init.source.offsetof + Lhs.offsetOf!S;
					} else static if (is(Lhs.Impl == typeof(Impl.sink))) {
						enum offsetOf = Impl.init.sink.offsetof + Lhs.offsetOf!S;
					}
				} else static if (is(typeof(Rhs.offsetOf!S))) {
					static if (is(Rhs.Impl == typeof(Impl.source))) {
						enum offsetOf = Impl.init.source.offsetof + Rhs.offsetOf!S;
					} else static if (is(Rhs.Impl == typeof(Impl.sink))) {
						enum offsetOf = Impl.init.sink.offsetof + Rhs.offsetOf!S;
					}
				}
			}
			void construct()(ref Impl impl) {
				static if (is(LhsImpl))
					Lhs.construct(impl.source);
				static if (is(RhsImpl))
					Rhs.construct(impl.sink);
				static if (hasCtorArgs!(Stages[cur])) {
					alias Requested = Stages[cur];
					enum index = tupleIndex!(cur, Stages);
					auto stage = cast(Stages[cur]*) _res.stages[index];
					impl.__ctor(stage.args);
				}
				// writefln("%-30s %X[%d]: [%(%02x%|, %)]", Stages[cur].Impl.stringof, &impl, impl.sizeof, (cast(ubyte*) &impl)[0 .. impl.sizeof]);
			}
		} static if (cur + 1 < end) {
			alias Next = Builder!(begin, cur + 1, end, Stages);
			static if (is(Next.Impl)) {
				alias Builder = Next;
			}
		}
	}
}

template GetStage(int index, alias SI)
{
	static if (SI.index == index) {
		alias GetStage = SI.Impl;
	}
}

struct Pipeline(Stages...)
{
	private alias Builder = .Builder!(0, 0, Stages.length, Stages);
	int foo;
	static if (is(Builder.Impl)) {
		alias Impl = Builder.Impl;
		alias ImplSeq = Builder.ImplSeq;
		static ref auto get(S)(ref S that) {
			void* thatPtr = &that;
			void* outerPtr = thatPtr - (Pipeline.init.stream.offsetof + Builder.offsetOf!S);
			return *cast(Pipeline*) outerPtr;
		}
		Impl stream;
	}
	int getFoo() { return foo; }
}

struct PullSource(Pl)
{
	void pull() {}
}

struct PullFilter(Pl, Source)
{
	Source source;
	void pull() { source.pull(); }
}

struct PullPush(Pl, Source, Sink)
{
	Source source;
	Sink sink;
	void run()
	{
		source.pull();
		sink.push();
	}
}

struct PushFilter(Pl, Sink)
{
	Sink sink;
	void push() { sink.push(); }
}

struct PushSink(Pl)
{
	void push()
	{
		//assert(Pl.get(this).getFoo() == 42);
	}
}

struct PushSource(Pl, Sink)
{
	Sink sink;
	void run()
	{
		sink.push();
	}
}

unittest
{
	alias P = Builder!(0, 0, 2, PushSource, PushSink);
	alias Q = Builder!(0, 0, 3, PullSource, PullPush, PushSink);
	static assert(!is(Builder!(0, 0, 3, PushSource, PullPush, PushSink).Impl));
	alias R = Builder!(0, 0, 5, PullSource, PullFilter, PullPush, PushFilter, PushSink);
//	alias Pl = Pipeline!(PullSource, PullFilter, PullPush, PushFilter, PushSink);
	pragma(msg, "     => ", Pl.Builder.stringof);
	static assert(is(Pl.Impl));
	static assert(is(R.ImplSeq[0] == PullSource!Pl));
	static assert(is(R.ImplSeq[1] == PullFilter!(Pl, PullSource!Pl)));
	static assert(is(R.ImplSeq[2] ==
			PullPush!(Pl,
				PullFilter!(Pl, PullSource!Pl),
				PushFilter!(Pl, PushSink!Pl))));
	static assert(is(R.ImplSeq[3] == PushFilter!(Pl, PushSink!Pl)));
	static assert(is(R.ImplSeq[4] == PushSink!Pl));

	R.Impl stream;
	static assert(R.offsetOf!(R.ImplSeq[0]) == stream.source.offsetof + stream.source.source.offsetof);
	static assert(R.offsetOf!(R.ImplSeq[1]) == stream.source.offsetof);
	static assert(R.offsetOf!(R.ImplSeq[2]) == 0);
	static assert(R.offsetOf!(R.ImplSeq[3]) == stream.sink.offsetof);
	static assert(R.offsetOf!(R.ImplSeq[4]) == stream.sink.offsetof + stream.sink.sink.offsetof);

	auto pipeline = Pl(42);
	pragma(msg, Pl.Impl.stringof);
	//pipeline.stream.run();
}
