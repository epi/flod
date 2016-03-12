/** Templates for declaring and examining static interfaces of pipeline stages.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://boost.org/LICENSE_1_0.txt, Boost License 1.0).
 */
module flod.traits;

import flod.meta : str;

struct PullSource(E) {
	size_t pull(E[] buf) { return buf.length; }
	enum methodStr = "pull";
}

struct PeekSource(E) {
	const(E)[] peek(size_t n) { return new E[n]; }
	void consume(size_t n) {}
	enum methodStr = "peek";
}

struct PushSource(E) {
	enum methodStr = "push";
}

struct AllocSource(E) {
	enum methodStr = "alloc";
}

struct PullSink(E) {
	enum methodStr = "pull";
}

struct PeekSink(E) {
	enum methodStr = "peek";
}

struct PushSink(E) {
	size_t push(const(E)[] buf) { return buf.length; }
	enum methodStr = "push";
}

struct AllocSink(E) {
	bool alloc(ref E[] buf, size_t n) { buf = new E[n]; return true; }
	void consume(size_t n) {}
	enum methodStr = "alloc";
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

package template Traits(alias Src, alias Snk, SrcE, SnkE, UDAs...) {
	static if (UDAs.length == 0) {
		alias Source = Src;
		alias Sink = Snk;
		alias SourceElementType = SrcE;
		alias SinkElementType = SnkE;
		static if (!is(Src == None))
			enum sourceMethodStr = Src!SrcE.methodStr;
		else
			enum sourceMethodStr = "";
		static if (!is(Snk == None))
			enum sinkMethodStr = Snk!SnkE.methodStr;
		else
			enum sinkMethodStr = "";
		enum str = .str!Sink ~ "!" ~ .str!SinkElementType ~ "-" ~ .str!Source ~ "!" ~ .str!SourceElementType;
	} else {
		import std.meta : anySatisfy;

		static if (is(typeof(UDAs[0]))) {
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
		} else {
			alias Traits = .Traits!(Src, Snk, SrcE, SnkE, UDAs[1 .. $]);
		}
	}
}

template Traits(alias S) {
	alias Traits = Traits!(None, None, void, void, __traits(getAttributes, S));
}

unittest {
	@peekSource!int @(100) @Id!"zombie"() @allocSink!(Id!1)
	struct Foo {}
	alias Tr = Traits!Foo;
	static assert(areSame!(Tr.Source, PeekSource));
	static assert(areSame!(Tr.SourceElementType, int));
	static assert(areSame!(Tr.Sink, AllocSink));
	static assert(areSame!(Tr.SinkElementType, Id!1));
}

unittest {
	@pullSource!int @pushSource!ubyte
	struct Bar {}
	static assert(!__traits(compiles, Traits!Bar)); // source interface specified twice
}

unittest {
	@pullSink!double @pushSink!string @peekSink!void
	struct Baz {}
	static assert(!__traits(compiles, Traits!Baz)); // sink interface specified 3x
}

enum isPullSource(alias S) = areSame!(Traits!S.Source, PullSource);
enum isPeekSource(alias S) = areSame!(Traits!S.Source, PeekSource);
enum isPushSource(alias S) = areSame!(Traits!S.Source, PushSource);
enum isAllocSource(alias S) = areSame!(Traits!S.Source, AllocSource);

enum isPullSink(alias S) = areSame!(Traits!S.Sink, PullSink);
enum isPeekSink(alias S) = areSame!(Traits!S.Sink, PeekSink);
enum isPushSink(alias S) = areSame!(Traits!S.Sink, PushSink);
enum isAllocSink(alias S) = areSame!(Traits!S.Sink, AllocSink);

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


/** Returns `true` if `S[0]` is a source and `S[1]` is a sink and they both use the same
 *  method of passing data.
 */
enum areCompatible(alias Source, alias Sink) =
	(isPeekSource!Source && isPeekSink!Sink)
		|| (isPullSource!Source && isPullSink!Sink)
		|| (isAllocSource!Source && isAllocSink!Sink)
		|| (isPushSource!Source && isPushSink!Sink);

template sourceMethod(alias S) {
	static if (isPeekSource!S) {
		alias sourceMethod = peekSource!(Traits!S.SourceElementType);
	} else static if (isPullSource!S) {
		alias sourceMethod = pullSource!(Traits!S.SourceElementType);
	} else static if (isAllocSource!S) {
		alias sourceMethod = allocSource!(Traits!S.SourceElementType);
	} else static if (isPushSource!S) {
		alias sourceMethod = pushSource!(Traits!S.SourceElementType);
	} else {
		bool sourceMethod() { return false; }
	}
}

template sinkMethod(alias S) {
	static if (isPeekSink!S) {
		alias sinkMethod = peekSink!(Traits!S.SinkElementType);
	} else static if (isPullSink!S) {
		alias sinkMethod = pullSink!(Traits!S.SinkElementType);
	} else static if (isAllocSink!S) {
		alias sinkMethod = allocSink!(Traits!S.SinkElementType);
	} else static if (isPushSink!S) {
		alias sinkMethod = pushSink!(Traits!S.SinkElementType);
	} else {
		bool sinkMethod() { return false; }
	}
}

unittest {
	@peekSource!double static struct Foo {}
	static assert(sourceMethod!Foo == peekSource!double);
}

mixin template TestScheduler() {
	static struct __Scheduler {
		void stop();
	}
	__Scheduler _flod_scheduler;

	int yield() { return 1; }
}

bool check(alias T)()
{
	if (__ctfe) {
		alias SoE = Traits!T.SourceElementType;
		alias SiE = Traits!T.SinkElementType;
		static if (isPushSink!T && isPullSource!T) {
			import flod.pipeline : AllWrapper;
			AllWrapper!(T!TestScheduler) t;
			SiE[15] pushbuf;
			SoE[15] pullbuf;
			static if (!__traits(compiles, t.push(pushbuf[])))
				t.push(pushbuf[]);
			static if (!__traits(compiles, t.pull(pullbuf[])))
				t.push(pullbuf[]);
		}
		else {
			pragma(msg, "@check not implemented for ", str!(Traits!T), " (", str!T, ")");
		}
	}
	return true;
}
