/** Templates for declaring and examining static interfaces of pipeline stages.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://boost.org/LICENSE_1_0.txt, Boost License 1.0).
 */
module flod.traits;

import std.meta : AliasSeq, Filter, NoDuplicates, staticMap;
import flod.meta : str, Id;

/// Enumerates all methods of passing data between stages.
enum Method {
	pull = 0,  /// Sink requests that the source fill sink's buffer with data.
	peek = 1,  /// Sink requests a read-only view on source's buffer.
	push = 2,  /// Source requests that the sink accepts data in source's buffer.
	alloc = 3, /// Source requests a writable view on sink's buffer.
}

private enum nullMethod = cast(Method) -1;

struct MethodAttribute {
	Method sinkMethod = nullMethod;
	Method sourceMethod = nullMethod;
}

private struct TypedMethodAttribute(SinkE, SourceE) {
	alias SinkElementType = SinkE;
	alias SourceElementType = SourceE;
	MethodAttribute methods;
	alias methods this;
}

/// This attribute specifies that the stage is a source.
auto source(E = void)(Method source_method)
{
	return TypedMethodAttribute!(void, E)(
		MethodAttribute(nullMethod, source_method));
}

/// This attribute specifies that the stage is a sink.
auto sink(E = void)(Method sink_method)
{
	return TypedMethodAttribute!(E, void)(
		MethodAttribute(sink_method, nullMethod));
}

/// This attribute specifies that the stage is a filter.
auto filter(E...)(Method sink_method, Method source_method)
{
	auto ma = MethodAttribute(sink_method, source_method);
	static if (E.length == 0)
		return TypedMethodAttribute!(void,  void)(ma);
	else static if (E.length == 1)
		return TypedMethodAttribute!(void,  E[0])(ma);
	else static if (E.length == 2)
		return TypedMethodAttribute!(E[0], E[1])(ma);
	else
		static assert(0, "Too many types specified");
}

/// ditto
auto filter(E...)(Method method) { return filter!E(method, method); }

private template getMethodAttributes(alias S) {
	private {
		alias getTypeOf(Z...) = typeof(Z[0]);
		enum isNotVoid(S) = !is(S == void);
		enum isMethodAttribute(Z...) = is(typeof(Z[0]) == TypedMethodAttribute!(A, B), A, B);
		template getNthType(size_t n) {
			alias getNthType(M : TypedMethodAttribute!EL, EL...) = EL[n];
		}
		template getUntyped(Z...) {
			enum getUntyped = Z[0].methods;
		}
	}

	private deprecated {
		alias traits = Traits!(None, None, void, void, __traits(getAttributes, S));
		static if (is(Id!(traits.Source) == Id!PullSource))
			enum sourcem = Method.pull;
		else static if (is(Id!(traits.Source) == Id!PeekSource))
			enum sourcem = Method.peek;
		else static if (is(Id!(traits.Source) == Id!PushSource))
			enum sourcem = Method.push;
		else static if (is(Id!(traits.Source) == Id!AllocSource))
			enum sourcem = Method.alloc;
		static if (is(Id!(traits.Sink) == Id!PullSink))
			enum sinkm = Method.pull;
		else static if (is(Id!(traits.Sink) == Id!PeekSink))
			enum sinkm = Method.peek;
		else static if (is(Id!(traits.Sink) == Id!PushSink))
			enum sinkm = Method.push;
		else static if (is(Id!(traits.Sink) == Id!AllocSink))
			enum sinkm = Method.alloc;
	}
	static if (is(typeof(sinkm)) || is(typeof(sourcem))) {
		static if (!is(typeof(sinkm)))
			enum sinkm = nullMethod;
		static if (!is(typeof(sourcem)))
			enum sourcem = nullMethod;
		deprecated alias deprecatedAttributes =
			AliasSeq!(filter!(traits.SinkElementType, traits.SourceElementType)(sinkm, sourcem));
		pragma(msg, "Deprecated attributes on ", .str!S);
	} else
		alias deprecatedAttributes = AliasSeq!();

	alias typed = AliasSeq!(deprecatedAttributes, Filter!(isMethodAttribute, __traits(getAttributes, S)));
	enum untyped = [ staticMap!(getUntyped, typed) ];

	private {
		alias allNthTypes(size_t n) =
			NoDuplicates!(
				Filter!(
					isNotVoid,
					staticMap!(
						getNthType!n,
						staticMap!(getTypeOf, typed))));
		alias SinkElementTypes = allNthTypes!0;
		alias SourceElementTypes = allNthTypes!1;
	}

	alias SinkElementType = AliasSeq!(SinkElementTypes, void)[0];
	alias SourceElementType = AliasSeq!(SourceElementTypes, void)[0];

	// check if all attributes define the same pair of types (or void, i.e. unspecified)
	static assert(SourceElementTypes.length <= 1,
		"Conflicting source element types specified: " ~ SourceElementTypes.stringof);
	static assert(SinkElementTypes.length <= 1,
		"Conflicting sink element types specified: " ~ SinkElementTypes.stringof);

	static if (untyped.length >= 2) {
		// check if methods for different kinds of stages aren't mixed
		static assert({
				import std.algorithm : reduce, map;
				int mask(MethodAttribute ma)
				{
					return ((ma.sinkMethod != nullMethod) ? 2 : 0)
						| ((ma.sourceMethod != nullMethod) ? 1 : 0);
				}
				return reduce!((a, b) => a | b)(0, untyped.map!mask)
					== reduce!((a, b) => a & b)(3, untyped.map!mask); }(),
			"A stage must be either a source, a sink or a filter - not a mix thereof");
	}
}

///
enum getMethods(alias S) = getMethodAttributes!S.untyped;

///
alias SinkElementType(alias S) = getMethodAttributes!S.SinkElementType;

///
alias SourceElementType(alias S) = getMethodAttributes!S.SourceElementType;

/// Gets the element type at source or sink end of i-th stage in `StageSeq`.
template SourceElementType(size_t i, StageSeq...) {
	alias E = SourceElementType!(StageSeq[i]);
	static if (!is(E == void))
		alias SourceElementType = E;
	else static if (i == StageSeq.length - 1)
		alias SourceElementType = SinkElementType!(i, StageSeq);
	else {
		alias W = SinkElementType!(StageSeq[i]);
		static if (!is(W == void))
			alias SourceElementType = W;
		else
			alias SourceElementType = SinkElementType!(i, StageSeq);
	}
}

/// ditto
template SinkElementType(size_t i, StageSeq...) {
	alias E = SinkElementType!(StageSeq[i]);
	static if (is(E == void) && i > 0)
		alias SinkElementType = SourceElementType!(i - 1, StageSeq);
	else
		alias SinkElementType = E;
}

///
unittest {
	@source!double(Method.pull) struct Foo {}
	@filter(Method.pull) struct Bar {}
	@sink(Method.pull) struct Baz {}
	static assert(is(SourceElementType!(0, Foo, Baz) == double));
	static assert(is(SinkElementType!(2, Foo, Bar, Baz) == double));
}

unittest {
	struct Foo {}
	static assert(getMethods!Foo == []);
	static assert(is(getMethodAttributes!Foo.SinkElementType == void));
	static assert(is(getMethodAttributes!Foo.SourceElementType == void));
}

unittest {
	@pushSink!int @pullSource!ulong
	struct Foo {}
	static assert(getMethods!Foo == [ filter(Method.push, Method.pull) ]);
	static assert(is(getMethodAttributes!Foo.SinkElementType == int));
	static assert(is(getMethodAttributes!Foo.SourceElementType == ulong));
}

unittest {
	@allocSink!int
	@sink(Method.push)
	struct Foo {}
	static assert(getMethods!Foo == [ sink(Method.alloc), sink(Method.push) ]);
	static assert(is(SinkElementType!Foo == int));
}

unittest {
	@source(Method.push) @sink(Method.push)
	struct Foo {}
	static assert(!__traits(compiles, getMethods!Foo)); // error: specified both source and sink attributes
}

unittest {
	@source!int(Method.push) @source!double(Method.pull)
	struct Foo {}
	static assert(!__traits(compiles, getMethods!Foo)); // error: conflicting source types (int, double)
}

unittest {
	@filter!(int, int)(Method.push) @filter!(double, int)(Method.push)
	struct Foo {}
	static assert(!__traits(compiles, getMethods!Foo)); // error: conflicting sink types (int, double)
}

deprecated {

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

bool implementsMethod(string endp)(Method m, MethodAttribute[] attrs...) {
	foreach (attr; attrs) {
		mixin(`bool im = attr.` ~ endp ~ `Method == m;`);
		if (im) return true;
	}
	return false;
}

enum isPullSource(alias S) = areSame!(Traits!S.Source, PullSource) || implementsMethod!`source`(Method.pull, getMethods!S);
enum isPeekSource(alias S) = areSame!(Traits!S.Source, PeekSource) || implementsMethod!`source`(Method.peek, getMethods!S);
enum isPushSource(alias S) = areSame!(Traits!S.Source, PushSource) || implementsMethod!`source`(Method.push, getMethods!S);
enum isAllocSource(alias S) = areSame!(Traits!S.Source, AllocSource) || implementsMethod!`source`(Method.alloc, getMethods!S);

enum isPullSink(alias S) = areSame!(Traits!S.Sink, PullSink) || implementsMethod!`sink`(Method.pull, getMethods!S);
enum isPeekSink(alias S) = areSame!(Traits!S.Sink, PeekSink) || implementsMethod!`sink`(Method.peek, getMethods!S);
enum isPushSink(alias S) = areSame!(Traits!S.Sink, PushSink) || implementsMethod!`sink`(Method.push, getMethods!S);
enum isAllocSink(alias S) = areSame!(Traits!S.Sink, AllocSink) || implementsMethod!`sink`(Method.alloc, getMethods!S);

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

enum isStage(alias S) = isSource!S || isSink!S;

/** Returns `true` if `Source` is a source and `Sink` is a sink and they both use the same
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
