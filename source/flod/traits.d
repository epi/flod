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
pure nothrow @nogc const:
	@property bool isActiveSource()
	{
		return sourceMethod == Method.push || sourceMethod == Method.alloc;
	}
	@property bool isPassiveSource()
	{
		return sourceMethod == Method.pull || sourceMethod == Method.peek;
	}
	@property bool isActiveSink()
	{
		return sinkMethod == Method.pull || sinkMethod == Method.peek;
	}
	@property bool isPassiveSink()
	{
		return sinkMethod == Method.push || sinkMethod == Method.alloc;
	}
	@property bool isDriver()
	{
		return (isActiveSource && !isPassiveSink) || (isActiveSink && !isPassiveSource);
	}
	@property bool isPassiveFilter()
	{
		return isPassiveSource && isPassiveSink;
	}
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

	alias typed = AliasSeq!(Filter!(isMethodAttribute, __traits(getAttributes, S)));
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
	@filter!(int, ulong)(Method.push, Method.pull)
	struct Foo {}
	static assert(getMethods!Foo == [ filter(Method.push, Method.pull) ]);
	static assert(is(getMethodAttributes!Foo.SinkElementType == int));
	static assert(is(getMethodAttributes!Foo.SourceElementType == ulong));
}

unittest {
	@sink!int(Method.alloc)
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

private bool implementsMethod(string endp)(Method m, MethodAttribute[] attrs...) {
	foreach (attr; attrs) {
		mixin(`bool im = attr.` ~ endp ~ `Method == m;`);
		if (im) return true;
	}
	return false;
}

enum isPullSource(alias S) = implementsMethod!`source`(Method.pull, getMethods!S);
enum isPeekSource(alias S) = implementsMethod!`source`(Method.peek, getMethods!S);
enum isPushSource(alias S) = implementsMethod!`source`(Method.push, getMethods!S);
enum isAllocSource(alias S) = implementsMethod!`source`(Method.alloc, getMethods!S);

enum isPullSink(alias S) = implementsMethod!`sink`(Method.pull, getMethods!S);
enum isPeekSink(alias S) = implementsMethod!`sink`(Method.peek, getMethods!S);
enum isPushSink(alias S) = implementsMethod!`sink`(Method.push, getMethods!S);
enum isAllocSink(alias S) = implementsMethod!`sink`(Method.alloc, getMethods!S);

unittest {
	@filter!(bool, int)(Method.pull)
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
