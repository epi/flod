///
module flod.meta;

package:

/// Used to compare alias lists.
struct Id(X...) {}

/// Tests if template `S` can be instantiated with argument list `A` and the instantiation is a type.
/// Workaround for https://issues.dlang.org/show_bug.cgi?id=15623
template isType(alias T, A...) {
	bool impl() { return is(T!A); }
	enum isType = impl();
}

unittest {
	struct NoFoo {}

	struct HasFoo {
		void foo() {}
	}

	struct CallsFoo(T) {
		T t;
		void run() { t.foo(); }
	}

	static assert(!isType!(CallsFoo, NoFoo));
	static assert( isType!(CallsFoo, HasFoo));
}

///
template str(W...) {
	static if (W.length > 1) {
		enum str = str!(W[0]) ~ "," ~ str!(W[1 .. $]);
	} else {
		alias V = W[0];
		static if (__traits(compiles, V.str))
			enum str = V.str;
		else static if (__traits(compiles, __traits(identifier, V)))
			enum str = __traits(identifier, V);
		else
			enum str = V.stringof;
	}
}

///
template ReplaceWithMask(ulong mask, ReplacementForZeros, Types...) {
	alias What = ReplacementForZeros;
	import std.meta : AliasSeq;
	static if (Types.length == 0)
		alias ReplaceWithMask = AliasSeq!();
	else {
		static if (mask & 1)
			alias ReplaceWithMask = AliasSeq!(ReplaceWithMask!(mask >> 1, What, Types[0 .. $ - 1]), Types[$ - 1]);
		else
			alias ReplaceWithMask = AliasSeq!(ReplaceWithMask!(mask >> 1, What, Types[0 .. $ - 1]), What);
	}
}

unittest {
	static struct Empty {}
	struct Z(Params...) {}
	alias List = ReplaceWithMask!(0b011011, Empty, int, bool, float, uint, ulong, double);
	static assert(is(Z!List == Z!(Empty, bool, float, Empty, ulong, double)));
}

public:

/// Mix it in inside a `struct` definition to make the `struct` non-copyable.
mixin template NonCopyable() {
	@disable this(this);
	@disable void opAssign(typeof(this));
}

///
template isCopyable(T) {
	enum isCopyable = is(typeof({ T a; T b = a; T c = b; }));
}

unittest {
	static struct A {}
	static struct B { @disable this(this); }
	static assert( isCopyable!A);
	static assert(!isCopyable!B);
}

///
auto moveIfNonCopyable(T)(auto ref T t)
{
	static if (isCopyable!T) {
		debug pragma(msg, "copying ", __traits(identifier, T), " (size=", t.sizeof, ")");
		return t;
	} else {
		import std.algorithm : move;
		debug pragma(msg, "moving ", __traits(identifier, T), " (size=", t.sizeof, ")");
		return move(t);
	}
}
