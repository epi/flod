/** Various metaprogramming helpers.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.meta;

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
	static if (W.length == 0) {
		enum str = "()";
	} else static if (W.length > 1) {
		enum str = str!(W[0]) ~ "," ~ str!(W[1 .. $]);
	} else {
		import std.traits : isExpressions;
		static if (isExpressions!(W[0])) {
			import std.conv : to;
			enum str = to!string(W[0]);
		} else {
			alias V = W[0];
			static if (is(typeof(V.str) : string))
				enum str = V.str;
			else static if (__traits(compiles, __traits(identifier, V)))
				enum str = __traits(identifier, V);
			else
				enum str = V.stringof;
		}
	}
}

///
template repeat(int id, string s) {
	static if (id == 0)
		enum repeat = "";
	else
		enum repeat = s ~ repeat!(id - 1, s);
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

/// Mix it in inside a `struct` definition to make the `struct` non-copyable.
mixin template NonCopyable() {
	@disable this(this);
	@disable void opAssign(typeof(this));
}

/// Evaluates to true iff instances of `T` can be copied.
template isCopyable(T) {
	enum isCopyable = is(typeof({ T a; T b = a; T c = b; }));
}

unittest {
	static struct A {}
	static struct B { @disable this(this); }
	static assert( isCopyable!A);
	static assert(!isCopyable!B);
}

/// Forwards to `std.algorithm.move` iff `t` is non-copyable.
auto moveIfNonCopyable(T, string file = __FILE__, int line = __LINE__)(auto ref T t)
{
	import std.conv : to;
	static if (isCopyable!T) {
		debug pragma(msg,
			file ~ "(" ~ to!string(line) ~ "): copying " ~ str!T ~ " (size=" ~ to!string(t.sizeof) ~ ")");
		return t;
	} else {
		import std.algorithm : move;
		debug pragma(msg,
			file ~ "(" ~ to!string(line) ~ "): moving " ~ str!T ~ " (size=" ~ to!string(t.sizeof) ~ ")");
		return move(t);
	}
}
