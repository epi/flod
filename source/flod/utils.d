/**
Types and functions for managing creation and copying of objects.

Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
Copyright: © 2016 Adrian Matoga
License: $(LINK2 http://boost.org/LICENSE_1_0.txt, Boost License 1.0).
*/
module flod.utils;

private struct DeferredConstructor(T, Args...) {
	Args args;
	T create() { return T(this.args); }
}

/**
Binds type `T` with constructor arguments `args`.
*/
auto createLater(T, Args...)(Args args)
{
	return DeferredConstructor!(T, Args)(args);
}

///
unittest {
	static struct Foo { @disable this(this); int x; string t; }
	static struct Bar { Foo foo; }
	auto dc = createLater!Foo(42, "blah");
	auto nc = dc;
	auto bar = Bar(nc.create());
	assert(bar.foo.x == 42);
	assert(bar.foo.t == "blah");
}
