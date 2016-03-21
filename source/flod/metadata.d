/** Pass metadata alongside the data stream.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.metadata;

import std.algorithm : isSorted;
import std.meta : Filter;
import std.typecons : tuple;

import flod.traits;

package enum TagOp { get, set }

package struct TagSpec(T, string k, TagOp o) {
	alias Type = T;
	enum string key = k;
	enum TagOp op = o;
}

enum tagSetter() = tuple().expand;
enum tagGetter() = tuple().expand;
enum tagSetter(T, string k, Z...) = tuple(TagSpec!(T, k, TagOp.set)(), tagSetter!Z).expand;
enum tagGetter(T, string k, Z...) = tuple(TagSpec!(T, k, TagOp.get)(), tagGetter!Z).expand;

private enum isTagSpec(S...) = is(typeof(S[0]) : TagSpec!_a, _a...);
package enum getTagSpecs(alias S) = Filter!(isTagSpec, __traits(getAttributes, S));

unittest {
	@tagSetter!(uint, "foo")
	@tagSetter!(string, "bar")
	@tagGetter!(double, "baz", string, "quux")
	@pushSink!uint
	static struct Foo {}
	enum x = getTagSpecs!Foo;
	static assert(x.length == 4);
	static assert(x[0] == TagSpec!(uint, "foo", TagOp.set)());
	static assert(x[1] == TagSpec!(string, "bar", TagOp.set)());
	static assert(x[2] == TagSpec!(double, "baz", TagOp.get)());
	static assert(x[3] == TagSpec!(string, "quux", TagOp.get)());
}

private struct Tag(T, string key, size_t[] setters)
	if (isSorted(setters))
{
	enum size_t length = setters.length;
	T[length] store;

	template storeIndex(size_t stageIndex, size_t left = 0, size_t right = length) {
		static if (right - left == 1) {
			enum storeIndex = left;
		} else {
			enum size_t m = (left + right) / 2;
			static if (stageIndex < setters[m])
				enum storeIndex = storeIndex!(stageIndex, left, m);
			else
				enum storeIndex = storeIndex!(stageIndex, m, right);
		}
	}

	void set(size_t index)(T value)
	{
		enum si = storeIndex!index;
		static assert(setters[si] == index);
		store[si] = value;
	}

	T get(size_t index)()
	{
		import std.conv : to;
		static assert(index > setters[0], "There is no setter for tag " ~ name
			~ " before stage #" ~ index.to!string);
		enum si = storeIndex!(index - 1);
		return store[si];
	}
}

unittest {
	Tag!(string, "sometag", [ 7, 13, 19, 32 ]) tag;
	tag.set!7("foo");
	tag.set!13("bar");
	tag.set!19("baz");
	tag.set!32("quux");
	static assert(!__traits(compiles, tag.set!5("fail")));
	static assert(!__traits(compiles, tag.set!14("fail")));
	static assert(!__traits(compiles, tag.get!0));
	static assert(!__traits(compiles, tag.get!7));
	assert(tag.get!8 == "foo");
	assert(tag.get!13 == "foo");
	assert(tag.get!14 == "bar");
	assert(tag.get!18 == "bar");
	assert(tag.get!19 == "bar");
	assert(tag.get!20 == "baz");
	assert(tag.get!32 == "baz");
	assert(tag.get!33 == "quux");
}

struct NullMetadata {
	enum bool isGetter(alias S) = false;
	enum bool isSetter(alias S) = false;
}
