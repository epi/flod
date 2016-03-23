/** Pass metadata alongside the data stream.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.metadata;

import std.algorithm : isSorted;
import std.meta : AliasSeq, Filter, allSatisfy, anySatisfy, staticMap;
import std.typecons : Tuple, tuple;

import flod.traits;

package enum TagOp { get, set }

package struct TagAttribute(T, string k, TagOp o) {
	alias Type = T;
	enum string key = k;
	enum TagOp op = o;
}

enum tagSetter() = tuple().expand;
enum tagGetter() = tuple().expand;
enum tagSetter(T, string k, Z...) = tuple(TagAttribute!(T, k, TagOp.set)(), tagSetter!Z).expand;
enum tagGetter(T, string k, Z...) = tuple(TagAttribute!(T, k, TagOp.get)(), tagGetter!Z).expand;

private enum isTagAttribute(S...) = is(typeof(S[0]) : TagAttribute!_a, _a...);
package enum getTagAttributes(S...) = tuple(Filter!(isTagAttribute, __traits(getAttributes, S[0]))).expand;

unittest {
	static struct Bar {}
	enum x = getTagAttributes!Bar;
	static assert(x.length == 0);
}

unittest {
	@tagSetter!(uint, "foo")
	@tagSetter!(string, "bar")
	@tagGetter!(double, "baz", string, "quux")
	@pushSink!uint
	static struct Foo {}
	enum x = getTagAttributes!Foo;
	static assert(x.length == 4);
	static assert(x[0] == TagAttribute!(uint, "foo", TagOp.set)());
	static assert(x[1] == TagAttribute!(string, "bar", TagOp.set)());
	static assert(x[2] == TagAttribute!(double, "baz", TagOp.get)());
	static assert(x[3] == TagAttribute!(string, "quux", TagOp.get)());
}

/// Bundles stage index and its TagAttributes
private template TagAttributeTuple(size_t i, ta...)
	if (allSatisfy!(isTagAttribute, ta))
{
	enum size_t index = i;
	enum tagAttributes = tuple(ta).expand;

	static if (tagAttributes.length) {
		enum front = tagAttributes[0];
		alias removeFront = TagAttributeTuple!(index, tagAttributes[1 .. $]);
	} else {
		alias removeFront = TagAttributeTuple!(index);
	}
}

/** Extracts tag attributes from a sequence of stages.
Params:
i = index of first stage in StageSeq
StageSeq = sequence of stages
*/
template FilterTagAttributes(size_t i, StageSeq...)
	if (allSatisfy!(isStage, StageSeq))
{
	static if (StageSeq.length) {
		alias tags = getTagAttributes!(StageSeq[0]);
		static if (tags.length)
			alias FilterTagAttributes = AliasSeq!(TagAttributeTuple!(i, getTagAttributes!(StageSeq[0])),
				.FilterTagAttributes!(i + 1, StageSeq[1 .. $]));
		else
			alias FilterTagAttributes = .FilterTagAttributes!(i + 1, StageSeq[1 .. $]);
	} else {
		alias FilterTagAttributes = AliasSeq!();
	}
}

/// Bundles tag value type, key, and indexes of all setters
private template TagSpec(T, string k, size_t[] s)
	if (isSorted(s))
{
	alias Type = T;
	enum string key = k;
	enum setters = s;
}

private enum isTagSpec(S...) = is(S[0].Type)
	&& is(typeof(S[0].key) == string) && is(typeof(S[0].setters) == size_t[]);

unittest {
	static assert( isTagSpec!(TagSpec!(double, "foo", [ 1, 4, 5 ])));
	static assert(!isTagSpec!());
	static assert(!isTagSpec!2);
	static assert(!isTagSpec!(Id!int));
}

template hasKey(string k) {
	enum bool hasKey(alias S) = S.key == k;
}

template TagSpecByKey(string k, tagSpecs...)
	if (allSatisfy!(isTagSpec, tagSpecs))
{
	alias TagSpecByKey = Filter!(hasKey!k, tagSpecs);
}

unittest {
	alias specs = AliasSeq!(
		TagSpec!(uint, "foo", [ 1, 2, 15 ]),
		TagSpec!(uint, "bar", [ 5 ]),
		TagSpec!(uint, "baz", [ 7, 42 ]));
	alias bar = TagSpecByKey!("bar", specs);
	static assert(is(Id!bar == Id!(specs[1])));
}

template RemoveTagSpecByKey(string k, tagSpecs...)
	if (allSatisfy!(isTagSpec, tagSpecs))
{
	static if (tagSpecs.length == 0)
		alias RemoveTagSpecByKey = AliasSeq!();
	else static if (tagSpecs[0].key == k)
		alias RemoveTagSpecByKey = RemoveTagSpecByKey!(k, tagSpecs[1 .. $]);
	else
		alias RemoveTagSpecByKey = AliasSeq!(tagSpecs[0], RemoveTagSpecByKey!(k, tagSpecs[1 .. $]));
}

unittest {
	alias specs = AliasSeq!(
		TagSpec!(uint, "foo", [ 1, 2, 15 ]),
		TagSpec!(uint, "bar", [ 5 ]),
		TagSpec!(uint, "baz", [ 7, 42 ]));
	alias nobar = RemoveTagSpecByKey!("bar", specs);
	static assert(is(Id!nobar == Id!(specs[0], specs[2])));
}

template MergeTagSpecs(alias NewSpec, tagSpecs...)
	if (allSatisfy!(isTagSpec, NewSpec, tagSpecs))
{
	alias MergeTagSpecs = AliasSeq!(NewSpec, RemoveTagSpecByKey!(NewSpec.key, tagSpecs));
}

/** Transposes a sequence of (index, tag_attributes...) tuples into a sequence of TagSpecs
*/
template TagSpecSeq(TagAttributeTupleSeq...) {
	static if (TagAttributeTupleSeq.length == 0)
		alias TagSpecSeq = AliasSeq!();
	else static if (TagAttributeTupleSeq[0].tagAttributes.length == 0)
		alias TagSpecSeq = .TagSpecSeq!(TagAttributeTupleSeq[1 .. $]);
	else {
		alias AttrTuple = TagAttributeTupleSeq[0];
		enum index = AttrTuple.index;
		alias Type = AttrTuple.front.Type;
		enum key = AttrTuple.front.key;
		enum op = AttrTuple.front.op;
		alias Tail = .TagSpecSeq!(AliasSeq!(AttrTuple.removeFront, TagAttributeTupleSeq[1 .. $]));
		alias Spec = TagSpecByKey!(key, Tail);

		static assert(allSatisfy!(isTagSpec, Spec, Tail));
		static assert(Spec.length == 0 || is(Spec[0].Type == Type), "Conflicting types for tag `" ~ key ~ "`: "
			~ str!(Spec[0].Type) ~ " and " ~ str!Type);

		static if (op != TagOp.set)
			alias TagSpecSeq = Tail;
		else static if (Spec.length == 0)
			alias TagSpecSeq = AliasSeq!(TagSpec!(Type, key, [ index ]), Tail);
		else static if (Spec.length == 1)
			alias TagSpecSeq = MergeTagSpecs!(TagSpec!(Type, key, [ index ] ~ Spec[0].setters), Tail);
		else
			static assert(0, "bug");
	}
}

unittest {
	alias stageTags = AliasSeq!(
		TagAttributeTuple!(3, tagSetter!(uint, "foo")),
		TagAttributeTuple!(4, tagSetter!(string, "bar")),
		TagAttributeTuple!(5, tagGetter!(uint, "foo"), tagSetter!(uint, "foo")),
		TagAttributeTuple!(6, tagGetter!(uint, "foo", string, "bar")));
	alias specs = TagSpecSeq!stageTags;
	static assert(specs.length == 2);
	static assert(is(Id!specs == Id!(
		TagSpec!(uint, "foo", [ 3, 5 ]),
		TagSpec!(string, "bar", [ 4 ]))));
}

/// Structure that holds values of a single tag for all subranges of a pipeline.
private struct Tag(alias Spec) {
	alias T = Spec.Type;
	enum size_t length = Spec.setters.length;
	T[length] store;

	template storeIndex(size_t stageIndex, size_t left = 0, size_t right = length) {
		static if (right - left == 1) {
			enum storeIndex = left;
		} else {
			enum size_t m = (left + right) / 2;
			static if (stageIndex < Spec.setters[m])
				enum storeIndex = storeIndex!(stageIndex, left, m);
			else
				enum storeIndex = storeIndex!(stageIndex, m, right);
		}
	}

	void set(size_t index)(T value)
	{
		enum si = storeIndex!index;
		static assert(Spec.setters[si] == index);
		store[si] = value;
	}

	T get(size_t index)()
	{
		import std.conv : to;
		static assert(index > Spec.setters[0], "There is no setter for tag " ~ name
			~ " before stage #" ~ index.to!string);
		enum si = storeIndex!(index - 1);
		return store[si];
	}
}

unittest {
	Tag!(TagSpec!(string, "sometag", [ 7, 13, 19, 32 ])) tag;
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

private string escape(string s)
{
	assert(__ctfe);
	import std.array : appender;
	import std.format : formattedWrite;
	import std.ascii : isAlpha, isAlphaNum;
	auto app = appender!string();
	app.put("_");
	if (s.length == 0)
		return app.data;
	if (s[0].isAlpha)
		app.put(s[0]);
	else
		app.formattedWrite("_%02X", s[0]);
	for (;;) {
		s = s[1 .. $];
		if (s.length == 0)
			break;
		if (s[0].isAlphaNum)
			app.put(s[0]);
		else
			app.formattedWrite("_%02X", s[0]);
	}
	return app.data;
}

unittest {
	static assert("".escape == "_");
	static assert("bar".escape == "_bar");
	static assert("5foo".escape == "__35foo");
	static assert("_foo.bar".escape == "__5Ffoo_2Ebar");
}

/// Structure that holds values of all tags for all subranges in a pipeline.
private struct TagTuple(tagSpecs...)
	if (allSatisfy!(isTagSpec, tagSpecs))
{
	alias specMap(alias spec) = AliasSeq!(Tag!spec, spec.key.escape);
	alias Tup = Tuple!(staticMap!(specMap, tagSpecs));
	Tup tags;

	alias ValueType(string key) = TagSpecByKey!(key, tagSpecs)[0].Type;

	ValueType!k get(string k, size_t i)()
	{
		import std.conv : to;
		mixin("return tags." ~ k.escape ~ ".get!(" ~ i.to!string ~ ");");
	}

	void set(string k, size_t i)(ValueType!k value)
	{
		import std.conv : to;
		mixin("tags." ~ k.escape ~ ".set!(" ~ i.to!string ~ ")(value);");
	}
}

unittest {
	alias specs = AliasSeq!(TagSpec!(uint, "foo.bar", [ 3, 5, 17 ]), TagSpec!(string, "bar.baz", [ 4, 5, 13 ]));
	alias T = TagTuple!specs;
	static assert(is(T.ValueType!"foo.bar" == uint));
	static assert(is(T.ValueType!"bar.baz" == string));
	T tup;
	tup.set!("foo.bar", 3)(42);
	assert(tup.get!("foo.bar", 4) == 42);
}

template hasOp(TagOp op) {
	enum hasOp(alias Attr) = Attr.op == op;
}

///
package struct Metadata(TagAttributeTuples...) {
private:
	template test(size_t i, TagOp op) {
		enum test(alias S) = S.index == i && anySatisfy!(hasOp!op, S.tagAttributes);
	}

	enum bool isGetter(size_t index) = anySatisfy!(test!(index, TagOp.get), TagAttributeTuples);
	enum bool isSetter(size_t index) = anySatisfy!(test!(index, TagOp.set), TagAttributeTuples);

	alias Tup = TagTuple!(TagSpecSeq!TagAttributeTuples);
	Tup tup;

public:
	///
	alias ValueType(string key) = Tup.ValueType!key;

	void set(string key, size_t index)(ValueType!key value) { tup.set!(key, index)(value); }
	ValueType!key get(string key, size_t index)() { return tup.get!(key, index); }
}