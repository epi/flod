/** Stream iteration functions.
 */
module flod.range;

auto byChunk(S)(S stream ) {
}


import std.stdio : File, KeepTerminator, writeln, writefln, stderr;

/+
struct InputStream(alias Comp, Type, T...)
{
	import std.typecons;
	static if (T.length) {
		Tuple!T tup;
	}

	static if (T.length) {
		this(T args)
		{
			tup = tuple(args);
		}
	}

	auto byLine(Terminator, Char = char)(KeepTerminator keepTerminator = KeepTerminator.no, Terminator terminator = '\x0a')
	{
		static struct Range
		{
			Type source;
			bool keep;
			Terminator term;
			this(bool keep, Terminator term, T r)
			{
				static if (T.length > 0)
					source = Type(r);
				this.keep = keep;
				this.term = term;
			}
			const(Char)[] line;

			private void next()
			{
				size_t i = 0;
				alias DT = typeof(source.peek(1)[0]);
				const(DT)[] buf = source.peek(256);
				if (buf.length == 0) {
					line = null;
					return;
				}
				while (buf[i] != term) {
					i++;
					if (i >= buf.length) {
						buf = source.peek(buf.length * 2);
						if (buf.length == i) {
							line = buf[];
							return;
						}
					}
				}
				line = buf[0 .. i + 1];
			}
			@property bool empty() { return line is null; }
			void popFront()
			{
				source.consume(line.length);
				next();
			};
			@property const(Char)[] front()
			{
				if (keep)
					return line;
				if (line.length > 0 && line[$ - 1] == term)
					return line[0 .. $ - 1];
				return line;
			}
		}
		auto r = Range(keepTerminator == KeepTerminator.yes, terminator, tup.expand);
		r.next();
		return r;
	}
}


import std.traits : isCallable;
auto stream(alias Comp, T...)(T args)
	if (isCallable!Comp || is(Comp) || isCallable!(Comp!T))
{
	alias Type = typeof(Comp(args));
	static if (isCallable!Comp || is(Comp)) {
		alias XComp = Comp;
	} else {
		alias XComp = Comp!T;
	}
	static if (isSource!Type && !isSink!Type)
		return InputStream!(XComp, Type, T)(args);
	else
		static assert(false);
	pragma(msg, Type.stringof);
}

unittest
{
	import std.range : take, array;
	auto testArray = "first line\nsecond line\nline without terminator";
	assert(stream!fromArray(testArray).byLine(KeepTerminator.yes, 'e').array == [
		"first line", "\nse", "cond line", "\nline", " without te", "rminator" ]);
	assert(stream!fromArray(testArray).byLine(KeepTerminator.no, 'e').array == [
		"first lin", "\ns", "cond lin", "\nlin", " without t", "rminator" ]);
	assert(stream!fromArray(testArray).byLine!(char, char)(KeepTerminator.yes).array == [
		"first line\n", "second line\n", "line without terminator" ]);
	assert(stream!fromArray(testArray).byLine!(char, char).array == [
		"first line", "second line", "line without terminator" ]);
	assert(stream!fromArray(testArray).byLine(KeepTerminator.yes, 'z').array == [
		"first line\nsecond line\nline without terminator" ]);
	foreach (c; stream!fromArray(testArray).byLine(KeepTerminator.yes, '\n'))
		c.writeln();
}
+/
