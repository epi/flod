/** Convert pipelines to ranges (of chunks, text lines, etc.).
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.range;

version(none):

import std.stdio : File, KeepTerminator, writeln, writefln, stderr;
import std.traits : isScalarType;

import flod.pipeline : pipe, isImmediatePipeline;
import flod.meta : moveIfNonCopyable, NonCopyable;

auto byLine(Terminator = char, Char = char, Pipeline)(auto ref Pipeline pipeline,
	KeepTerminator keepTerminator = KeepTerminator.no, Terminator terminator = '\x0a')
	if (isImmediatePipeline!Pipeline && isScalarType!Terminator)
{
	static struct ByLine(Source) {
		Source source;
		Terminator term;
		bool keep;

		this()(auto ref Source source, Terminator term, bool keep)
		{
			this.source = moveIfNonCopyable(source);
			this.term = term;
			this.keep = keep;
			next();
		}

		const(Char)[] line;
		private void next()
		{
			size_t i = 0;
			alias DT = typeof(source.peek!Char(1)[0]);
			const(DT)[] buf = source.peek!Char(256);
			if (buf.length == 0) {
				line = null;
				return;
			}
			while (buf[i] != term) {
				i++;
				if (i >= buf.length) {
					buf = source.peek!Char(buf.length * 2);
					if (buf.length == i) {
						line = buf[];
						return;
					}
				}
			}
			line = buf[0 .. i + 1];
		}

		@property bool empty()
		{
			return line is null;
		}

		void popFront()
		{
			source.consume(line.length);
			next();
		}

		@property const(Char)[] front()
		{
			if (keep)
				return line;
			if (line.length > 0 && line[$ - 1] == term)
				return line[0 .. $ - 1];
			return line;
		}

		int opApply(scope int delegate(const(Char[]) ln) dg)
		{
			for (;;) {
				auto buf = source.peek!Char(2);
				size_t start = 0;
				size_t i = 0;
				if (buf.length == 0)
					return 0;
				for (;;) {
					while (buf[i] != term) {
						i++;
						if (i >= buf.length) {
							assert(i == buf.length);
							if (start > 0)
								goto cont;
							buf = source.peek!Char(i * 2);
							if (buf.length == i)
								return dg(buf[0 .. i]);
						}
					}
					if (int result = dg(buf[start .. i + (keep ? 1 : 0)]))
						return result;
					start = ++i;
				}
			cont:
				source.consume!Char(start);
			}
			return 0;
		}
	}
	auto r = pipeline.pipe!ByLine(terminator, keepTerminator == KeepTerminator.yes);

	return r;
}

unittest
{
	import std.range : take, array;
	auto testArray = "first line\nsecond line\nline without terminator";
	assert(testArray.byLine(KeepTerminator.yes, 'e').array == [
		"first line", "\nse", "cond line", "\nline", " without te", "rminator" ]);
	assert(testArray.byLine(KeepTerminator.no, 'e').array == [
		"first lin", "\ns", "cond lin", "\nlin", " without t", "rminator" ]);
	assert(testArray.byLine!(char, char)(KeepTerminator.yes).array == [
		"first line\n", "second line\n", "line without terminator" ]);
	assert(testArray.byLine!(char, char).array == [
		"first line", "second line", "line without terminator" ]);
	assert(testArray.byLine(KeepTerminator.yes, 'z').array == [
		"first line\nsecond line\nline without terminator" ]);
	foreach (c; testArray.byLine(KeepTerminator.yes, '\n'))
		c.writeln();
}
