/** Convert pipelines to ranges (of chunks, text lines, etc.).
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.range;

import std.stdio : File, KeepTerminator, writeln, writefln, stderr;
import std.traits : isScalarType;

import flod.pipeline : pipe, isImmediatePipeline;
import flod.meta : moveIfNonCopyable, NonCopyable;

auto byLine(Terminator = char, Char = char, Pipeline)(auto ref Pipeline pipeline,
	KeepTerminator keepTerminator = KeepTerminator.no, Terminator terminator = '\x0a')
	if (isImmediatePipeline!Pipeline && isScalarType!Terminator)
{
	static struct ByLine {
		Pipeline pipeline;
		Terminator term;
		bool keep;

		this()(auto ref Pipeline pipeline, Terminator term, bool keep)
		{
			this.pipeline = moveIfNonCopyable(pipeline);
			this.term = term;
			this.keep = keep;
			next();
		}

		const(Char)[] line;
		private void next()
		{
			size_t i = 0;
			alias DT = typeof(pipeline.peek(1)[0]);
			const(DT)[] buf = pipeline.peek(256);
			if (buf.length == 0) {
				line = null;
				return;
			}
			while (buf[i] != term) {
				i++;
				if (i >= buf.length) {
					buf = pipeline.peek(buf.length * 2);
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
			pipeline.consume(line.length);
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
				auto buf = pipeline.peek!Char(2);
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
							buf = pipeline.peek(i * 2);
							if (buf.length == i)
								return dg(buf[0 .. i]);
						}
					}
					if (int result = dg(buf[start .. i + (keep ? 1 : 0)]))
						return result;
					start = ++i;
				}
			cont:
				pipeline.consume!Char(start);
			}
			return 0;
		}
	}
	auto r = ByLine(moveIfNonCopyable(pipeline), terminator, keepTerminator == KeepTerminator.yes);

	return r;
}

unittest
{
	import std.range : take, array;
	auto testArray = "first line\nsecond line\nline without terminator";
	assert(pipe(testArray).byLine(KeepTerminator.yes, 'e').array == [
		"first line", "\nse", "cond line", "\nline", " without te", "rminator" ]);
	assert(pipe(testArray).byLine(KeepTerminator.no, 'e').array == [
		"first lin", "\ns", "cond lin", "\nlin", " without t", "rminator" ]);
	assert(pipe(testArray).byLine!(char, char)(KeepTerminator.yes).array == [
		"first line\n", "second line\n", "line without terminator" ]);
	assert(pipe(testArray).byLine!(char, char).array == [
		"first line", "second line", "line without terminator" ]);
	assert(pipe(testArray).byLine(KeepTerminator.yes, 'z').array == [
		"first line\nsecond line\nline without terminator" ]);
	foreach (c; pipe(testArray).byLine(KeepTerminator.yes, '\n'))
		c.writeln();
}
