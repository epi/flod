/** Stream iteration functions.
 */
module flod.range;

import std.stdio : File, KeepTerminator, writeln, writefln, stderr;
import std.traits : isScalarType;

import flod.pipeline : isPipeline;

auto byLine(Terminator = char, Char = char, Pipeline)(Pipeline pipeline, KeepTerminator keepTerminator = KeepTerminator.no, Terminator terminator = '\x0a')
	if (isPipeline!Pipeline && isScalarType!Terminator)
{
	import flod.stream : RefCountedStream, refCountedStream, stream;

	static struct ByLine {
		RefCountedStream!Pipeline stream;
		Pipeline pipeline;
		Terminator term;
		bool keep;

		this(Pipeline pipeline, Terminator term, bool keep)
		{
			this.pipeline = pipeline;
			this.term = term;
			this.keep = keep;
		}
		const(Char)[] line;

		private void next()
		{
			size_t i = 0;
			alias DT = typeof(stream.peek(1)[0]);
			const(DT)[] buf = stream.peek(256);
			if (buf.length == 0) {
				line = null;
				return;
			}
			while (buf[i] != term) {
				i++;
				if (i >= buf.length) {
					buf = stream.peek(buf.length * 2);
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
			if (line !is null)
				return false;
			if (stream.isNull) {
				stream = refCountedStream(pipeline);
				next();
				return false;
			}
			else {
				return true;
			}
		}

		void popFront()
		{
			assert(!stream.isNull);
			stream.consume(line.length);
			next();
		}

		@property const(Char)[] front()
		{
			assert(!stream.isNull);
			if (keep)
				return line;
			if (line.length > 0 && line[$ - 1] == term)
				return line[0 .. $ - 1];
			return line;
		}

		int opApply(scope int delegate(const(Char[]) ln) dg)
		{
			auto stream = pipeline.stream();
			for (;;) {
				auto buf = stream.peek!Char(2);
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
							buf = stream.peek(i * 2);
							if (buf.length == i)
								return dg(buf[0 .. i]);
						}
					}
					if (int result = dg(buf[start .. i + (keep ? 1 : 0)]))
						return result;
					start = ++i;
				}
			cont:
				stream.consume!Char(start);
			}
			return 0;
		}
	}

	auto r = ByLine(pipeline, terminator, keepTerminator == KeepTerminator.yes);
	return r;
}

unittest
{
	import flod.pipeline : pipe;
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
