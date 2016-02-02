/** Stream iteration functions.
 */
module flod.range;

import std.stdio : File, KeepTerminator, writeln, writefln, stderr;
import std.traits : isScalarType;

import flod.stream : isStream;


auto byLine(Terminator = char, Char = char, Stream)(Stream s, KeepTerminator keepTerminator = KeepTerminator.no, Terminator terminator = '\x0a')
	if (isStream!Stream && isScalarType!Terminator)
{
	static struct ByLine
	{
		import flod.stream : RefCountedStream;
		RefCountedStream!Stream stream;
		Terminator term;
		bool keep;
		this(RefCountedStream!Stream stream, Terminator term, bool keep)
		{
			this.stream = stream;
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

		@property bool empty() { return line is null; }

		void popFront()
		{
			stream.consume(line.length);
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
	}
	auto r = ByLine(s.create(), terminator, keepTerminator == KeepTerminator.yes);
	r.next();
	return r;
}

unittest
{
	import flod.stream : stream;
	import std.range : take, array;
	auto testArray = "first line\nsecond line\nline without terminator";
	assert(stream(testArray).byLine(KeepTerminator.yes, 'e').array == [
		"first line", "\nse", "cond line", "\nline", " without te", "rminator" ]);
	assert(stream(testArray).byLine(KeepTerminator.no, 'e').array == [
		"first lin", "\ns", "cond lin", "\nlin", " without t", "rminator" ]);
	assert(stream(testArray).byLine!(char, char)(KeepTerminator.yes).array == [
		"first line\n", "second line\n", "line without terminator" ]);
	assert(stream(testArray).byLine!(char, char).array == [
		"first line", "second line", "line without terminator" ]);
	assert(stream(testArray).byLine(KeepTerminator.yes, 'z').array == [
		"first line\nsecond line\nline without terminator" ]);
	foreach (c; stream(testArray).byLine(KeepTerminator.yes, '\n'))
		c.writeln();
}
