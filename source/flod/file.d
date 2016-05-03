/** File I/O pipes.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.file;

import std.stdio : File;

import flod.traits : source, sink, Method;
import flod.pipeline : isSchema;

@source!ubyte(Method.pull)
private struct FileReader(alias Context, A...) {
	mixin Context!A;
private:
	File file;

public:
	this(in char[] name) { file = File(name, "rb"); }

	size_t pull()(ubyte[] buf)
	{
		return file.rawRead(buf).length;
	}
}

/// Returns a pipe that reads `ubyte`s from file `name`.
auto read(in char[] name)
{
	import flod.pipeline : pipe;
	return pipe!FileReader(name);
}

@sink(Method.push)
private struct FileWriter(alias Context, A...) {
	mixin Context!A;
private:
	File file;

public:
	this(File file) { this.file = file; }
	this(in char[] name) { file = File(name, "wb"); }

	size_t push(const(InputElementType)[] buf)
	{
		file.rawWrite(buf);
		return buf.length;
	}
}

/// Returns a pipe that writes to file `name`.
auto write(S)(S schema, in char[] name)
	if (isSchema!S)
{
	import flod.pipeline : pipe;
	return schema.pipe!FileWriter(name);
}

/// Returns a pipe that writes to file `file`.
auto write(S)(S schema, File file)
	if (isSchema!S)
{
	import flod.pipeline : pipe;
	return schema.pipe!FileWriter(file);
}

unittest {
	import std.file : remove, exists, stdread = read;
	scope(exit) {
		if (exists(".test"))
			remove(".test");
	}
	auto f1 = stdread("/etc/passwd");

	read("/etc/passwd").write(".test");
	auto f2 = stdread(".test");
	assert(f1 == f2);

	read("/etc/passwd").write(File(".test", "wb"));
	auto f3 = stdread(".test");
	assert(f1 == f3);
}
