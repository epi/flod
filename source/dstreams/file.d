/** Streams for reading and writing files.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module dstreams.file;

struct MmappedFile {
	import std.string : toStringz;
	import std.exception : enforce;
	import core.sys.posix.fcntl : open, O_RDONLY;
	import core.sys.posix.unistd : close;
	import core.sys.posix.sys.mman : mmap, munmap, MAP_ANON, MAP_PRIVATE, MAP_FAILED, PROT_READ;
	import core.sys.posix.sys.stat : stat_t, fstat;

	private const(ubyte)[] stream;
	private const(ubyte)[] mmfile;

	this(string name)
	{
		int fd = open(name.toStringz(), O_RDONLY);
		scope(exit) close(fd);
		enforce(fd >= 0);
		stat_t st;
		enforce(fstat(fd, &st) == 0);
		void* ptr = mmap(null, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
		enforce(ptr != MAP_FAILED);
		mmfile = (cast(const(ubyte*)) ptr)[0 .. st.st_size];
		stream = mmfile;
	}

	const(ubyte)[] peek(size_t n) pure nothrow
	{
		return stream;
	}

	void consume(size_t n) pure nothrow
	{
		assert(n <= stream.length);
		stream = stream[n .. $];
	}

	~this() nothrow
	{
		if (mmfile.ptr)
			munmap(cast(void*) mmfile.ptr, mmfile.length);
	}
}

struct Xor(Source) {
	Source source;

	uint val;

	this(uint val)
	{
		this.val = val;
	}

	size_t pull(ubyte[] buf) pure nothrow
	{
		ubyte val = cast(ubyte) this.val;
		auto ib = source.peek(buf.length);
		foreach (i, b; ib)
			buf[i] = b ^ val;
		source.consume(ib.length);
		return ib.length;
	}
}

struct FileReader {
	import std.stdio : File;
	File file;

	this(File file)
	{
		this.file = file;
	}

	this(in char[] name)
	{
		this.file.__ctor(name, "rb");
	}

	size_t pull(ubyte[] buf)
	{
		return file.rawRead(buf).length;
	}
}
static assert(isUnbufferedPullSource!FileReader, Why!(FileReader).isNotUnbufferedPullSource);

struct FileWriter {
	import std.stdio : File;
	File file;

	this(File file)
	{
		this.file = file;
	}

	this(in char[] name)
	{
		this.file.__ctor(name, "wb");
	}

	size_t push(const(ubyte)[] buf)
	{
		file.rawWrite(buf);
		return buf.length;
	}
}

struct ByLine(Source) {
	Source source;

	const(char)[] line;

	void init()
	{
		next();
	}

	void next()
	{
		line = cast(const(char)[]) source.peek(4096);
		if (line.length == 0) {
			line = null;
			return;
		}
		foreach (i, c; line)
		{
			if (c == '\n') {
				line = line[0 .. i + 1];
				return;
			}
		}
	}

	bool empty()
	{
		return line == null;
	}

	void popFront()
	{
		source.consume(line.length);
		next();
	}

	auto front()
	{
		return line;
	}
}

import dstreams.stream;
import dstreams.traits;

unittest
{
	import dstreams.common : take, drop, NullSource;
	auto s = stream!FileReader("/etc/passwd").drop(3).pipe!PullPush.take(3).pipe!FileWriter("ep.out");
	s.run();
	stream!NullSource.pipe!PullPush.pipe!FileWriter("empty_file").run();
}

void test_fw()
{
	//import std.algorithm : swap;
	auto file = MmappedFile("/etc/passwd");
	//auto xor = Xor!(MmappedFile)(0);
	//xor.source = &file;
	//swap(xor.source, file);
	//auto fw = FileWriter!(typeof(xor))("test.out");
	//swap(fw.source, xor);
	//fw.source = &xor;
	//fw.pull();
	ByLine!(MmappedFile*) bl;
	bl.source = &file;
	bl.init;
	import std.stdio;
	foreach (line; bl) {
		write(line);
	}
}
