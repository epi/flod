/** Streams for reading and writing files.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod.file;


import flod.pipeline;
import flod.traits;
import flod.meta : NonCopyable;

struct MmappedFile {
	mixin NonCopyable;
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
static assert(isPeekSource!MmappedFile);

import std.stdio : File;

auto fromFile(File file)
{
	static struct FileReader {
		File file;
		size_t pull(T)(T[] buf)
		{
			return file.rawRead(buf).length;
		}
	}
	static assert (isPullSource!FileReader);
	return pipe!FileReader(file);
}

auto toFile(Pipeline)(auto ref Pipeline pipeline, File file)
	if (isPipeline!Pipeline)
{
	static struct FileWriter {
		File file;
		size_t push(T)(const(T)[] buf)
		{
			file.rawWrite(buf);
			return buf.length;
		}
	}
	static assert (isPushSink!FileWriter);
	return pipeline.pipe!FileWriter(file);
}

unittest
{
	import flod.adapter : pullPush;
	fromFile(File("/etc/passwd"))
		.pullPush(65536).toFile(File("dupa", "wb")).run();
}
