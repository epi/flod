/** Top-level flod module. Provides the most commonly used stuff.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod;

import std.stdio : File, KeepTerminator, writeln, writefln, stderr;

struct FromArray(T)
{
	const(T)[] array;

	/* pull source with own buffer */
	const(T[]) peek(size_t length)
	{
		import std.algorithm : min;
		return array[0 .. min(length, array.length)];
	}

	void consume(size_t length)
	{
		array = array[length .. $];
	}
}

auto fromArray(T)(const(T[]) array) { return FromArray!T(array); }

struct PullBuffer(Source, T)
{
	pragma(msg, "PullBuffer: ", typeof(Source.init).stringof, ", ", T.stringof);
	Source source;

	T[] buf;
	size_t readOffset;
	size_t peekOffset;

	const(T)[] peek(size_t size)
	{
		if (peekOffset + size > buf.length) {
			buf.length = (peekOffset + size + 4095) & ~size_t(4095);
			writefln("PullBuffer grow %d", buf.length);
		}
		if (peekOffset + size > readOffset) {
			size_t r = source.pull(buf[readOffset .. $]);
			readOffset += r;
		}
		return buf[peekOffset .. $];
	}

	void consume(size_t size)
	{
		peekOffset += size;
		if (peekOffset == buf.length) {
			peekOffset = 0;
			readOffset = 0;
		}
	}
}

template SourceDataType(Source)
{
	private import std.traits : arity, ReturnType, Parameters = ParameterTypeTuple, isDynamicArray, ForeachType, Unqual;

	static if (__traits(compiles, arity!(Source.init.pull))
	        && arity!(Source.init.pull) == 1
	        && is(ReturnType!(Source.init.pull) == size_t)
	        && isDynamicArray!(Parameters!(Source.init.pull)[0])) {
		alias SourceDataType = Unqual!(ForeachType!(Parameters!(Source.init.pull)[0]));
	} else {
		static assert(0, Sink.stringof ~ " is not a proper sink type");
	}
}

auto pullBuffer(Source, T = SourceDataType!Source)(Source source)
{
	return PullBuffer!(Source, T)(source);
}

struct PushBuffer(Sink, T)
{
	pragma(msg, "PushBuffer: ", typeof(Sink.init).stringof, ", ", T.stringof);
	Sink sink;

	T[] buf;
	size_t allocOffset;

	T[] alloc(size_t size)
	{
		if (allocOffset + size > buf.length) {
			buf.length = allocOffset + size;
			writefln("PushBuffer grow %d", buf.length);
		}
		return buf[allocOffset .. $];
	}

	void commit(size_t size)
	{
		sink.push(buf[allocOffset .. size]);
		allocOffset += size;
		if (allocOffset == buf.length)
			allocOffset = 0;
	}
}

template SinkDataType(Sink)
{
	private import std.traits : arity, ReturnType, Parameters = ParameterTypeTuple, isDynamicArray, ForeachType, Unqual;

	static if (__traits(compiles, arity!(Sink.init.push))
	        && arity!(Sink.init.push) == 1
	        && is(ReturnType!(Sink.init.push) == void)
	        && isDynamicArray!(Parameters!(Sink.init.push)[0])) {
		alias SinkDataType = Unqual!(ForeachType!(Parameters!(Sink.init.push)[0]));
	} else {
		static assert(0, Sink.stringof ~ " is not a proper sink type");
	}
}

auto pushBuffer(Sink, T = SinkDataType!Sink)(Sink sink)
{
	return PushBuffer!(Sink, T)(sink);
}

struct CompactPullBuffer(Source, T = SourceDataType!Sink)
{
	import std.exception : enforce;
	import core.sys.posix.stdlib : mkstemp;
	import core.sys.posix.unistd : close, unlink, ftruncate;
	import core.sys.posix.sys.mman : mmap, munmap, MAP_ANON, MAP_PRIVATE, MAP_FIXED, MAP_SHARED, MAP_FAILED, PROT_WRITE, PROT_READ;

	private {
		Source source;
		void* buffer;
		size_t length;
		size_t peekOffset;
		size_t readOffset;
	}

	this(Source source)
	{
		enum order = 12;
		length = size_t(1) << order;
		int fd = createFile();
		scope(exit) close(fd);
		allocate(fd);
		this.source = source;
	}

	void dispose()
	{
		if (buffer) {
			munmap(buffer, length << 1);
			buffer = null;
		}
	}

	private int createFile()
	{
		static immutable path = "/dev/shm/flod-CompactPullBuffer-XXXXXX";
		char[path.length + 1] mutablePath = path.ptr[0 .. path.length + 1];
		int fd = mkstemp(mutablePath.ptr);
		enforce(fd >= 0, "Failed to create shm file");
		scope(failure) enforce(close(fd) == 0, "Failed to close shm file");
		enforce(unlink(mutablePath.ptr) == 0, "Failed to unlink shm file " ~ mutablePath);
		enforce(ftruncate(fd, length) == 0, "Failed to set shm file size");
		return fd;
	}

	private void allocate(int fd)
	{
		void* addr = mmap(null, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
		enforce(addr != MAP_FAILED, "Failed to mmap 1st part");
		scope(failure) munmap(addr, length);
		buffer = addr;
		addr = mmap(buffer + length, length, PROT_READ | PROT_WRITE, MAP_FIXED | MAP_SHARED, fd, 0);
		if (addr != buffer + length) {
			assert(addr == MAP_FAILED);
			addr = mmap(buffer - length, length, PROT_READ | PROT_WRITE, MAP_FIXED | MAP_SHARED, fd, 0);
			// TODO: if failed, try mmapping anonymous buf of 2 * length, then mmap the two copies inside it
			enforce(addr == buffer - length, "Failed to mmap 2nd part");
			buffer = addr;
		}
		writefln("%016x,%08x", buffer, length * 2);
	}

	const(T)[] peek(size_t size)
	{
		enforce(size <= length, "Growing buffer not implemented");
		auto buf = cast(T*) buffer;
		assert(readOffset <= peekOffset + 4096);
		while (peekOffset + size > readOffset)
			readOffset += source.pull(buf[readOffset .. readOffset]); //peekOffset + 4096]);
		assert(buf[0 .. length] == buf[length .. 2 *length]);
		stderr.writefln("%08x,%08x", peekOffset, readOffset - peekOffset);
		return buf[peekOffset .. readOffset];
	}

	void consume(size_t size)
	{
		assert(peekOffset + size <= readOffset);
		peekOffset += size;
		if (peekOffset >= length) {
			peekOffset -= length;
			readOffset -= length;
		}
		writefln("%08x %08x", peekOffset, readOffset);
	}
}

auto compactPullBuffer(Source, T = SourceDataType!Source)(Source source)
{
	return CompactPullBuffer!(Source, T)(source);
}

struct ToFile
{
	File file;

	void push(const(ubyte)[] buf)
	{
		file.rawWrite(buf[]);
	}
}

/* pull sink, push source */
struct RabbitStage
{
	import deimos.samplerate;

	SRC_STATE *state;
	int channels;
	double ratio;

	this(int type, int channels, double ratio)
	{
		int error;
		state = src_new(type, channels, &error);
		this.channels = channels;
		this.ratio = ratio;
	}

	void finalize()
	{
		src_delete(state);
	}

	void process(Source, Sink)(Source source, Sink sink)
	{
		SRC_DATA data;

		auto inbuf = source.peek(channels);
		auto outbuf = sink.alloc(channels);

		data.data_in = inbuf.ptr;
		data.data_out = outbuf.ptr;
		data.input_frames = inbuf.length / channels;
		data.output_frames = outbuf.length / channels;

		data.end_of_input = inbuf.length < channels * float.sizeof;

		data.src_ratio = ratio;

		src_process(state, &data);

		source.consume(data.input_frames_used * channels);
		sink.commit(data.output_frames_used * channels);
	}
}

enum isSource(T) = true;
enum isSink(T) = false;
enum isBufferedPullSource(T) = true;

// TODO: detect type of sink and itsSink
auto pushToPullBuffer(Sink, ItsSink)(Sink sink, ItsSink itsSink)
{
	return new PushToPullBuffer!(Sink, ItsSink)(sink, itsSink);
}
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