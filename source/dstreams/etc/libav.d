/** Reading and writing multimedia streams using $(LINK2 https://libav.org/,libav).
 *
 * Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 * Copyright: © 2016 Adrian Matoga
 * License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0)
 */
module dstreams.etc.libav;

import dstreams.etc.bindings.libavutil.file;
import dstreams.etc.bindings.libavutil.mem;
import dstreams.etc.bindings.libavformat.avformat;
import dstreams.etc.bindings.libavformat.avio;

import std.exception : enforce;

///
struct AVMappedFile
{
	ubyte* buf;
	size_t size;

	this(string c)
	{
		import std.string : toStringz;
		auto s = c.toStringz();
		enforce(av_file_map(s, &buf, &size, 0, null) == 0);
	}

	@disable this(this);

	~this()
	{
		av_file_unmap(buf, size);
	}

	const(ubyte)[] peek(size_t s)
	{
		return buf[0 .. size];
	}

	void consume(size_t s)
	{
		assert(s < size);
		buf += s;
		size -= s;
	}
}

import std.stdio;

///
struct AVDemux(alias Source) {
	AVIOContext* ioCtx;
	AVFormatContext* formatCtx;
	void* ioBuffer;
	Throwable e;

	shared static this()
	{
		av_register_all();
	}

	int read(ubyte* buf, int length)
	{
		try {
			stderr.writefln("Read %d", length);
			return cast(int) Source.pull(buf[0 .. length]);
		}
		catch (Throwable e) {
			this.e = e;
			return -1;
		}
	}

	extern(C) static int readPacket(void* opaque, ubyte* buf, int length)
	{
		stderr.writefln("readPacket %d", length);
		auto p = cast(AVDemux*) opaque;
		return p.read(buf, length);
	}

	void initialize() {
		formatCtx = avformat_alloc_context();
		enforce(formatCtx !is null);
		stderr.writeln("avformat_alloc_context ok");
		scope(failure) if (formatCtx)avformat_close_input(&formatCtx);
		ioBuffer = av_malloc(4096);
		enforce(ioBuffer !is null);
		scope(failure) if (ioBuffer) av_freep(&ioBuffer);
		stderr.writeln("avmalloc ok");
		ioCtx = avio_alloc_context(cast(ubyte*) ioBuffer, 4096, 0, &this, &readPacket, null, null);
		enforce(ioCtx !is null);
		stderr.writeln("avio_alloc_context ok");
		formatCtx.pb = ioCtx;
		int ret = avformat_open_input(&formatCtx, null, null, null);
		enforce (ret >=0);
		stderr.writeln("avformat_open_input ok");
		ret = avformat_find_stream_info(formatCtx, null);
		enforce( ret >=0);
		stderr.writeln("avformat_find_stream_info ok");
		av_dump_format(formatCtx, 0, null, 0);
	}

	~this()
	{
		if (ioCtx)
			av_freep(cast(void**) &ioCtx);
		if (formatCtx)
			avformat_close_input(&formatCtx);
	}

	@disable this(this);


}

struct InputStream(TypSrc, Src, T...)
{
	import std.typecons : Tuple;

	Tuple!T tup;

	static if (T.length) {
		this(T args)
		{
			tup = args;
		}
	}

	auto pipe(alias X, U...)(U args) {
		static if (is(X!(TypSrc.init))) {
			auto w = Src(tup.expand);
			auto xw = X!w();
			xw.initialize();
			pragma(msg, "all good");
		} else {
			static assert(0, "cannot connect " ~ X.stringof ~ " to source type " ~ TypSrc.stringof);
		}
		return 1;//
	}
}

struct Pipeline(Src)
{

}

/+

// the following is an idea for an alternative, shorter syntax, more similar to range-based code

template UnbufferedPullSource(Src)
{
	static if (isUnbufferedPullSource!Src) {
		alias UnbufferedPullSource = Src;
	} else if (isBufferedPullSource!Src) {
		struct UnbufferedPullSource {
			Src src;
			size_t pull(ubyte[] buf) {
				auto sbuf = src.peek(buf.length);
				buf[0 .. sbuf.length] = sbuf[];
				src.consume(buf.length);
				return sbuf.length;
			}
		}
	} else if (isUnbufferedPushSource!Src) {
	} else if (isBufferedPushSource!Src) {
	} else
		static assert(0, Src.stringof ~ " is not a proper source type");
}

auto avdemux(Src)(Src src)
{
	return AVDemux!(UnbufferedPullSource!Src)(src);
}

fromFile(File(Runtime.args[1])).avdemux().run();
+/

auto stream(alias Src, T...)(T args)
{
	alias TypSrc = typeof(Src(args));
	return InputStream!(TypSrc, Src, T)(args);
}

unittest
{
	//auto amf = AVMappedFile("dub.sdl");

	//import etc.linux.memoryerror;
	//etc.linux.memoryerror.registerMemoryErrorHandler();

	//import core.runtime : Runtime;
	//auto stream = stream!FromFile(File(Runtime.args[1])).pipe!AVDemux();

	/+
	{
	auto fromFile = FromFile(File(Runtime.args[1]));
	auto demux = AVDemux!fromFile();
	demux.initialize();
	}
	+/
}
