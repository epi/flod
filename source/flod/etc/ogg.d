/** Reading and writing $(LINK2 https://www.xiph.org/ogg,Ogg) streams.
 *
 * Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 * Copyright: © 2016 Adrian Matoga
 * License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0)
 */
module flod.etc.ogg;

import std.exception : enforce;
import std.stdio;
import std.typecons : Tuple, tuple;
import std.meta : allSatisfy;

import deimos.ogg.ogg;
import flod.traits : isUnbufferedPushSink, isBufferedPushSink;
import flod.stream : BufferedToUnbufferedPushSource;

/// buffered push sink
/// buffered push n-source
struct OggReader(MultiSink)
{
	MultiSink sink;
private:
	ogg_sync_state oy;

	alias PushFn = void function(OggReader* self, const(ubyte)[] buf);
	alias CloseFn = void function(OggReader* self);
	enum PushFn freeStream = cast(PushFn) cast(void*) -1;

	static struct OggStream {
		int serial;
		PushFn push = freeStream;
		CloseFn close;
		ogg_stream_state os;
	}

	OggStream[4] streams;

	static void doPush(string type)(OggReader* self, const(ubyte)[] buf) { self.sink.push!type(buf); }

	static void doClose(string type)(OggReader* self) { self.sink.close!type(); }

	OggStream* getStreamBySerial(int serial)
	{
		foreach (ref stream; streams[]) {
			if (stream.serial == serial)
				return &stream;
		}
		foreach (ref stream; streams[]) {
			if (stream.push == freeStream) {
				stream.push = null;
				stream.close = null;
				stream.serial = serial;
				enforce(ogg_stream_init(&stream.os, serial) == 0, "ogg_stream_init failed");
				return &stream;
			}
		}
		throw new Exception("too many streams");
	}

public:
	void open()
	{
		ogg_sync_init(&oy);
		zz = 0;
	}

	void close()
	{
		foreach (ref stream; streams[]) {
			if (stream.close)
				stream.close(&this);
		}
	}

	ubyte[] alloc(size_t n)
	{
		auto buf = ogg_sync_buffer(&oy, n);
		enforce(buf !is null, "libogg: buffer allocation failed");
		return buf[0 .. n];
	}

	size_t zz;
	void commit(size_t n)
	{
		auto res = ogg_sync_wrote(&oy, n);
		enforce(res == 0, "libogg: written data not accepted");
		ogg_page og;
		for (;;) {
			res = ogg_sync_pageout(&oy, &og);
			if (res == 0)
				return;
			enforce(res == 1, "libogg: written data not accepted");
			//writefln("HEAD %(%02x%| %)", og.header[0 .. og.header_len]);
			//writefln("DATA %(%02x%| %)", og.body_[0 .. og.body_len]);
			int serial = ogg_page_serialno(&og);
			OggStream* str = getStreamBySerial(serial);
			enforce(ogg_stream_pagein(&str.os, &og) == 0);
			ogg_packet packet;
			for (;;) {
				res = ogg_stream_packetout(&str.os, &packet);
				if (res == 0)
					break;
				if (res == -1) {
					//throw new Exception("ogg lost sync");
					stderr.writeln("ogg lost sync");
					break;
				}
				/+
				writefln("%08x bytes=%10d b_o_s=%3d e_o_s=%3d granulepos=%10d %10d packetno=%10d",
					serial, packet.bytes, packet.b_o_s, packet.e_o_s, packet.granulepos, zz, packet.packetno);
				zz += packet.bytes;
				+/
				auto apacket = packet.packet[0 .. packet.bytes];

				if (packet.b_o_s) {
					writefln("BOS %08x bytes=%d: %(%02x%| %)", serial, packet.bytes, packet.packet[0 .. packet.bytes]);
					if (apacket.length > 7 && apacket[1 .. 7] == "vorbis") {
						str.push = &doPush!"audio";
						str.close = &doClose!"audio";
					}
				}

				//sinks[0].push(packet.packet[0 .. packet.bytes]);
				//if (str.os)
			//		sink.push!"audio"(packet.packet[0 .. packet.bytes]);
			//	sink.push!"vizeo"(packet.packet[0 .. packet.bytes]);
				if (str.push)
					str.push(&this, packet.packet[0 .. packet.bytes]);

				if (packet.e_o_s) {
					writefln("EOS %08x bytes=%d: %(%02x%| %)", serial, packet.bytes, packet.packet[0 .. packet.bytes]);
					str.close(&this);
					str.push = null;
					str.close = null;
				}

			}
		}
	}
}

struct StreamType {
	string type;
}

StreamType streamType(string t)
{
	return StreamType(t);
}

template isType(T...)
{
	enum isType = T.length == 1 && is(T[0]);
}

struct PushTee(Sink...)
	if (allSatisfy!(isType, Sink))
{
	Tuple!Sink sinks;
	void close(string tag)() {}
	void push(string type, T)(const(T)[] inbuf)
	{
		foreach (ref sink; sinks) {
			import std.traits : hasUDA, getUDAs, isPointer, PointerTarget;
			static if (isPointer!(typeof(sink)))
				alias Sk = PointerTarget!(typeof(sink));
			else
				alias Sk = typeof(sink);
			if (hasUDA!(Sk, StreamType) && getUDAs!(Sk, StreamType)[0].type == type) {
				static if (allSatisfy!(isUnbufferedPushSink, Sink)) {
					sink.push(inbuf);
				} else static if (allSatisfy!(isBufferedPushSink, Sink)) {
					auto outbuf = sink.alloc(inbuf.length);
					outbuf[] = inbuf[];
					sink.commit(outbuf.length);
				}
			}
		}
	}
}

@streamType("audio")
struct VorbisDecoder(Sink) {
	void open()
	{
	}

	void push(const(ubyte)[] b)
	{
		writeln("audio ", b.length);
		// discard
	}

	void close()
	{
	}
}
//static assert(isUnbufferedPushSink!VorbisDecoder);

@streamType("video")
struct TheoraDecoder(Sink) {
	void open()
	{
	}

	void push(const(ubyte)[] b)
	{
		writeln("video ", b.length);
		// discard
	}

	void close()
	{
	}
}
//static assert(isUnbufferedPushSink!TheoraDecoder);

unittest
{
	import std.stdio;
	import flod;
	import etc.linux.memoryerror;
	import flod.common : NullSink;
	etc.linux.memoryerror.registerMemoryErrorHandler();

//	auto f = File("/media/epi/Passport/video/yt/test.ogv");
//	CurlReader!(BufferedToUnbufferedPushSource!(OggReader!(PushTee!(VorbisDecoder!NullSink, TheoraDecoder!NullSink)))) source;
//	source.open("file:///home/epi/export.ogg"); //"http://icecast.radiovox.org:8000/live.ogg");
//	source.run();
	/+
	import flod.stream;
	stream!CurlReader("http://icecast.radiovox.org:8000/live.ogg").pipe!CurlBuffer.pipe!OggReader.tee(
		stream!VorbisDecoder.pipe!AlsaPcmOutput,
		stream!TheoraDecoder.pipe!SdlVideo).run();

	stream!CurlReader("http://icecast.radiovox.org:8000/live.ogg").pipe!CurlBuffer.pipe!OggReader.pipe!VorbisDecoder.pipe!AlsaPcmOutput.run();
	+/
}
