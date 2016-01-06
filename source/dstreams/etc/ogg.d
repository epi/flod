/** Reading and writing $(LINK2 https://www.xiph.org/ogg,Ogg) streams.
 *
 * Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 * Copyright: © 2016 Adrian Matoga
 * License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0)
 */
module dstreams.etc.ogg;

import std.exception : enforce;
import std.stdio;

import dstreams.etc.bindings.ogg.ogg;


/// buffered push sink
/// buffered push source
struct OggReader {

	ogg_sync_state oy;
	static struct OggStream
	{
		int serial;
		ogg_stream_state os;
	}

	OggStream[4] streams;
	int nstreams;

	ogg_stream_state* getStreamBySerial(int serial)
	{
		foreach (n; 0 .. nstreams) {
			if (streams[n].serial == serial)
				return &streams[n].os;
		}
		if (nstreams == streams.length)
			throw new Exception("too many streams");
		streams[nstreams].serial = serial;
		enforce(ogg_stream_init(&streams[nstreams].os, serial) == 0, "ogg_stream_init failed");
		return &streams[nstreams++].os;
	}

	void init()
	{
		ogg_sync_init(&oy);
		zz = 0;
	}

	ubyte[] alloc(size_t n)
	{
		auto buf = ogg_sync_buffer(&oy, n);
		enforce(buf !is null, "libogg: buffer allocation failed");
		return buf[0 .. n];
	}

	size_t zz;
	void commit(Sink)(Sink sink, size_t n)
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
			ogg_stream_state* os = getStreamBySerial(serial);
			enforce(ogg_stream_pagein(os, &og) == 0);
			ogg_packet packet;
			for (;;) {
				res = ogg_stream_packetout(os, &packet);
				if (res == 0)
					break;
				if (res == -1) {
					//throw new Exception("ogg lost sync");
					stderr.writeln("ogg lost sync");
					break;
				}
				writefln("%08x bytes=%10d b_o_s=%3d e_o_s=%3d granulepos=%10d %10d packetno=%10d",
					serial, packet.bytes, packet.b_o_s, packet.e_o_s, packet.granulepos, zz, packet.packetno);
				zz += packet.bytes;
				sink.push(packet.packet[0 .. packet.bytes]);
			}
		}
	}
}

struct VorbisDecoder {
	void push(const(ubyte)[] b)
	{
		// discard
	}
}

struct CurlBuffer {
	OggReader or;
	VorbisDecoder vd;

	void init()
	{
		or.init();
	}

	void push(const(ubyte)[] b)
	{
		auto buf = or.alloc(b.length);
		buf[] = b[];
		writeln(b.length);
		or.commit(vd, buf.length);
	}
}



unittest
{
	import std.stdio;
	import dstreams;
	import etc.linux.memoryerror;
	etc.linux.memoryerror.registerMemoryErrorHandler();
//	auto f = File("/media/epi/Passport/music/radio/uxa/rasta_banana_bonk.ogg");
//	auto f = File("/media/epi/Passport/video/yt/test.ogv");
	auto cr = curlReader("http://icecast.radiovox.org:8000/live.ogg");
	CurlBuffer cb;
	cb.init();
	cr.push(cb);
}
