/** Bindings to $(LINK2 https://www.xiph.org/ogg,libogg).
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0)
 */
module dstreams.etc.bindings.ogg.ogg;

import core.stdc.config : c_long;

///
struct ogg_packet {
  ubyte* packet;
  c_long  bytes;
  c_long  b_o_s;
  c_long  e_o_s;

  long    granulepos;

  long    packetno;     /** sequence number for decode; the framing
                                knows where there's a hole in the data,
                                but we need coupling so that the codec
                                (which is in a separate abstraction
                                layer) also knows about the gap */
}

struct  oggpack_buffer {
  c_long endbyte;
  int  endbit;

  ubyte* buffer;
  ubyte* ptr;
  c_long storage;
}

///
struct  ogg_stream_state {
	ubyte*   body_data;       /// bytes from packet bodies
	c_long   body_storage;    /// storage elements allocated
	c_long   body_fill;       /// elements stored; fill mark
	c_long   body_returned;   /// elements of fill returned

	/// The values that will go to the segment table
	/// granulepos values for headers.
	/// Not compact this way, but it is simple coupled to the lacing fifo
	int*    lacing_vals;
	long*   granule_vals;     /// ditto
	c_long  lacing_storage;   /// ditto
	c_long  lacing_fill;      /// ditto
	c_long  lacing_packet;    /// ditto
	c_long  lacing_returned;  /// ditto

	ubyte[282] header;        /// working space for header encode
	int        header_fill;

	int        e_o_s;         /// set when we have buffered the last packet in the logical bitstream
	int        b_o_s;         /// set after we've written the initial page of a logical bitstream
	c_long     serialno;      ///
	int        pageno;        ///
	long       packetno;      /** sequence number for decode; the framing
	                         knows where there's a hole in the data,
	                         but we need coupling so that the codec
	                         (which is in a seperate abstraction
	                         layer) also knows about the gap */
	long       granulepos;    ///

}

///
struct ogg_page {
	ubyte* header;     ///
	c_long header_len; ///
	ubyte* body_;      ///
	c_long body_len;   ///
}

///
struct ogg_sync_state {
	ubyte* data;     ///
	int storage;     ///
	int fill;        ///
	int returned;    ///
	int unsynced;    ///
	int headerbytes; ///
	int bodybytes;   ///
}

///
extern(C) int ogg_sync_init(ogg_sync_state* oy);

///
extern(C) ubyte* ogg_sync_buffer(ogg_sync_state* oy, long size);

///
extern(C) int ogg_sync_wrote(ogg_sync_state* oy, long bytes);

///
extern(C) int ogg_sync_pageout(ogg_sync_state* oy, ogg_page* og);

///
extern(C) int ogg_stream_pagein(ogg_stream_state* os, ogg_page* og);

///
extern(C) int ogg_stream_packetout(ogg_stream_state* os, ogg_packet* op);

///
extern(C) int ogg_stream_packetpeek(ogg_stream_state* os, ogg_packet* op);

///
extern(C) c_long ogg_page_pageno(ogg_page* og);


/* Ogg BITSTREAM PRIMITIVES: general ***************************/

extern(C) int      ogg_stream_init(ogg_stream_state* os, int serialno);
extern(C) int      ogg_stream_clear(ogg_stream_state* os);
extern(C) int      ogg_stream_reset(ogg_stream_state* os);
extern(C) int      ogg_stream_reset_serialno(ogg_stream_state* os,int serialno);
extern(C) int      ogg_stream_destroy(ogg_stream_state* os);
extern(C) int      ogg_stream_check(ogg_stream_state* os);
extern(C) int      ogg_stream_eos(ogg_stream_state* os);

extern(C) void     ogg_page_checksum_set(ogg_page* og);

extern(C) int      ogg_page_version(const(ogg_page)* og);
extern(C) int      ogg_page_continued(const(ogg_page)* og);
extern(C) int      ogg_page_bos(const(ogg_page)* og);
extern(C) int      ogg_page_eos(const(ogg_page)* og);
extern(C) long     ogg_page_granulepos(const(ogg_page)* og);
extern(C) int      ogg_page_serialno(const(ogg_page)* og);
extern(C) c_long   ogg_page_pageno(const(ogg_page)* og);
extern(C) int      ogg_page_packets(const(ogg_page)* og);

extern(C) void     ogg_packet_clear(ogg_packet* op);
