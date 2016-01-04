///
module mad;

import core.stdc.config;

alias mad_fixed_t = int;
alias mad_fixed64hi_t = int;
alias mad_fixed64lo_t = uint;

struct mad_timer_t {
	c_long  seconds;    /// whole seconds
	c_ulong fraction;	/// 1/MAD_TIMER_RESOLUTION seconds
};

enum MadLayer {
	i   = 1,  /// Layer I
	ii  = 2,  /// Layer II
	iii = 3   /// Layer III
}

enum MadMode {
	singleChannel = 0,   /// single channel
	dualChannel   = 1,   /// dual channel
	jointStereo   = 2,   /// joint (MS/intensity) stereo
	stereo        = 3    /// normal LR stereo
}

enum MadEmphasis {
	none      = 0,   /// no emphasis
	_50_15_us = 1,   /// 50/15 microseconds emphasis
	ccit_j_17 = 2,   /// CCITT J.17 emphasis
	reserved  = 3    /// unknown emphasis
}

enum MAD_BUFFER_GUARD = 8;
enum MAD_BUFFER_MDLEN = (511 + 2048 + MAD_BUFFER_GUARD);

enum MadError {
	NONE           = 0x0000, /// no error

	BUFLEN         = 0x0001, /// input buffer too small (or EOF)
	BUFPTR         = 0x0002, /// invalid (null) buffer pointer

	NOMEM          = 0x0031, /// not enough memory

	LOSTSYNC       = 0x0101, /// lost synchronization
	BADLAYER       = 0x0102, /// reserved header layer value
	BADBITRATE     = 0x0103, /// forbidden bitrate value
	BADSAMPLERATE  = 0x0104, /// reserved sample frequency value
	BADEMPHASIS    = 0x0105, /// reserved emphasis value

	BADCRC         = 0x0201, /// CRC check failed
	BADBITALLOC    = 0x0211, /// forbidden bit allocation value
	BADSCALEFACTOR = 0x0221, /// bad scalefactor index
	BADMODE        = 0x0222, /// bad bitrate/mode combination
	BADFRAMELEN    = 0x0231, /// bad frame length
	BADBIGVALUES   = 0x0232, /// bad big_values count
	BADBLOCKTYPE   = 0x0233, /// reserved block_type
	BADSCFSI       = 0x0234, /// bad scalefactor selection info
	BADDATAPTR     = 0x0235, /// bad main_data_begin pointer
	BADPART3LEN    = 0x0236, /// bad audio data length
	BADHUFFTABLE   = 0x0237, /// bad Huffman table select
	BADHUFFDATA    = 0x0238, /// Huffman data overrun
	BADSTEREO      = 0x0239  /// incompatible block_type for JS
}

auto MAD_RECOVERABLE(E)(E error) { error & 0xff00; }

struct MadPcm {
	uint samplerate;		/* sampling frequency (Hz) */
	ushort channels;		/* number of channels */
	ushort length;		/* number of samples per channel */
	mad_fixed_t[1152][2] samples;		/* PCM output samples [ch][sample] */
};

struct MadHeader {
	MadLayer layer;			/* audio layer (1, 2, or 3) */
	MadMode mode;			/* channel mode (see above) */
	int mode_extension;			/* additional mode info */
	MadEmphasis emphasis;		/* de-emphasis to use (see above) */

	c_ulong bitrate;		/* stream bitrate (bps) */
	uint samplerate;		/* sampling frequency (Hz) */

	ushort crc_check;		/* frame CRC accumulator */
	ushort crc_target;		/* final target CRC checksum */

	int flags;				/* flags (see below) */
	int private_bits;			/* private bits (see below) */

	mad_timer_t duration;			/* audio playing time of frame */
}

struct mad_bitptr {
	const(ubyte) *byte_;
	ushort cache;
	ushort left;
}

///
struct mad_stream {
	const(ubyte)* buffer;           /* input bitstream buffer */
	const(ubyte)* bufend;           /* end of buffer */
	c_ulong skiplen;                /* bytes to skip before next frame */

	int sync;				/* stream sync found */
	c_ulong freerate;		/* free bitrate (fixed) */

	const(ubyte)* this_frame;       /// start of current frame
	const(ubyte)* next_frame;       /// start of next frame
	mad_bitptr ptr;                 /// current processing bit pointer

	mad_bitptr anc_ptr;		/* ancillary bits pointer */
	uint anc_bitlen;		/* number of ancillary bits */

	char[MAD_BUFFER_MDLEN]* main_data; /// Layer III main_data()
	uint md_len;			/* bytes in main_data */

	int options;				/* decoding options (see below) */
	MadError error;			/* error code (see above) */
};

struct mad_frame {
	MadHeader header;                  /// MPEG audio header
	int options;                        /// decoding options (from stream)
	mad_fixed_t[32][36][2] sbsample;    /// synthesis subband filter samples
	mad_fixed_t[18][32][2]* overlap;    /// Layer III block overlap data
}

enum MadDecoderMode {
	sync = 0,
	async
}

enum MadFlow {
	continue_ = 0x0000,	/* continue normally */
	stop      = 0x0010,	/* stop decoding normally */
	break_    = 0x0011,	/* stop decoding and signal an error */
	ignore    = 0x0020	/* ignore the current frame */
}

alias MadInputFunc   = extern(C) MadFlow function(void*, mad_stream* stream);
alias MadHeaderFunc  = extern(C) MadFlow function(void*, const(MadHeader)* header);
alias MadFilterFunc  = extern(C) MadFlow function(void*, const(mad_stream)* stream, mad_frame* frame);
alias MadOutputFunc  = extern(C) MadFlow function(void*, const(MadHeader)* header, MadPcm* pcm);
alias MadErrorFunc   = extern(C) MadFlow function(void*, mad_stream* stream, mad_frame* frame);
alias MadMessageFunc = extern(C) MadFlow function(void*, void*, uint*);

struct MadDecoder {
	MadDecoderMode mode;

	int options;

	long async_pid;
	int async_in;
	int async_out;

	void *sync;
	/*
  struct {
    mad_stream stream;
    mad_frame frame;
    mad_synth synth;
  } *sync;*/

	void* cb_data;

	MadInputFunc input_func;
	MadHeaderFunc header_func;
	MadFilterFunc filter_func;
	MadOutputFunc output_func;
	MadErrorFunc error_func;
	MadMessageFunc message_func;
}

extern(C)
void mad_decoder_init(
	MadDecoder* decoder, void* data,
	MadInputFunc input,
	MadHeaderFunc header,
	MadFilterFunc filter,
	MadOutputFunc output,
	MadErrorFunc error,
	MadMessageFunc message);

extern(C)
int mad_decoder_finish(MadDecoder* decoder);

extern(C)
int mad_decoder_run(MadDecoder* decoder, MadDecoderMode mode);

extern(C)
int mad_decoder_message(MadDecoder* decoder, void* p, uint* ui);

extern(C)
void mad_stream_buffer(mad_stream* stream, const(void)* buf, c_ulong len);

enum MAD_F_FRACBITS = 28;
enum MAD_F_ONE      = 0x10000000;


