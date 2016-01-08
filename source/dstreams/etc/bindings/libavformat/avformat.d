/** Bindings to $(LINK2 https://www.ffmpeg.org/libavformat.html,libavformat).
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0)
 */
module dstreams.etc.bindings.libavformat.avformat;

import dstreams.etc.bindings.libavformat.avio;

///
struct AVInputFormat;
///
struct AVOutputFormat;

///
struct AVStream;

///
struct AVProgram;

///
struct AVPacketList;

///
enum AVCodecID { something }

///
struct AVChapter;

/**
 * Format I/O context.
 * New fields can be added to the end with minor version bumps.
 * Removal, reordering and changes to existing fields require a major
 * version bump.
 * sizeof(AVFormatContext) must not be used outside libav*, use
 * avformat_alloc_context() to create an AVFormatContext.
 */
struct AVFormatContext {
    /**
     * A class for logging and AVOptions. Set by avformat_alloc_context().
     * Exports (de)muxer private options if they exist.
     */
    const(AVClass)* av_class;

    /**
     * Can only be iformat or oformat, not both at the same time.
     *
     * decoding: set by avformat_open_input().
     * encoding: set by the user.
     */
    AVInputFormat* iformat;
    AVOutputFormat* oformat;

    /**
     * Format private data. This is an AVOptions-enabled struct
     * if and only if iformat/oformat.priv_class is not NULL.
     */
    void* priv_data;

    /**
     * I/O context.
     *
     * decoding: either set by the user before avformat_open_input() (then
     * the user must close it manually) or set by avformat_open_input().
     * encoding: set by the user.
     *
     * Do NOT set this field if AVFMT_NOFILE flag is set in
     * iformat/oformat.flags. In such a case, the (de)muxer will handle
     * I/O in some other way and this field will be NULL.
     */
    AVIOContext *pb;

    /* stream info */
    int ctx_flags; /** Format-specific flags, see AVFMTCTX_xx */

    /**
     * A list of all streams in the file. New streams are created with
     * avformat_new_stream().
     *
     * decoding: streams are created by libavformat in avformat_open_input().
     * If AVFMTCTX_NOHEADER is set in ctx_flags, then new streams may also
     * appear in av_read_frame().
     * encoding: streams are created by the user before avformat_write_header().
     */
    uint nb_streams;
    AVStream** streams;

    char[1024] filename; /** input or output filename */

    /**
     * Decoding: position of the first frame of the component, in
     * AV_TIME_BASE fractional seconds. NEVER set this value directly:
     * It is deduced from the AVStream values.
     */
    long start_time;

    /**
     * Decoding: duration of the stream, in AV_TIME_BASE fractional
     * seconds. Only set this value if you know none of the individual stream
     * durations and also do not set any of them. This is deduced from the
     * AVStream values if not set.
     */
    long duration;

    /**
     * Decoding: total stream bitrate in bit/s, 0 if not
     * available. Never set it directly if the file_size and the
     * duration are known as Libav can compute it automatically.
     */
    int bit_rate;

    uint packet_size;
    int max_delay;

    int flags;

	enum Flag {
		GENPTS      = 0x0001, /// Generate missing pts even if it requires parsing future frames.
		IGNIDX      = 0x0002, /// Ignore index.
		NONBLOCK    = 0x0004, /// Do not block when reading packets from input.
		IGNDTS      = 0x0008, /// Ignore DTS on frames that contain both DTS & PTS
		NOFILLIN    = 0x0010, /// Do not infer any values from other values, just return what is stored in the container
		NOPARSE     = 0x0020, /// Do not use AVParsers, you also must set AVFMT_FLAG_NOFILLIN as the fillin code works on frames and no parsing -> no frames. Also seeking to frames can not work if parsing to find frame boundaries has been disabled
		NOBUFFER    = 0x0040, /// Do not buffer frames when possible
		CUSTOM_IO   = 0x0080, /// The caller has supplied a custom AVIOContext, don't avio_close() it.
		DISCARD_CORRUPT = 0x0100, /// Discard frames marked corrupted
	}
    /**
     * decoding: size of data to probe; encoding: unused.
     */
    uint probesize;

    /**
     * decoding: maximum time (in AV_TIME_BASE units) during which the input should
     * be analyzed in avformat_find_stream_info().
     */
    int max_analyze_duration;

    const(ubyte)* key;
    int keylen;

    uint nb_programs;
    AVProgram** programs;

    /**
     * Forced video codec_id.
     * Demuxing: Set by user.
     */
    AVCodecID video_codec_id;

    /**
     * Forced audio codec_id.
     * Demuxing: Set by user.
     */
    AVCodecID audio_codec_id;

    /**
     * Forced subtitle codec_id.
     * Demuxing: Set by user.
     */
    AVCodecID subtitle_codec_id;

    /**
     * Maximum amount of memory in bytes to use for the index of each stream.
     * If the index exceeds this size, entries will be discarded as
     * needed to maintain a smaller size. This can lead to slower or less
     * accurate seeking (depends on demuxer).
     * Demuxers for which a full in-memory index is mandatory will ignore
     * this.
     * muxing  : unused
     * demuxing: set by user
     */
    uint max_index_size;

    /**
     * Maximum amount of memory in bytes to use for buffering frames
     * obtained from realtime capture devices.
     */
    uint max_picture_buffer;

    uint nb_chapters;
    AVChapter** chapters;

    AVDictionary* metadata;

    /**
     * Start time of the stream in real world time, in microseconds
     * since the unix epoch (00:00 1st January 1970). That is, pts=0
     * in the stream was captured at this real world time.
     * - encoding: Set by user.
     * - decoding: Unused.
     */
    long start_time_realtime;

    /**
     * decoding: number of frames used to probe fps
     */
    int fps_probe_size;

    /**
     * Error recognition; higher values will detect more errors but may
     * misdetect some more or less valid parts as errors.
     * - encoding: unused
     * - decoding: Set by user.
     */
    int error_recognition;

    /**
     * Custom interrupt callbacks for the I/O layer.
     *
     * decoding: set by the user before avformat_open_input().
     * encoding: set by the user before avformat_write_header()
     * (mainly useful for AVFMT_NOFILE formats). The callback
     * should also be passed to avio_open2() if it's used to
     * open the file.
     */
    AVIOInterruptCB interrupt_callback;

    /**
     * Flags to enable debugging.
     */
    int debug_;
	enum FF_FDEBUG_TS       = 0x0001;
    /*****************************************************************
     * All fields below this line are not part of the public API. They
     * may not be used outside of libavformat and can be changed and
     * removed at will.
     * New public fields should be added right above.
     *****************************************************************
     */

    /**
     * This buffer is only needed when packets were already buffered but
     * not decoded, for example to get the codec parameters in MPEG
     * streams.
     */
    AVPacketList* packet_buffer;
    AVPacketList* packet_buffer_end;

    /* av_seek_frame() support */
    long data_offset; /** offset of the first packet */

    /**
     * Raw packets from the demuxer, prior to parsing and decoding.
     * This buffer is used for buffering packets until the codec can
     * be identified, as parsing cannot be done without knowing the
     * codec.
     */
    AVPacketList* raw_packet_buffer;
    AVPacketList* raw_packet_buffer_end;
    /**
     * Packets split by the parser get queued here.
     */
    AVPacketList* parse_queue;
    AVPacketList* parse_queue_end;
    /**
     * Remaining size available for raw_packet_buffer, in bytes.
     */
	enum RAW_PACKET_BUFFER_SIZE = 2500000;
    int raw_packet_buffer_remaining_size;
}

///
struct AVDictionary;

/**
 * Return the `LIBAVFORMAT_VERSION_INT` constant.
 */
extern(C) uint avformat_version();

/**
 * Return the libavformat build-time configuration.
 */
extern(C) const(char)* avformat_configuration();

/**
 * Return the libavformat license.
 */
extern(C) const(char)* avformat_license();

/**
 * Initialize libavformat and register all the muxers, demuxers and
 * protocols. If you do not call this function, then you can select
 * exactly which formats you want to support.
 *
 * See_also: `av_register_input_format`, `av_register_output_format`, `av_register_protocol`.
 */
extern(C) void av_register_all();

///
extern(C) void av_register_input_format(AVInputFormat* format);

///
extern(C) void av_register_output_format(AVOutputFormat* format);

/**
 * Allocate an `AVFormatContext`.
 * Function `avformat_free_context` can be used to free the context and everything
 * allocated by the framework within it.
 */
extern(C) AVFormatContext* avformat_alloc_context();

/**
 * Free an `AVFormatContext` and all its streams.
 *
 * Params:
 * s = context to free
 */
extern(C) void avformat_free_context(AVFormatContext* s);

/**
 * Open an input stream and read the header. The codecs are not opened.
 * The stream must be closed with `av_close_input_file`.
 *
 * Params:
 * ps       = Pointer to user-supplied `AVFormatContext` (allocated by `avformat_alloc_context`).
 *            May be a pointer to `null`, in which case an `AVFormatContext` is allocated by this
 *            function and written into ps.
 *            Note that a user-supplied `AVFormatContext` will be freed on failure.
 * filename = Name of the stream to open.
 * fmt      = If non-`null`, this parameter forces a specific input format.
 *            Otherwise the format is autodetected.
 * options  = A dictionary filled with `AVFormatContext` and demuxer-private options.
 *            On return this parameter will be destroyed and replaced with a dict containing
 *            options that were not found. May be `null`.
 *
 * Returns: 0 on success, a negative `AVERROR` on failure.
 *
 * Note: If you want to use custom IO, preallocate the format context and set its pb field.
 */
extern(C) int avformat_open_input(AVFormatContext** ps, const(char)* filename, AVInputFormat* fmt, AVDictionary** options);

/**
 * Close an opened input `AVFormatContext`. Free it and all its contents
 * and set *s to NULL.
 */
extern(C) void avformat_close_input(AVFormatContext** s);

/**
 * Read packets of a media file to get stream information. This
 * is useful for file formats with no headers such as MPEG. This
 * function also computes the real framerate in case of MPEG-2 repeat
 * frame mode.
 * The logical file position is not changed by this function;
 * examined packets may be buffered for later processing.
 *
 * Params:
 * ic = media file handle
 * options = If non-`null`, an `ic.nb_streams` long array of pointers to
 *                 dictionaries, where i-th member contains options for
 *                 codec corresponding to i-th stream.
 *                 On return each dictionary will be filled with options that were not found.
 * Returns: >=0 if OK, `AVERROR_xxx` on error.
 *
 * Note: This function isn't guaranteed to open all the codecs, so
 *       options being non-empty at return is a perfectly normal behavior.
 *
 * To_do: Let the user decide somehow what information is needed so that
 *       we do not waste time getting stuff the user does not need.
 */
extern(C) int avformat_find_stream_info(AVFormatContext* ic, AVDictionary** options);

///
extern(C) void av_dump_format(AVFormatContext* ic, int index, const(char)* url, int is_output);

