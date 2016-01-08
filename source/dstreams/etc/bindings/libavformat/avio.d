/** Bindings to $(LINK2 https://www.ffmpeg.org/libavformat.html,libavformat).
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0)
 */
module dstreams.etc.bindings.libavformat.avio;

import core.stdc.config : c_long, c_ulong;

///
struct AVClass;

/**
 * Callback for checking whether to abort blocking functions.
 * AVERROR_EXIT is returned in this case by the interrupted
 * function. During blocking operations, callback is called with
 * opaque as parameter. If the callback returns 1, the
 * blocking operation will be aborted.
 *
 * No members can be added to this struct without a major bump, if
 * new elements have been added after this struct in AVFormatContext
 * or AVIOContext.
 */
struct AVIOInterruptCB {
	alias Callback = extern(C) int function(void*);
	Callback callback;
    void* opaque;
};


///
struct AVIOContext {
	alias ReadPacketFunc = extern(C) int function(void* opaque, ubyte* buf, int buf_size);
	alias WritePacketFunc = extern(C) int function(void* opaque, ubyte* buf, int buf_size);
	alias SeekFunc = extern(C) long function(void* opaque, long offset, int whence);
	alias ReadSeekFunc  = extern(C) long function(void* opaque, int stream_index, long timestamp, int flags);
	alias ReadPauseFunc = extern(C) int function(void* opaque, int pause);
	alias UpdateChecksumFunc = c_ulong function(c_ulong checksum, const(ubyte)* buf, uint size);

	/**
     * A class for private options.
     *
     * If this `AVIOContext` is created by `avio_open2()`, `av_class` is set and
     * passes the options down to protocols.
     *
     * If this `AVIOContext` is manually allocated, then `av_class` may be set by
     * the caller.
     *
     * Warning: This field can be `null`, be sure to not pass this `AVIOContext`
     * to any `av_opt_*` functions in that case.
     */
	const(AVClass)* av_class;
	ubyte* buffer;       /// Start of the _buffer
	int    buffer_size;  /// Maximum _buffer size
	ubyte* buf_ptr;      /// Current position in the _buffer
	ubyte* buf_end;      /// End of the data, may be less than _buffer+_buffer_size if the read function returned less data than requested, e.g. for streams where no more data has been received yet.
	void*  opaque;       /// A private pointer, passed to the read/write/_seek/... functions.

	ReadPacketFunc read_packet;    ///
	WritePacketFunc write_packet;  ///
	SeekFunc seek;                 ///

	long pos;            /// position in the file of the current _buffer
	int must_flush;      /// true if the next _seek should flush
	int eof_reached;     /// true if eof reached
	int write_flag;      /// true if open for writing
	int max_packet_size; ///
	c_ulong checksum;    ///
	ubyte* checksum_ptr; ///
	UpdateChecksumFunc update_checksum; ///
	int error;           /// contains the _error code or 0 if no _error happened

	/**
     * Pause or resume playback for network streaming protocols - e.g. MMS.
     */
	ReadPauseFunc read_pause;

	/**
     * Seek to a given timestamp in stream with the specified stream_index.
     * Needed for some network streaming protocols which don't support seeking
     * to byte position.
     */
	ReadSeekFunc read_seek;

	/**
     * A combination of `AVIO_SEEKABLE_` flags or 0 when the stream is not _seekable.
     */
	int seekable;
}


/**
 * Allocate and initialize an AVIOContext for buffered I/O. It must be later
 * freed with av_free().
 *
 * buffer = Memory block for input/output operations via `AVIOContext`.
 *        The _buffer must be allocated with `av_malloc` and friends.
 * buffer_size = The _buffer size is very important for performance.
 *        For protocols with fixed blocksize it should be set to this blocksize.
 *        For others a typical size is a cache page, e.g. 4kb.
 * write_flag = Set to 1 if the _buffer should be writable, 0 otherwise.
 * opaque = An opaque pointer to user-specific data.
 * read_packet = A function for refilling the _buffer, may be `null`.
 * write_packet = A function for writing the _buffer contents, may be `null`.
 * seek = A function for seeking to specified byte position, may be `null`.
 *
 * Returns: Allocated AVIOContext or NULL on failure.
 */
extern(C) AVIOContext* avio_alloc_context(
	ubyte* buffer, int buffer_size, int write_flag, void* opaque,
	AVIOContext.ReadPacketFunc read_packet,
	AVIOContext.WritePacketFunc write_packet,
	AVIOContext.SeekFunc seek);
