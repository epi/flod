/** Bindings to $(LINK2 https://www.ffmpeg.org/libavutil.html,libavutil).
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0)
 */
module dstreams.etc.bindings.libavutil.file;

/** Slurp entire file into memory, using `mmap` where available.
 *
 * Params:
 * filename   = name of the file to read
 * bufptr     = pointer to the buffer will be stored here
 * size       = size of the buffer will be stored here
 * log_offset = loglevel offset used for logging
 * log_ctx    = context used for logging
 *
 * Returns:
 * A non negative number in case of success, a negative value
 * corresponding to an AVERROR error code in case of failure.
 */
extern(C) int av_file_map(const(char)* filename, ubyte** bufptr, size_t* size, int log_offset, void* log_ctx);

/** Free the buffer allocated by `av_file_map`.
 */
extern(C) void av_file_unmap(ubyte* bufptr, size_t size);
