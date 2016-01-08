/** Bindings to $(LINK2 https://www.ffmpeg.org/libavutil.html,libavutil).
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0)
 */
module dstreams.etc.bindings.libavutil.mem;

/**
 * Allocate a block of size bytes with alignment suitable for all
 * memory accesses (including vectors if available on the CPU).
 *
 * Params: size = Size in bytes for the memory block to be allocated.
 * Returns: Pointer to the allocated block, `null` if the block cannot be allocated.
 * See_also: `av_mallocz`;
 */
extern(C) void* av_malloc(size_t size);

/**
 * Allocate or reallocate a block of memory.
 *
 * If ptr is `null` and size > 0, allocate a new block. If
 * size is zero, free the memory block pointed to by ptr.
 * Params:
 * ptr = Pointer to a memory block already allocated with
 * `av_malloc`, `av_mallocz` or `av_realloc` or `null`.
 * size = Size in bytes for the memory block to be allocated or
 * reallocated.
 * Returns: Pointer to a newly reallocated block or `null` if the block
 * cannot be reallocated or the function is used to free the memory block.
 * See_also: `av_fast_realloc`
 */
extern(C) void* av_realloc(void* ptr, size_t size);

/**
 * Free a memory block which has been allocated with `av_malloc`, `av_mallocz` or
 * `av_realloc`.
 * Params:
 * ptr = Pointer to the memory block which should be freed.
 * Note: ptr = `null` is explicitly allowed.
 *
 * It is recommended that you use `av_freep` instead.
 * See_also: `av_freep`
 */
extern(C) void av_free(void* ptr);

/**
 * Allocate a block of size bytes with alignment suitable for all
 * memory accesses (including vectors if available on the CPU) and
 * zero all the bytes of the block.
 *
 * Params: size = Size in bytes for the memory block to be allocated.
 * Returns: Pointer to the allocated block, `null` if it cannot be allocated.
 * See_also: `av_malloc`
 */
extern(C) void* av_mallocz(size_t size);

/**
 * Duplicate the string s.
 *
 * Params: s = string to be duplicated
 * Returns: Pointer to a newly allocated string containing a
 * copy of s or `null` if the string cannot be allocated.
 */
extern(C) char* av_strdup(const(char)* s);

/**
 * Free a memory block which has been allocated with `av_malloc`, `av_mallocz` or
 * `av_realloc` and set the pointer pointing to it to `null`.
 * Params: ptr = Pointer to the pointer to the memory block which should be freed.
 * See_also: `av_free`
 */
extern(C) void av_freep(void** ptr);
