/** Bindings to $(LINK2 https://xiph.org/vorbis/,Vorbis codec).
 *
 * Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 * Copyright: © 2016 Adrian Matoga
 * License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0)
 */
module dstreams.etc.bindings.vorbis.codec;

import core.stdc.config : c_long;

import deimos.ogg.ogg : ogg_packet, oggpack_buffer;

///
struct vorbis_info {
	int version_;
	int channels;
	c_long rate;

	c_long bitrate_upper;
	c_long bitrate_nominal;
	c_long bitrate_lower;
	c_long bitrate_window;

	void* codec_setup;
}

///
struct vorbis_comment {
  /* unlimited user comment fields. */
  char** user_comments;
  int*   comment_lengths;
  int    comments;
  char*  vendor;
}

///
struct vorbis_dsp_state {
	int          analysisp;
	vorbis_info* vi;

	float**      pcm;
	float**      pcmret;
	int          pcm_storage;
	int          pcm_current;
	int          pcm_returned;

	int          preextrapolate;
	int          eofflag;

	c_long       lW;
	c_long       W;
	c_long       nW;
	c_long       centerW;

	long         granulepos;
	long         sequence;

	long         glue_bits;
	long         time_bits;
	long         floor_bits;
	long         res_bits;

	void*        backend_state;
}

///
struct vorbis_block {
  /* necessary stream state for linking to the framing abstraction */
  float  **pcm;       /* this is a pointer into local storage */
  oggpack_buffer opb;

  c_long  lW;
  c_long  W;
  c_long  nW;
  int   pcmend;
  int   mode;

  int         eofflag;
  long granulepos;
  long sequence;
  vorbis_dsp_state *vd; /* For read-only access of configuration */

  /* local storage to avoid remallocing; it's up to the mapping to
     structure it */
  void               *localstore;
  c_long                localtop;
  c_long                localalloc;
  c_long                totaluse;
  alloc_chain *reap;

  /* bitmetrics for the frame */
  c_long glue_bits;
  c_long time_bits;
  c_long floor_bits;
  c_long res_bits;

  void *internal;

}

///
struct alloc_chain {
  void* ptr;
  alloc_chain* next;
}

extern(C) void     vorbis_info_init(vorbis_info* vi);
extern(C) void     vorbis_info_clear(vorbis_info* vi);
extern(C) int      vorbis_info_blocksize(vorbis_info* vi,int zo);
extern(C) void     vorbis_comment_init(vorbis_comment* vc);
extern(C) void     vorbis_comment_add(vorbis_comment* vc, const(char)* comment);
extern(C) void     vorbis_comment_add_tag(vorbis_comment* vc,
                                       const(char)* tag, const(char)* contents);
extern(C) char   * vorbis_comment_query(vorbis_comment* vc, const(char)* tag, int count);
extern(C) int      vorbis_comment_query_count(vorbis_comment* vc, const(char)* tag);
extern(C) void     vorbis_comment_clear(vorbis_comment* vc);

extern(C) int      vorbis_block_init(vorbis_dsp_state* v, vorbis_block* vb);
extern(C) int      vorbis_block_clear(vorbis_block* vb);
extern(C) void     vorbis_dsp_clear(vorbis_dsp_state* v);
extern(C) double   vorbis_granule_time(vorbis_dsp_state* v,
                                    long granulepos);

extern(C) const(char)* vorbis_version_string();

// Vorbis PRIMITIVES: analysis/DSP layer

extern(C) int      vorbis_analysis_init(vorbis_dsp_state* v,vorbis_info* vi);
extern(C) int      vorbis_commentheader_out(vorbis_comment* vc, ogg_packet* op);
extern(C) int      vorbis_analysis_headerout(vorbis_dsp_state* v,
                                          vorbis_comment* vc,
                                          ogg_packet* op,
                                          ogg_packet* op_comm,
                                          ogg_packet* op_code);
extern(C) float**  vorbis_analysis_buffer(vorbis_dsp_state* v,int vals);
extern(C) int      vorbis_analysis_wrote(vorbis_dsp_state* v,int vals);
extern(C) int      vorbis_analysis_blockout(vorbis_dsp_state* v,vorbis_block* vb);
extern(C) int      vorbis_analysis(vorbis_block* vb,ogg_packet* op);

extern(C) int      vorbis_bitrate_addblock(vorbis_block* vb);
extern(C) int      vorbis_bitrate_flushpacket(vorbis_dsp_state* vd,
                                           ogg_packet* op);

// Vorbis PRIMITIVES: synthesis layer
extern(C) int      vorbis_synthesis_idheader(ogg_packet* op);
extern(C) int      vorbis_synthesis_headerin(vorbis_info* vi,vorbis_comment* vc,
                                          ogg_packet* op);

extern(C) int      vorbis_synthesis_init(vorbis_dsp_state* v,vorbis_info* vi);
extern(C) int      vorbis_synthesis_restart(vorbis_dsp_state* v);
extern(C) int      vorbis_synthesis(vorbis_block* vb,ogg_packet* op);
extern(C) int      vorbis_synthesis_trackonly(vorbis_block* vb,ogg_packet* op);
extern(C) int      vorbis_synthesis_blockin(vorbis_dsp_state* v,vorbis_block* vb);
extern(C) int      vorbis_synthesis_pcmout(vorbis_dsp_state* v, float*** pcm);
extern(C) int      vorbis_synthesis_lapout(vorbis_dsp_state* v, float*** pcm);
extern(C) int      vorbis_synthesis_read(vorbis_dsp_state* v, int samples);
extern(C) c_long     vorbis_packet_blocksize(vorbis_info* vi, ogg_packet* op);

extern(C) int      vorbis_synthesis_halfrate(vorbis_info* v,int flag);
extern(C) int      vorbis_synthesis_halfrate_p(vorbis_info* v);

// Vorbis ERRORS and return codes

enum OV_FALSE      = -1;
enum OV_EOF        = -2;
enum OV_HOLE       = -3;

enum OV_EREAD      = -128;
enum OV_EFAULT     = -129;
enum OV_EIMPL      = -130;
enum OV_EINVAL     = -131;
enum OV_ENOTVORBIS = -132;
enum OV_EBADHEADER = -133;
enum OV_EVERSION   = -134;
enum OV_ENOTAUDIO  = -135;
enum OV_EBADPACKET = -136;
enum OV_EBADLINK   = -137;
enum OV_ENOSEEK    = -138;
