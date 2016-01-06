/** Bindings to $(LINK2 http://www.mega-nerd.com/SRC/,libsamplerate).
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 *  Note that libsamplerate itself is licensed under $(LINK2 http://www.gnu.org/copyleft/gpl.html, GPL version 3)
 *  or $(LINK2 http://www.mega-nerd.com/SRC/license.html,its own commercial license).
 */
module dstreams.etc.bindings.samplerate;

import core.stdc.config;

import std.conv;
import std.exception;

struct SRC_STATE;

struct SRC_DATA
{
	float*  data_in;
	float*  data_out;

	c_long  input_frames;
	c_long  output_frames;
	c_long  input_frames_used;
	c_long  output_frames_gen;

	int     end_of_input;

	double  src_ratio;
}

struct SRC_CB_DATA
{
	long    frames;
	float*  data_in;
}

alias extern(C) long function(void* cb_data, float** data) src_callback_t;

extern (C)
SRC_STATE* src_new(int converter_type, int channels, int *error);

extern (C)
SRC_STATE* src_callback_new(src_callback_t func, int converter_type, int channels,
	int *error, void* cb_data);

extern (C)
SRC_STATE* src_delete(SRC_STATE* state);

extern (C)
int src_process(SRC_STATE* state, SRC_DATA* data);

extern (C)
c_long src_callback_read(SRC_STATE* state, double src_ratio, long frames, float* data);

extern (C)
int src_simple(SRC_DATA* data, int converter_type, int channels);

extern (C)
const(char)* src_get_name(int converter_type);
extern (C)
const(char)* src_get_description(int converter_type);
extern (C)
const(char)* src_get_version();

extern (C)
int src_set_ratio(SRC_STATE* state, double new_ratio);

extern (C)
int src_reset(SRC_STATE* state);

extern (C)
int src_is_valid_ratio(double ratio);

extern (C)
int src_error(SRC_STATE* state);

extern (C)
const(char)* src_strerror(int error);

enum SRC_TYPE
{
	SRC_SINC_BEST_QUALITY		= 0,
	SRC_SINC_MEDIUM_QUALITY		= 1,
	SRC_SINC_FASTEST			= 2,
	SRC_ZERO_ORDER_HOLD			= 3,
	SRC_LINEAR					= 4,
}

extern (C)
void src_short_to_float_array(const(short)* inp, float* outp, int len);
extern (C)
void src_float_to_short_array(const(float)* inp, short* outp, int len);

extern (C)
void src_int_to_float_array(const(int)* inp, float* outp, int len);
extern (C)
void src_float_to_int_array(const(float)* inp, int* outp, int len);
