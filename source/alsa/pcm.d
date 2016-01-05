///
module alsa.pcm;

// Dummy struct, only pointers are used.
struct snd_pcm_t
{
}

enum snd_pcm_stream_t
{
	SND_PCM_STREAM_PLAYBACK = 0,
	SND_PCM_STREAM_CAPTURE,
	SND_PCM_STREAM_LAST = SND_PCM_STREAM_CAPTURE
}

enum snd_pcm_access_t
{
	SND_PCM_ACCESS_MMAP_INTERLEAVED = 0,
	SND_PCM_ACCESS_MMAP_NONINTERLEAVED,
	SND_PCM_ACCESS_MMAP_COMPLEX,
	SND_PCM_ACCESS_RW_INTERLEAVED,
	SND_PCM_ACCESS_RW_NONINTERLEAVED,
	SND_PCM_ACCESS_LAST = SND_PCM_ACCESS_RW_NONINTERLEAVED
}

enum snd_pcm_format_t
{
	SND_PCM_FORMAT_UNKNOWN = -1,
	SND_PCM_FORMAT_S8 = 0,
	SND_PCM_FORMAT_U8,
	SND_PCM_FORMAT_S16_LE,
	SND_PCM_FORMAT_S16_BE,
	SND_PCM_FORMAT_U16_LE,
	SND_PCM_FORMAT_U16_BE,
	SND_PCM_FORMAT_S24_LE,
	SND_PCM_FORMAT_S24_BE,
	SND_PCM_FORMAT_U24_LE,
	SND_PCM_FORMAT_U24_BE,
	SND_PCM_FORMAT_S32_LE,
	SND_PCM_FORMAT_S32_BE,
	SND_PCM_FORMAT_U32_LE,
	SND_PCM_FORMAT_U32_BE,
	SND_PCM_FORMAT_FLOAT_LE,
	SND_PCM_FORMAT_FLOAT_BE,
	SND_PCM_FORMAT_FLOAT64_LE,
	SND_PCM_FORMAT_FLOAT64_BE,
	SND_PCM_FORMAT_IEC958_SUBFRAME_LE,
	SND_PCM_FORMAT_IEC958_SUBFRAME_BE,
	SND_PCM_FORMAT_MU_LAW,
	SND_PCM_FORMAT_A_LAW,
	SND_PCM_FORMAT_IMA_ADPCM,
	SND_PCM_FORMAT_MPEG,
	SND_PCM_FORMAT_GSM,
	SND_PCM_FORMAT_SPECIAL = 31,
	SND_PCM_FORMAT_S24_3LE = 32,
	SND_PCM_FORMAT_S24_3BE,
	SND_PCM_FORMAT_U24_3LE,
	SND_PCM_FORMAT_U24_3BE,
	SND_PCM_FORMAT_S20_3LE,
	SND_PCM_FORMAT_S20_3BE,
	SND_PCM_FORMAT_U20_3LE,
	SND_PCM_FORMAT_U20_3BE,
	SND_PCM_FORMAT_S18_3LE,
	SND_PCM_FORMAT_S18_3BE,
	SND_PCM_FORMAT_U18_3LE,
	SND_PCM_FORMAT_U18_3BE,
	SND_PCM_FORMAT_LAST = SND_PCM_FORMAT_U18_3BE,
}

import core.stdc.config;

alias c_long snd_pcm_sframes_t;
alias c_ulong snd_pcm_uframes_t;

extern (C)
int snd_pcm_open(snd_pcm_t **pcm, const char *name, snd_pcm_stream_t stream, int mode);

extern (C)
int snd_pcm_set_params(
	snd_pcm_t *pcm,
	snd_pcm_format_t format,
	snd_pcm_access_t access,
	uint channels,
	uint rate,
	int soft_resample,
	uint latency);

extern (C)
snd_pcm_sframes_t snd_pcm_writei(snd_pcm_t *pcm, const void *buffer, snd_pcm_uframes_t size);

extern (C)
int snd_pcm_recover(snd_pcm_t *pcm, int err, int silent);

extern (C)
int snd_pcm_close(snd_pcm_t *pcm);
