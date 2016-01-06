import dstreams;

int testMad()
{
	import core.sys.posix.sys.stat;
	import core.sys.posix.sys.mman;
	import core.sys.posix.unistd;
//	import etc.linux.memoryerror;
//	registerMemoryErrorHandler();
	stat_t st;
	void *fdm;

//	if (fstat(STDIN_FILENO, &st) == -1 || st.st_size == 0)
//		return 2;

//	fdm = mmap(null, st.st_size, PROT_READ, MAP_SHARED, STDIN_FILENO, 0);
//	if (fdm == MAP_FAILED)
//		return 3;

//	auto source = fromArray(fdm[0 .. st.st_size]);
//	auto f = File("/media/epi/Passport/music/chips_n_games/epi/epi-tp558-silly_cone.mp3", "rb");
//	auto source = fromFile(f);
//	auto sourcebuf = compactPullBuffer(source);
//	scope(exit) sourcebuf.dispose();
	auto source = curlReader("file:///media/epi/Passport/music/chips_n_games/dubmood_2/sc/toffelskater_2007.128.mp3");
	auto proc = madDecoder();
//	auto sink = ToFile(stdout);
	auto sink = alsaSink(2, 44100, 16);
	auto sinkbuf = pushBuffer(sink);
	auto sourcebuf = pushToPullBuffer(proc, sinkbuf);
//	  decode(fdm, st.st_size);

//	proc.process(sourcebuf, sinkbuf);
	source.push(sourcebuf);

//	auto pipeline = fromFile(stdin) | madDecoder() | alsaSink(2, 44100, 16);
//	pipeline.run();

//	if (munmap(fdm, st.st_size) == -1)
//		return 4;

	return 0;
}

int main(string[] args)
{
	testMad();

	auto stream1 = stream!curlReader("file:///media/epi/Passport/music/chips_n_games/dubmood_2/sc/toffelskater_2007.128.mp3");
	auto stream2 = stream!madDecoder();
	auto stream3 = stream!MadDecoder;


	/+
	stream!PosixFile("some_movie.avi")
		.pipe!AviDemux()
		.tee!("audio", "video")(
			stream!MadDecoder.pipe!AlsaSink(2, 44100, 16),
			stream!MpegDecoder.pipe!SDLVideo())
		.run();

	auto vs = stream!Capture("/dev/video0");
	auto as = stream!AlsaRecord();
	merge!("video", "audio")(vs, as).pipe!AviMux.pipe!PosixFile("recorded_movie.avi");

	stream!Capture("/dev/video0").merge!AlsaRecord.pipe!AviMux.pipe!PosixFile("recorded_movie.avi");
+/
	return 0;
}
