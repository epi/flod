import std.exception : enforce;

import flod.etc.alsa;
import flod.etc.mad;
import flod.file : MmappedFile;
import flod.pipeline;
import flod.traits;

int main(string[] args)
{
	enforce(args.length > 1);
	foreach (fn; args[1 .. $]) {
		pipe!MmappedFile(fn)
			.pipe!MadDecoder
			.pipe!AlsaPcm(2, 44100, 16)
			.run();
	}
	return 0;
}
