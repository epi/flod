import std.exception : enforce;

import flod.etc.alsa;
import flod.etc.mad;
import flod.file : fromFile;
import flod.pipeline;
import flod.traits;
import std.stdio;

int main(string[] args)
{
	enforce(args.length > 1, "Specify a file to play");
	foreach (fn; args[1 .. $]) {
		auto f = File(fn, "rb");
		fromFile(File(fn))
			.decodeMp3
			.pipe!AlsaPcm(2, 44100, 16)
			.run();
	}
	return 0;
}
