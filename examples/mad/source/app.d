import std.exception : enforce;

import flod.etc.alsa;
import flod.etc.mad;
import flod.etc.curl;
import flod.pipeline;
import flod.adapter;
import flod.traits;
import std.stdio;

int main(string[] args)
{
	enforce(args.length > 1, "Specify a file to play");
	foreach (fn; args[1 .. $]) {
		download(fn)
			.pushPull
			.pullPeek
			.decodeMp3
			.pipe!AlsaPcm(2, 44100, 16);
	}
	return 0;
}
