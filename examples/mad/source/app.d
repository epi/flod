import std.exception : enforce;
import std.regex : match;

import flod.etc.alsa;
import flod.etc.mad;
import flod.etc.curl;
import flod.pipeline;

int main(string[] args)
{
	enforce(args.length > 1, "Specify a file to play");
	foreach (fn; args[1 .. $]) {
		auto url = args[1].match(`^[a-z]+://`).empty ? "file://" ~ fn : fn;
		download(url).decodeMp3.pipe!AlsaPcm(2, 44100, 16);
	}
	return 0;
}
