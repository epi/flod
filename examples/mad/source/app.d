import std.exception : enforce;

import flod : AlsaSink, MadDecoder;
import flod.file : MmappedFile;
import flod.stream : stream;
import flod.common : drop;

int main(string[] args)
{
	enforce(args.length > 1);
	foreach (fn; args[1 .. $]) {
		stream!MmappedFile(fn)
			.pipe!MadDecoder
			.pipe!AlsaSink(2, 44100, 16).run();
	}
	return 0;
}
