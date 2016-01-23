import std.exception : enforce;

import dstreams : AlsaSink, MadDecoder;
import dstreams.file : MmappedFile;
import dstreams.stream : stream;
import dstreams.common : drop;

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
