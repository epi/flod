import std.exception : enforce;
import std.regex : match;

import dstreams;

int main(string[] args)
{
	enforce(args.length > 1);
	auto url = args[1].match(`^[a-z]+://.*`).empty ? "file://" ~ args[1] : args[1];
	writeln(url);
	auto source = curlReader(url);
	auto proc = madDecoder();
	auto sink = alsaSink(2, 44100, 16);
	auto sinkbuf = pushBuffer(sink);
	auto sourcebuf = pushToPullBuffer(proc, sinkbuf);
	source.push(sourcebuf);
	return 0;
}
