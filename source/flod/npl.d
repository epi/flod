///
module flod.npl;

auto mymove(X)(auto ref X x, string file = __FILE__, uint line = __LINE__)
{
	import std.algorithm : move;
	import std.experimental.logger : infof;
	infof(true, "(from %s:%d) MOVE %s", file, line, X.stringof);
	return move(x);
}

mixin template NonCopyable()
{
	@disable this(this);
	@disable void opAssign(typeof(this));
}

struct PullSource {
	mixin NonCopyable;

	size_t pull(T)(T[] buf)
	{
		buf[] = T.init;
		return buf.length;
	}
}

auto inits()
{
	return PullSource();
}

template PullFilter(T) {
	struct PullFilter(Source) {
	private:
		Source source;
		T what;
		T withWhat;

	public:
		mixin NonCopyable;

		size_t pull(T)(T[] buf)
		{
			source.pull(buf);
			foreach (ref b; buf) {
				if (b == what)
					b = withWhat;
			}
			return buf.length;
		}
	}
}

///
auto pipe(alias Stage, Pipeline, Args...)(auto ref Pipeline pipeline, auto ref Args args)
{
	return Stage!(Pipeline)(mymove(pipeline), args);
}

auto replace(Pipeline, T)(auto ref Pipeline pipeline, T what, T withWhat)
{
	return pipeline.pipe!(PullFilter!T)(what, withWhat);
}

unittest
{
	auto x = inits().replace(ubyte.init, ubyte(5));
	ubyte[100] buf;
	x.pull(buf);
	import std.range : repeat, array;
	assert(buf[] == repeat(ubyte(5), 100).array());
}

unittest
{
	auto i = inits();
	auto r = i.replace(ubyte.init, ubyte(5));
	ubyte[100] buf;
}

auto deferredCreate(alias Stage, Args...)(auto ref Args args)
{
	static struct DeferredCtor {
		mixin NonCopyable;
		Args args;

		auto create(Sink)(auto ref Sink sink)
		{
			//import std.algorithm : move;
			return Stage!Sink(mymove(sink), args);
		}

	}
	return DeferredCtor(args);
}

auto deferredCreate2(alias Stage, Pipeline, Args...)(auto ref Pipeline pipeline, auto ref Args args)
{
	static struct DeferredCtor {
		mixin NonCopyable;
		Pipeline pipeline;
		Args args;

		auto create(Sink)(auto ref Sink sink)
		{
			//import std.algorithm : move;
			return Stage!(Pipeline, Sink)(mymove(pipeline), mymove(sink), args);
		}

	}
	return DeferredCtor(mymove(pipeline), args);
}

auto chainedDeferredCreate(alias Stage, Next, Args...)(auto ref Next next, auto ref Args args)
{
	static struct ChainedDeferredCtor {
		mixin NonCopyable;
		Next next;
		Args args;

		auto create(Sink)(auto ref Sink sink)
		{
			//import std.algorithm : move;
			return next.create(Stage!Sink(mymove(sink), args));
		}
	}
	//import std.algorithm : move;
	return ChainedDeferredCtor(mymove(next), args);
}

template PushSource(T) {
	struct PushSource(Sink) {
		mixin NonCopyable;

		Sink sink;
		const(T)[] blob;

		int step()()
		{
			return sink.push(blob) != blob.length;
		}
	}
}

auto blobPush(T)(const(T)[] arr)
{
	return deferredCreate!(PushSource!T)(arr);
}

struct PushSink {
	mixin NonCopyable;

	size_t push(T)(const(T)[] buf)
	{
		import std.stdio : writeln;
		writeln(buf);
		return buf.length;
	}
}

auto pushSink(Chain)(auto ref Chain chain)
{
	return chain.create(PushSink());
}

unittest
{
	auto x = deferredCreate!(PushSource!int)([1, 2, 3]);
	auto y = blobPush([1, 2, 3]);
	blobPush([ 1, 2, 3 ]).pushSink().step();
}

struct PushTake(Sink) {
	mixin NonCopyable;

private:
	Sink sink;
	size_t count;

public:
	size_t push(T)(const(T)[] buf)
	{
		if (count >= buf.length) {
			count -= buf.length;
			return sink.push(buf);
		} else if (count > 0) {
			auto c = count;
			count = 0;
			return sink.push(buf[0 .. c]);
		} else {
			return 0;
		}
	}
}

auto take(Chain)(auto ref Chain chain, size_t count)
{
	return chainedDeferredCreate!PushTake(chain, count);
}

void run(Pipeline)(auto ref Pipeline t)
{
	while (t.step() == 0) {}
}

unittest
{
	blobPush("test").take(20).take(20).pushSink().run();
}

import std.stdio : File;

struct FileWriter
{
	File file;

	size_t push(T)(const(T)[] buf)
	{
		file.rawWrite(buf);
		return buf.length;
	}
}

auto writeFile(Pipeline)(auto ref Pipeline pipeline, File file)
{
	return pipeline.create(FileWriter(file));
}

unittest
{
	blobPush("test").take(100).writeFile(File("test", "wb")).run();
}

void writerep(T)(ref const(T) p)
{
	import std.stdio : writefln;

	writefln("%(%02x%| %)", (cast(const(ubyte)*) &p)[0 .. T.sizeof]);
}

unittest
{
	auto f = File("best", "wb");
	auto source = blobPush("test");
	writerep(source);
	auto tak = source.take(100);
	writerep(tak);
	auto wri = tak.writeFile(f);
	writerep(source);
	writerep(tak);
	writerep(wri);

	blobPush("test").take(100).writeFile(f).run();
	f.rawWrite("X");
}

auto fromFile(File file)
{
	static struct FileReader {
		mixin NonCopyable;
		File file;

		size_t pull(T)(T[] buf)
		{
			return file.rawRead(buf).length;
		}
	}

	return FileReader(file);
}

template PullPush(size_t chunkSize) {
	struct PullPush(Source, Sink) {
		mixin NonCopyable;
		Source source;
		Sink sink;

		int step()
		{
			ubyte[chunkSize] buf;
			auto n = source.pull(buf);
			return sink.push(buf[0 .. n]) != buf.length;
		}
	}
}

auto pullPush(size_t chunkSize = 4096, Pipeline)(auto ref Pipeline pipeline)
{
	return deferredCreate2!(PullPush!(chunkSize))(mymove(pipeline));
}

unittest
{
	fromFile(File("log")).pullPush.take(31337).writeFile(File("log_copy", "wb")).run();
}
