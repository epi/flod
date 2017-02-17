module flod.internal.test.runner;

version(unittest) {}
else static assert(0, "This file should only be built when unittests are enabled");

private:

shared static this()
{
	import core.runtime : Runtime;
	Runtime.moduleUnitTester = function bool() { return true; };
}

struct Unittest {
	void function() func;
	string[] comments;
}

struct ModInfo {
	string name;
	Unittest[] unittests;
}

auto gatherTests(Module...)()
{
	if (!__ctfe)
		return null;

	import std.array : appender;
	import std.meta : Filter;
	import std.traits : fullyQualifiedName;
	auto mapp = appender!(ModInfo[]);
	foreach (m; Module) {
		auto app = appender!(Unittest[]);
		foreach (ut; __traits(getUnitTests, m)) {
			string[] su;
			foreach (uda; __traits(getAttributes, ut)) {
				static if (is(typeof(uda) : string))
					su ~= uda;
			}
			app.put(Unittest(&ut, su));
		}
		mapp.put(ModInfo(fullyQualifiedName!m, app.data));
	}
	return mapp.data;
}

auto gatherAllTests()
{
	return gatherTests!(
		flod,
		flod.adapter,
		flod.buffer,
		flod.file,
		flod.meta,
		flod.metadata,
		flod.pipeline,
		flod.range,
		flod.traits,
		flod.utils);
}

int runTests(bool[string] module_list)
{
	import std.stdio;
	import std.parallelism;
	import std.algorithm : map, filter, joiner;
	import core.atomic;

	static __gshared immutable modules = gatherAllTests;
	shared uint failed;
	shared uint passed;
	foreach (test; modules
		.filter!(m => !module_list.length || m.name in module_list)
		.map!(m => m.unittests[])
		.joiner
		.parallel) {
		try {
			scope(success) atomicOp!"+="(passed, 1);
			scope(failure) atomicOp!"+="(failed, 1);
			test.func();
		} catch (Throwable e) {
			stderr.writefln(`Test "%s" failed: %s at %s:%s: %s`,
				test.comments.length ? test.comments[0] : "",
				typeid(e), e.file, e.line, e.msg);
			stderr.writeln(e.info);
		}
	}
	writefln("%d tests executed. Passed: %d, failed: %d.", passed + failed, passed, failed);
	return !!failed;
}

int main(string[] args)
{
	import std.getopt;
	bool[string] module_list;
	getopt(args, "m", (string a, string b) { module_list[b] = true; });
	return runTests(module_list);
}
