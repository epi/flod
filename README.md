[![Build Status](https://travis-ci.org/epi/flod.svg?branch=master)](https://travis-ci.org/epi/flod)
# flod

*flod* is a D library implementing the pipeline pattern with compile-time pipeline composition.
API documentation is available [here](http://epi.github.io/flod/ddox/flod.html).

## Goals

- UFCS-based filter chaining syntax, similar to how ranges are composed.
  Also includes converting between pipelines and ranges where it makes sense.
  ```d
  read("file.gz").inflate(Format.gzip).byLine
      .map!(a => a.stripRight).join("\n")
      .deflate(Format.gzip).write("file-without-trailing-whitespaces.gz");
  ```

- No single interface for passing data is imposed on pipe implementors.
  Push or pull, with your own buffer or requiring an external buffer, it shouldn't matter.

- No gratuitous indirections – a pipeline is a single struct instance, composed of
  several struct template instances, allowing the compiler to show its inlining capabilities.

- Deterministic resource management – stages are constructed in place, and destroyed
  in exactly the reverse order. Just make a stage struct non-copyable and release all
  resources (file handles, C library contexts, memory-mapped hardware, etc.) in the
  destructor and you're safe.

- Support for out-of-band data, such as file names or information about the stream encoding,
  e.g. image size and color depth, or audio stream sample rate.

- Liberal licensing – the library should be free to use, modify, extend, distribute and
  embed in proprietary and/or commercial products without restrictions.

## Progress

- [x] Can connect stages with incompatible interfaces (buffered vs. unbuffered, push vs. pull).
- [x] Can read from input ranges and built-in arrays.
- [x] Can write to output ranges.
- [ ] Input range interface to read from a pipeline by element.
- [ ] Input range interface to read from a pipeline by chunk.
- [ ] Input range interface to read from a pipeline by line.
- [ ] Output range interface (put to pipeline).
- [ ] Reading from and writing to files.
- [ ] Metadata support.
- [ ] Hints for best chunk size.
- [ ] Stream sequences (e.g. archives of multiple files).
- [ ] Multiplexing and demultiplexing.
- [ ] Automatic parallelization (if possible).
- [ ] Dynamic (run-time polymorphic) stage interface.
- [ ] Stable API.

## 3rd party packages

The *flod* package provides only basic building blocks.
Pipes doing useful transformations will be implemented in separate packages,
either natively in D or as wrappers over libraries written in other languages, mainly C.
Some of them are already under development:
- [flod-curl](https://github.com/epi/flod-curl) – download files from the web using libcurl.
- [flod-zlib](https://github.com/epi/flod-zlib) – inflate and deflate streams of data.
- [flod-mad](https://github.com/epi/flod-mad) – decode MPEG-compressed audio to raw PCM streams.
- [flod-alsa](https://github.com/epi/flod-alsa) – replay PCM streams using default audio output.

## Examples

To see what can be done with *flod*, see the examples found in
[this repo](https://github.com/epi/flod-examples).

## Release history

- v0.0.2 (2016-03-15)
  - Can use built-in arrays and input ranges as pipeline sources.
  - Can use output ranges as pipeline sinks.

- v0.0.1 (2016-03-13)
  - First release – can connect pipes with any interfaces (even incompatible).

## License

*flod* is licensed under Boost Software License v1.0, see the file COPYING for details.
External packages may use different licenses, so be careful when using them, especially when
combining pipes distributed under different licenses.
