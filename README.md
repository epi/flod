[![Build Status](https://travis-ci.org/epi/flod.svg?branch=master)](https://travis-ci.org/epi/flod)
# flod

*flod* is a D library for composing stream processing pipelines.
API documentation is available [here](http://epi.github.io/flod/ddox/flod.html).

## 3rd party packages

By convention, 3rd party packages (e.g. wrappers for existing libraries) should be
named flod-xxx.

## Examples

See the examples directory. Currently there is one example, replaying mp3 files using MAD.
Extra packages are required to build it. They are not in dub repository yet, so they need to
be downloaded manually: [flod-alsa](https://github.com/epi/flod-alsa) and
[flod-mad](https://github.com/epi/flod-mad).

## License

*flod* is licensed under Boost Software License v1.0, see the file COPYING for details.
