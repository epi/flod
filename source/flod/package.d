/** High-level interface for $(B flod). Provides the most commonly used functions of the package.
 *
 * Package_description:
 *
 * $(B flod) is a library for processing streams of data using composable building blocks.
 *
 * In $(B flod), a $(I stream) is a chain of one or more $(I components), which communicate with
 * each other using the specified interfaces.
 *
 * A stream component can be either a $(I source), a $(I sink), or a $(I filter), which is
 * both a sink and a source. A source-only component is a component that only produces data, e.g.
 * reads a file or generates random bytes. A sink-only component is a component that only consumes
 * data from previous stream components. It may use the data for example to build an array or replays
 * audio samples. A filter component receives data on its sink end,
 * and produces transformed data on its source end. Examples of such components are media decoders
 * or cipher implementations.
 *
 * There are four methods of passing data from a source to a sink:
 * $(TABLE
 *  $(TR $(TH method)     $(TH description)                                         $(TH buffer is owned by))
 *  $(TR $(TD $(I pull))  $(TD `sink` calls `source.pull()`)                        $(TD sink))
 *  $(TR $(TD $(I push))  $(TD `source` calls `sink.push()`)                        $(TD source))
 *  $(TR $(TD $(I peek))  $(TD `sink` calls `source.peek()` and `source.consume()`) $(TD source))
 *  $(TR $(TD $(I alloc)) $(TD `source` calls `sink.alloc()` and `sink.commit()`)   $(TD sink))
 * )
 *
 * Note that a filter can mix different methods for its sink and source ends. For example, it may
 * call `peek` and `consume` to access the input data, and output transformed data using `push`.
 *
 *  Authors: $(LINK2 https://github.com/epi, Adrian Matoga)
 *  Copyright: © 2016 Adrian Matoga
 *  License: $(LINK2 http://www.boost.org/users/license.html, BSL-1.0).
 */
module flod;
