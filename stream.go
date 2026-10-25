// Package stream provides lazy, synchronous, error-aware generic streams
// built on [iter.Seq2].
//
// A [Stream] carries successful elements as (value, nil) and a failure as
// (zero, err). All built-in operators are fail-fast: the first error ends the
// traversal, it is delivered exactly once, and no user callback or source read
// happens after it. Returning false from yield (for example by breaking out of
// a range loop) stops every upstream stage immediately.
//
// # Laziness and reuse
//
// A Stream is a recipe for traversal, not storage. Nothing runs until a
// terminal operation (Collect, Count, First, ForEach, Reduce, GroupBy) or a
// range over [Stream.Seq] starts. Each traversal re-runs the whole pipeline,
// including side effects in Peek callbacks. Whether a stream can be traversed
// more than once depends on its source: a slice-backed stream can, but a
// stream backed by a database cursor, channel, file scanner or stateful
// generator may be one-shot.
//
// # Concurrency
//
// The library creates no goroutines or channels. Concurrent traversals are
// safe only if the source and every user callback support concurrent use.
//
// # Generic methods
//
// Type-changing operators such as Map, FlatMap, Reduce and GroupBy are
// generic methods, which Go 1.27 supports on concrete types only. Stream is
// therefore a struct; there is no fluent stream interface.
package stream

import "iter"

// Stream is a lazy, error-aware sequence of T values.
//
// The zero Stream is invalid; use a constructor such as [From], [Of],
// [FromSeq] or [FromSeq2]. Operating on a zero Stream panics.
type Stream[T any] struct {
	seq iter.Seq2[T, error]
}

// newStream wraps an iterator produced by this package. Built-in operators
// already honor the fail-fast contract, so they skip FromSeq2 normalization.
func newStream[T any](seq iter.Seq2[T, error]) Stream[T] {
	return Stream[T]{seq: seq}
}

// iter returns the underlying iterator, panicking on an uninitialized Stream.
func (s Stream[T]) iter() iter.Seq2[T, error] {
	if s.seq == nil {
		panic("stream: uninitialized Stream (use a constructor such as From or FromSeq2)")
	}
	return s.seq
}

// From returns a stream over values in order.
//
// The slice is not copied; it is read on each traversal, so changes to it
// before or during traversal are visible. A nil or empty slice yields an
// empty stream.
func From[T any](values []T) Stream[T] {
	return newStream(func(yield func(T, error) bool) {
		for _, v := range values {
			if !yield(v, nil) {
				return
			}
		}
	})
}

// Of returns a stream over the given values in order.
func Of[T any](values ...T) Stream[T] {
	return From(values)
}

// FromSeq returns a stream that yields each value of src as (v, nil).
//
// FromSeq panics if src is nil.
func FromSeq[T any](src iter.Seq[T]) Stream[T] {
	if src == nil {
		panic("stream: nil iterator")
	}
	return newStream(func(yield func(T, error) bool) {
		for v := range src {
			if !yield(v, nil) {
				return
			}
		}
	})
}

// FromSeq2 returns a stream over src, normalizing it to the fail-fast
// contract: values up to the first non-nil error are forwarded, the error is
// forwarded once as (zero, err), and src is then stopped. Any value src pairs
// with an error is discarded.
//
// FromSeq2 panics if src is nil.
func FromSeq2[T any](src iter.Seq2[T, error]) Stream[T] {
	if src == nil {
		panic("stream: nil iterator")
	}
	return newStream(func(yield func(T, error) bool) {
		for v, err := range src {
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			if !yield(v, nil) {
				return
			}
		}
	})
}

// Seq returns the stream as an iterator for use with range:
//
//	for v, err := range s.Seq() {
//		if err != nil {
//			return err
//		}
//		...
//	}
//
// At most one non-nil error is yielded, and it is always the last event.
func (s Stream[T]) Seq() iter.Seq2[T, error] {
	return s.iter()
}
