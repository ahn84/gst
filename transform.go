package stream

import "iter"

// Map returns a stream of f applied to each element.
//
// f is called exactly once per successful upstream element and never for an
// upstream error. If f returns an error, the stream yields (zero R, err) and
// stops; the value f returned alongside the error is discarded.
func (s Stream[T]) Map[R any](f func(T) (R, error)) Stream[R] {
	seq := s.iter()
	return newStream(func(yield func(R, error) bool) {
		for v, err := range seq {
			if err != nil {
				var zero R
				yield(zero, err)
				return
			}
			mapped, err := f(v)
			if err != nil {
				var zero R
				yield(zero, err)
				return
			}
			if !yield(mapped, nil) {
				return
			}
		}
	})
}

// MapValue is like [Stream.Map] for a mapping that cannot fail.
func (s Stream[T]) MapValue[R any](f func(T) R) Stream[R] {
	seq := s.iter()
	return newStream(func(yield func(R, error) bool) {
		for v, err := range seq {
			if err != nil {
				var zero R
				yield(zero, err)
				return
			}
			if !yield(f(v), nil) {
				return
			}
		}
	})
}

// FlatMap returns a stream of the elements of f(v) for each element v, in
// order. Each child stream is traversed completely before the next element is
// read from upstream.
//
// An error from upstream or from a child ends the whole stream. When the
// consumer stops, both the current child and the upstream stop. FlatMap
// panics if f returns an uninitialized Stream.
func (s Stream[T]) FlatMap[R any](f func(T) Stream[R]) Stream[R] {
	seq := s.iter()
	return newStream(func(yield func(R, error) bool) {
		for outer, err := range seq {
			if err != nil {
				var zero R
				yield(zero, err)
				return
			}
			for inner, err := range f(outer).iter() {
				if err != nil {
					var zero R
					yield(zero, err)
					return
				}
				if !yield(inner, nil) {
					return
				}
			}
		}
	})
}

// FlatMapValue is like [Stream.FlatMap] for children given as plain
// [iter.Seq] iterators. It panics if f returns a nil iterator.
func (s Stream[T]) FlatMapValue[R any](f func(T) iter.Seq[R]) Stream[R] {
	seq := s.iter()
	return newStream(func(yield func(R, error) bool) {
		for outer, err := range seq {
			if err != nil {
				var zero R
				yield(zero, err)
				return
			}
			child := f(outer)
			if child == nil {
				panic("stream: FlatMapValue callback returned nil iterator")
			}
			for inner := range child {
				if !yield(inner, nil) {
					return
				}
			}
		}
	})
}

// Peek calls f for each element as it passes through and forwards it
// unchanged. If f returns an error, the stream yields it and stops.
//
// f runs during traversal only, and runs again on every traversal; it must
// not assume exactly-once execution.
func (s Stream[T]) Peek(f func(T) error) Stream[T] {
	seq := s.iter()
	return newStream(func(yield func(T, error) bool) {
		for v, err := range seq {
			if err == nil {
				err = f(v)
			}
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

// PeekValue is like [Stream.Peek] for a callback that cannot fail.
func (s Stream[T]) PeekValue(f func(T)) Stream[T] {
	seq := s.iter()
	return newStream(func(yield func(T, error) bool) {
		for v, err := range seq {
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			f(v)
			if !yield(v, nil) {
				return
			}
		}
	})
}
