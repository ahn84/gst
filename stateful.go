package stream

import "slices"

// DistinctBy returns a stream that keeps the first element for each key and
// drops later elements with the same key, preserving encounter order.
//
// A fresh key set is created for each traversal. Memory is O(number of
// distinct keys) per traversal. T itself need not be comparable.
func (s Stream[T]) DistinctBy[K comparable](key func(T) K) Stream[T] {
	seq := s.iter()
	return newStream(func(yield func(T, error) bool) {
		seen := make(map[K]struct{})
		for v, err := range seq {
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			k := key(v)
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
			if !yield(v, nil) {
				return
			}
		}
	})
}

// Sorted returns a stream of the elements ordered by compare, as with
// [slices.SortFunc]. The sort is not guaranteed to be stable.
//
// Sorted is lazy to start, but on traversal it reads and buffers the whole
// upstream before yielding anything: O(N) memory and O(N log N) time, and it
// never completes on an infinite stream. A downstream Limit does not reduce
// that cost. On an upstream error the buffer is discarded and only the error
// is yielded.
func (s Stream[T]) Sorted(compare func(T, T) int) Stream[T] {
	seq := s.iter()
	return newStream(func(yield func(T, error) bool) {
		var values []T
		for v, err := range seq {
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			values = append(values, v)
		}
		slices.SortFunc(values, compare)
		for _, v := range values {
			if !yield(v, nil) {
				return
			}
		}
	})
}
