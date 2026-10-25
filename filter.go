package stream

// Filter returns a stream of the elements for which predicate returns true.
// A predicate error is yielded as (zero, err) and ends the stream.
func (s Stream[T]) Filter(predicate func(T) (bool, error)) Stream[T] {
	seq := s.iter()
	return newStream(func(yield func(T, error) bool) {
		for v, err := range seq {
			keep := false
			if err == nil {
				keep, err = predicate(v)
			}
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			if keep && !yield(v, nil) {
				return
			}
		}
	})
}

// FilterValue is like [Stream.Filter] for a predicate that cannot fail.
func (s Stream[T]) FilterValue(predicate func(T) bool) Stream[T] {
	seq := s.iter()
	return newStream(func(yield func(T, error) bool) {
		for v, err := range seq {
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			if predicate(v) && !yield(v, nil) {
				return
			}
		}
	})
}

// Limit returns a stream of at most n elements. Upstream is stopped as soon
// as the nth element has been delivered, so later elements and errors are
// never read. If n <= 0, upstream is not started at all.
func (s Stream[T]) Limit(n int) Stream[T] {
	seq := s.iter()
	return newStream(func(yield func(T, error) bool) {
		if n <= 0 {
			return
		}
		remaining := n
		for v, err := range seq {
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			if !yield(v, nil) {
				return
			}
			remaining--
			if remaining == 0 {
				return
			}
		}
	})
}

// Skip returns a stream that discards the first n elements (none if
// n <= 0). An upstream error while skipping is still yielded.
func (s Stream[T]) Skip(n int) Stream[T] {
	seq := s.iter()
	return newStream(func(yield func(T, error) bool) {
		skipped := 0
		for v, err := range seq {
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			if skipped < n {
				skipped++
				continue
			}
			if !yield(v, nil) {
				return
			}
		}
	})
}

// TakeWhile returns a stream of the leading elements for which predicate
// returns true. At the first false it stops without yielding that element or
// reading further. A predicate error is yielded and ends the stream.
func (s Stream[T]) TakeWhile(predicate func(T) (bool, error)) Stream[T] {
	seq := s.iter()
	return newStream(func(yield func(T, error) bool) {
		for v, err := range seq {
			keep := false
			if err == nil {
				keep, err = predicate(v)
			}
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			if !keep || !yield(v, nil) {
				return
			}
		}
	})
}
