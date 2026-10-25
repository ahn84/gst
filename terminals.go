package stream

// Collect returns all elements in order. On error it returns (nil, err) and
// discards any elements already read. Memory is O(N).
func (s Stream[T]) Collect() ([]T, error) {
	var out []T
	for v, err := range s.iter() {
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// Count returns the number of elements, or (0, err) on error.
func (s Stream[T]) Count() (int, error) {
	n := 0
	for _, err := range s.iter() {
		if err != nil {
			return 0, err
		}
		n++
	}
	return n, nil
}

// First returns the first element and stops the stream without reading
// further. It returns (zero, false, nil) for an empty stream and
// (zero, false, err) if the first event is an error.
func (s Stream[T]) First() (value T, ok bool, err error) {
	for v, err := range s.iter() {
		if err != nil {
			var zero T
			return zero, false, err
		}
		return v, true, nil
	}
	var zero T
	return zero, false, nil
}

// ForEach calls action for each element in order. It stops at the first
// upstream or action error and returns it. Effects of actions already run are
// not undone.
func (s Stream[T]) ForEach(action func(T) error) error {
	for v, err := range s.iter() {
		if err == nil {
			err = action(v)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Reduce folds the elements into an accumulator, starting from seed. On any
// upstream or f error it returns (zero R, err), discarding the partial
// accumulator.
//
// seed is passed by value. If R is a pointer, map or slice, mutations made by
// f before an error remain visible through the caller's reference.
func (s Stream[T]) Reduce[R any](seed R, f func(R, T) (R, error)) (R, error) {
	acc := seed
	for v, err := range s.iter() {
		if err == nil {
			acc, err = f(acc, v)
		}
		if err != nil {
			var zero R
			return zero, err
		}
	}
	return acc, nil
}

// GroupBy groups elements by key, preserving encounter order within each
// group. On error it returns (nil, err). Memory is O(N). Map iteration order
// is unspecified.
func (s Stream[T]) GroupBy[K comparable](key func(T) K) (map[K][]T, error) {
	groups := make(map[K][]T)
	for v, err := range s.iter() {
		if err != nil {
			return nil, err
		}
		k := key(v)
		groups[k] = append(groups[k], v)
	}
	return groups, nil
}
