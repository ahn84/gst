package stream

// OnErrorResume returns a stream that yields elements until the first error,
// then, instead of yielding that error, yields the elements of f(err). The
// original upstream is not resumed.
//
// f is called at most once, and only if an error occurs; stopping before an
// error never calls it. Errors from the replacement stream are yielded and
// end the stream. OnErrorResume panics if f returns an uninitialized Stream.
func (s Stream[T]) OnErrorResume(f func(error) Stream[T]) Stream[T] {
	seq := s.iter()
	return newStream(func(yield func(T, error) bool) {
		for v, err := range seq {
			if err != nil {
				for rv, rerr := range f(err).iter() {
					if rerr != nil {
						var zero T
						yield(zero, rerr)
						return
					}
					if !yield(rv, nil) {
						return
					}
				}
				return
			}
			if !yield(v, nil) {
				return
			}
		}
	})
}
