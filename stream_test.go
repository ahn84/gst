package stream

import (
	"errors"
	"iter"
	"slices"
	"sync"
	"testing"
)

var (
	errX = errors.New("x")
	errY = errors.New("y")
)

// event is one (value, error) pair emitted by a probe.
type event struct {
	v   int
	err error
}

func ok(vs ...int) []event {
	out := make([]event, len(vs))
	for i, v := range vs {
		out[i] = event{v: v}
	}
	return out
}

// probe is an instrumented source. It records how many events it emitted and
// whether its deferred cleanup ran. It deliberately keeps emitting after an
// error so tests can check that consumers stop at the first one.
type probe struct {
	events  []event
	started int
	reads   int
	closed  int
}

func newProbe(events ...event) *probe { return &probe{events: events} }

func (p *probe) seq2() iter.Seq2[int, error] {
	return func(yield func(int, error) bool) {
		p.started++
		defer func() { p.closed++ }()
		for _, e := range p.events {
			p.reads++
			if !yield(e.v, e.err) {
				return
			}
		}
	}
}

func (p *probe) stream() Stream[int] { return FromSeq2(p.seq2()) }

// countingSeq yields 0..n-1 and reports reads through *reads.
func countingSeq(n int, reads *int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := range n {
			*reads++
			if !yield(i) {
				return
			}
		}
	}
}

// drain collects every event, including errors, without stopping early.
func drain[T any](s Stream[T]) (vals []T, errs []error) {
	for v, err := range s.Seq() {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		vals = append(vals, v)
	}
	return vals, errs
}

func mustPanic(t *testing.T, want string, f func()) {
	t.Helper()
	defer func() {
		t.Helper()
		r := recover()
		if r == nil {
			t.Fatalf("expected panic %q", want)
		}
		if r != want {
			t.Fatalf("panic = %v, want %q", r, want)
		}
	}()
	f()
}

func TestFrom(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []int
	}{
		{"nil", nil},
		{"empty", []int{}},
		{"some", []int{3, 1, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := From(tc.in).Collect()
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, tc.in) {
				t.Fatalf("got %v, want %v", got, tc.in)
			}
		})
	}
}

func TestFromReadsSliceLazily(t *testing.T) {
	in := []int{1, 2}
	s := From(in)
	in[0] = 9
	got, _ := s.Collect()
	if !slices.Equal(got, []int{9, 2}) {
		t.Fatalf("got %v; From should not copy the slice", got)
	}
}

func TestOf(t *testing.T) {
	got, err := Of("a", "b", "c").Collect()
	if err != nil || !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("got %v, %v", got, err)
	}
	if n, _ := Of[int]().Count(); n != 0 {
		t.Fatalf("Of() count = %d", n)
	}
}

func TestFromSeq(t *testing.T) {
	vals, errs := drain(FromSeq(slices.Values([]int{0, 1, 2})))
	if len(errs) != 0 || !slices.Equal(vals, []int{0, 1, 2}) {
		t.Fatalf("got %v, %v", vals, errs)
	}

	reads := 0
	for v := range FromSeq(countingSeq(100, &reads)).Seq() {
		if v == 2 {
			break
		}
	}
	if reads != 3 {
		t.Fatalf("reads = %d, want 3", reads)
	}
}

func TestFromSeq2NormalizesFirstError(t *testing.T) {
	p := newProbe(event{v: 1}, event{v: 2, err: errX}, event{v: 3}, event{err: errY})
	var got []event
	for v, err := range p.stream().Seq() {
		got = append(got, event{v, err})
	}
	want := []event{{v: 1}, {v: 0, err: errX}}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if p.reads != 2 || p.closed != 1 {
		t.Fatalf("reads=%d closed=%d, want 2 and 1", p.reads, p.closed)
	}
}

func TestNilIteratorPanics(t *testing.T) {
	mustPanic(t, "stream: nil iterator", func() { FromSeq2[int](nil) })
	mustPanic(t, "stream: nil iterator", func() { FromSeq[int](nil) })
}

func TestZeroStreamPanics(t *testing.T) {
	const msg = "stream: uninitialized Stream (use a constructor such as From or FromSeq2)"
	mustPanic(t, msg, func() { Stream[int]{}.Seq() })
	mustPanic(t, msg, func() { _, _ = Stream[int]{}.Collect() })
	mustPanic(t, msg, func() { Stream[int]{}.Limit(1) })
}

func TestSeqEarlyStop(t *testing.T) {
	p := newProbe(ok(1, 2, 3, 4)...)
	for v, err := range p.stream().Seq() {
		if err != nil {
			t.Fatal(err)
		}
		if v == 2 {
			break
		}
	}
	if p.reads != 2 || p.closed != 1 {
		t.Fatalf("reads=%d closed=%d, want 2 and 1", p.reads, p.closed)
	}
}

func TestRetraversal(t *testing.T) {
	s := From([]int{1, 2, 3}).MapValue(func(v int) int { return v * 2 })
	a, _ := s.Collect()
	b, _ := s.Collect()
	if !slices.Equal(a, b) || !slices.Equal(a, []int{2, 4, 6}) {
		t.Fatalf("a=%v b=%v", a, b)
	}
}

// opCase applies one operator to an int stream, for invariant tests that run
// against every operator.
type opCase struct {
	name string
	op   func(Stream[int]) Stream[int]
}

var allOps = []opCase{
	{"Map", func(s Stream[int]) Stream[int] { return s.Map(func(v int) (int, error) { return v, nil }) }},
	{"MapValue", func(s Stream[int]) Stream[int] { return s.MapValue(func(v int) int { return v }) }},
	{"FlatMap", func(s Stream[int]) Stream[int] { return s.FlatMap(func(v int) Stream[int] { return Of(v) }) }},
	{"FlatMapValue", func(s Stream[int]) Stream[int] {
		return s.FlatMapValue(func(v int) iter.Seq[int] { return slices.Values([]int{v}) })
	}},
	{"Peek", func(s Stream[int]) Stream[int] { return s.Peek(func(int) error { return nil }) }},
	{"PeekValue", func(s Stream[int]) Stream[int] { return s.PeekValue(func(int) {}) }},
	{"Filter", func(s Stream[int]) Stream[int] { return s.Filter(func(int) (bool, error) { return true, nil }) }},
	{"FilterValue", func(s Stream[int]) Stream[int] { return s.FilterValue(func(int) bool { return true }) }},
	{"Limit", func(s Stream[int]) Stream[int] { return s.Limit(1 << 30) }},
	{"Skip", func(s Stream[int]) Stream[int] { return s.Skip(0) }},
	{"TakeWhile", func(s Stream[int]) Stream[int] { return s.TakeWhile(func(int) (bool, error) { return true, nil }) }},
	{"DistinctBy", func(s Stream[int]) Stream[int] { return s.DistinctBy(func(v int) int { return v }) }},
	{"Sorted", func(s Stream[int]) Stream[int] { return s.Sorted(func(a, b int) int { return a - b }) }},
	{"OnErrorResume", func(s Stream[int]) Stream[int] {
		return s.OnErrorResume(func(err error) Stream[int] { return FromSeq2(func(yield func(int, error) bool) { yield(0, err) }) })
	}},
}

func TestAllOpsForwardUpstreamErrorOnce(t *testing.T) {
	for _, tc := range allOps {
		t.Run(tc.name, func(t *testing.T) {
			p := newProbe(event{v: 1}, event{err: errX}, event{v: 2})
			vals, errs := drain(tc.op(p.stream()))
			if len(errs) != 1 || !errors.Is(errs[0], errX) {
				t.Fatalf("errs = %v, want exactly [x]", errs)
			}
			if slices.Contains(vals, 2) {
				t.Fatalf("value after error was emitted: %v", vals)
			}
			if p.reads != 2 || p.closed != 1 {
				t.Fatalf("reads=%d closed=%d, want 2 and 1", p.reads, p.closed)
			}
		})
	}
}

func TestAllOpsHonorEarlyStop(t *testing.T) {
	for _, tc := range allOps {
		t.Run(tc.name, func(t *testing.T) {
			p := newProbe(ok(1, 2, 3, 4, 5)...)
			for range tc.op(p.stream()).Seq() {
				break
			}
			want := 1
			if tc.name == "Sorted" {
				want = 5 // Sorted must buffer everything before yielding.
			}
			if p.reads != want || p.closed != 1 {
				t.Fatalf("reads=%d closed=%d, want %d and 1", p.reads, p.closed, want)
			}
		})
	}
}

func TestAllOpsAreLazy(t *testing.T) {
	for _, tc := range allOps {
		t.Run(tc.name, func(t *testing.T) {
			p := newProbe(ok(1, 2)...)
			_ = tc.op(p.stream())
			if p.started != 0 {
				t.Fatal("operator started upstream before traversal")
			}
		})
	}
}

// TestConcurrentTraversalOfReplayableSource exercises -race: a slice-backed
// pipeline with traversal-local state may be consumed concurrently.
func TestConcurrentTraversalOfReplayableSource(t *testing.T) {
	s := From([]int{3, 1, 2, 3, 1}).
		DistinctBy(func(v int) int { return v }).
		Sorted(func(a, b int) int { return a - b })
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			got, err := s.Collect()
			if err != nil || !slices.Equal(got, []int{1, 2, 3}) {
				t.Errorf("got %v, %v", got, err)
			}
		})
	}
	wg.Wait()
}
