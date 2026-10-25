package stream

import (
	"errors"
	"iter"
	"slices"
	"strconv"
	"testing"
)

type labeled struct {
	N     int
	Label string
}

func TestMapTypeChangingChain(t *testing.T) {
	got, err := From([]int{1, 2, 3}).
		MapValue(strconv.Itoa).
		Map(func(s string) (labeled, error) {
			n, err := strconv.Atoi(s)
			return labeled{n, "#" + s}, err
		}).
		Collect()
	if err != nil {
		t.Fatal(err)
	}
	want := []labeled{{1, "#1"}, {2, "#2"}, {3, "#3"}}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestMapCalledOncePerSuccess(t *testing.T) {
	calls := map[int]int{}
	p := newProbe(event{v: 1}, event{v: 2}, event{err: errX})
	_, errs := drain(p.stream().Map(func(v int) (int, error) {
		calls[v]++
		return v, nil
	}))
	if len(errs) != 1 {
		t.Fatalf("errs = %v", errs)
	}
	if len(calls) != 2 || calls[1] != 1 || calls[2] != 1 {
		t.Fatalf("calls = %v", calls)
	}
}

func TestMapCallbackError(t *testing.T) {
	wrapped := errors.Join(errX)
	p := newProbe(ok(1, 2, 3)...)
	calls := 0
	var got []event
	for v, err := range p.stream().Map(func(v int) (string, error) {
		calls++
		if v == 2 {
			return "discarded", wrapped
		}
		return strconv.Itoa(v), nil
	}).Seq() {
		got = append(got, event{len(v), err})
		if err != nil && v != "" {
			t.Fatalf("error emitted with non-zero value %q", v)
		}
	}
	if len(got) != 2 || got[0].err != nil || !errors.Is(got[1].err, errX) {
		t.Fatalf("got %v", got)
	}
	if calls != 2 || p.reads != 2 || p.closed != 1 {
		t.Fatalf("calls=%d reads=%d closed=%d", calls, p.reads, p.closed)
	}
}

func TestMapValue(t *testing.T) {
	got, _ := Of(1, 2).MapValue(func(v int) float64 { return float64(v) / 2 }).Collect()
	if !slices.Equal(got, []float64{0.5, 1}) {
		t.Fatalf("got %v", got)
	}
}

func TestFlatMapOrderAndEmptyChildren(t *testing.T) {
	got, err := Of(0, 1, 2, 3).FlatMap(func(n int) Stream[string] {
		out := make([]string, n)
		for i := range n {
			out[i] = strconv.Itoa(n) + "." + strconv.Itoa(i)
		}
		return From(out)
	}).Collect()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"1.0", "2.0", "2.1", "3.0", "3.1", "3.2"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestFlatMapChildErrorStopsOuter(t *testing.T) {
	outer := newProbe(ok(1, 2, 3)...)
	var children []*probe
	vals, errs := drain(outer.stream().FlatMap(func(v int) Stream[int] {
		c := newProbe(event{v: v * 10})
		if v == 2 {
			c = newProbe(event{v: 20}, event{err: errX}, event{v: 21})
		}
		children = append(children, c)
		return c.stream()
	}))
	if !slices.Equal(vals, []int{10, 20}) || len(errs) != 1 || !errors.Is(errs[0], errX) {
		t.Fatalf("vals=%v errs=%v", vals, errs)
	}
	if outer.reads != 2 || outer.closed != 1 || len(children) != 2 {
		t.Fatalf("outer reads=%d closed=%d children=%d", outer.reads, outer.closed, len(children))
	}
	if children[1].reads != 2 || children[1].closed != 1 {
		t.Fatalf("child reads=%d closed=%d", children[1].reads, children[1].closed)
	}
}

func TestFlatMapEarlyStopRunsAllDefers(t *testing.T) {
	outer := newProbe(ok(1, 2, 3)...)
	var children []*probe
	s := outer.stream().FlatMap(func(v int) Stream[int] {
		c := newProbe(ok(v*10, v*10+1, v*10+2)...)
		children = append(children, c)
		return c.stream()
	})
	var got []int
	for v := range s.Seq() {
		got = append(got, v)
		if v == 21 {
			break
		}
	}
	if !slices.Equal(got, []int{10, 11, 12, 20, 21}) {
		t.Fatalf("got %v", got)
	}
	if outer.reads != 2 || outer.closed != 1 {
		t.Fatalf("outer reads=%d closed=%d", outer.reads, outer.closed)
	}
	if len(children) != 2 || children[1].reads != 2 || children[1].closed != 1 || children[0].closed != 1 {
		t.Fatalf("children=%d", len(children))
	}
}

func TestFlatMapNotCalledOnUpstreamError(t *testing.T) {
	calls := 0
	_, errs := drain(newProbe(event{err: errX}).stream().FlatMap(func(int) Stream[int] {
		calls++
		return Of(1)
	}))
	if calls != 0 || len(errs) != 1 {
		t.Fatalf("calls=%d errs=%v", calls, errs)
	}
}

func TestFlatMapUninitializedChildPanics(t *testing.T) {
	mustPanic(t, "stream: uninitialized Stream (use a constructor such as From or FromSeq2)", func() {
		_, _ = Of(1).FlatMap(func(int) Stream[int] { return Stream[int]{} }).Collect()
	})
}

func TestFlatMapValue(t *testing.T) {
	reads := 0
	s := Of(5, 5).FlatMapValue(func(n int) iter.Seq[int] { return countingSeq(n, &reads) })
	got, _ := s.Limit(7).Collect()
	if !slices.Equal(got, []int{0, 1, 2, 3, 4, 0, 1}) {
		t.Fatalf("got %v", got)
	}
	if reads != 7 {
		t.Fatalf("reads = %d, want 7", reads)
	}
	mustPanic(t, "stream: FlatMapValue callback returned nil iterator", func() {
		_, _ = Of(1).FlatMapValue(func(int) iter.Seq[int] { return nil }).Collect()
	})
}

func TestPeekLazyAndRepeats(t *testing.T) {
	var seen []int
	s := From([]int{1, 2}).PeekValue(func(v int) { seen = append(seen, v) })
	if len(seen) != 0 {
		t.Fatal("PeekValue ran before traversal")
	}
	_, _ = s.Collect()
	_, _ = s.Collect()
	if !slices.Equal(seen, []int{1, 2, 1, 2}) {
		t.Fatalf("seen = %v", seen)
	}
}

func TestPeekError(t *testing.T) {
	p := newProbe(ok(1, 2, 3)...)
	vals, errs := drain(p.stream().Peek(func(v int) error {
		if v == 2 {
			return errX
		}
		return nil
	}))
	if !slices.Equal(vals, []int{1}) || len(errs) != 1 || !errors.Is(errs[0], errX) || p.reads != 2 {
		t.Fatalf("vals=%v errs=%v reads=%d", vals, errs, p.reads)
	}
}
