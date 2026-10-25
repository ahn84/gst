package stream

import (
	"cmp"
	"errors"
	"slices"
	"testing"
)

// record is not comparable, so DistinctBy must rely on its key alone.
type record struct {
	ID   string
	Tags []string
	Meta map[string]int
}

func TestDistinctByNonComparable(t *testing.T) {
	in := []record{
		{ID: "a", Tags: []string{"first"}},
		{ID: "b"},
		{ID: "a", Tags: []string{"second"}},
		{ID: "c", Meta: map[string]int{"k": 1}},
		{ID: "b"},
	}
	got, err := From(in).DistinctBy(func(r record) string { return r.ID }).Collect()
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(got))
	for i, r := range got {
		ids[i] = r.ID
	}
	if !slices.Equal(ids, []string{"a", "b", "c"}) || got[0].Tags[0] != "first" {
		t.Fatalf("got %+v", got)
	}
}

func TestDistinctByFreshStatePerTraversal(t *testing.T) {
	s := Of(1, 1, 2).DistinctBy(func(v int) int { return v })
	a, _ := s.Collect()
	b, _ := s.Collect()
	if !slices.Equal(a, []int{1, 2}) || !slices.Equal(b, []int{1, 2}) {
		t.Fatalf("a=%v b=%v", a, b)
	}
}

func TestDistinctByErrorAfterDuplicates(t *testing.T) {
	p := newProbe(event{v: 1}, event{v: 1}, event{err: errX})
	vals, errs := drain(p.stream().DistinctBy(func(v int) int { return v }))
	if !slices.Equal(vals, []int{1}) || len(errs) != 1 {
		t.Fatalf("vals=%v errs=%v", vals, errs)
	}
}

func TestSorted(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []int
		want []int
	}{
		{"empty", nil, nil},
		{"duplicates", []int{3, 1, 3, 2, 1}, []int{1, 1, 2, 3, 3}},
		{"sorted", []int{1, 2, 3}, []int{1, 2, 3}},
		{"reversed", []int{3, 2, 1}, []int{1, 2, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := From(tc.in).Sorted(cmp.Compare[int]).Collect()
			if err != nil || !slices.Equal(got, tc.want) {
				t.Fatalf("got %v, %v", got, err)
			}
		})
	}
}

func TestSortedEmitsNothingBeforeUpstreamCompletes(t *testing.T) {
	p := newProbe(ok(3, 1, 2)...)
	for range p.stream().Sorted(cmp.Compare[int]).Seq() {
		if p.closed != 1 || p.reads != 3 {
			t.Fatalf("value emitted before upstream finished: reads=%d closed=%d", p.reads, p.closed)
		}
	}
}

func TestSortedDropsBufferOnError(t *testing.T) {
	p := newProbe(event{v: 3}, event{v: 1}, event{err: errX})
	vals, errs := drain(p.stream().Sorted(cmp.Compare[int]))
	if len(vals) != 0 || len(errs) != 1 || !errors.Is(errs[0], errX) {
		t.Fatalf("vals=%v errs=%v", vals, errs)
	}
}

func TestSortedThenLimitReadsEverything(t *testing.T) {
	p := newProbe(ok(5, 4, 3, 2, 1)...)
	got, _ := p.stream().Sorted(cmp.Compare[int]).Limit(1).Collect()
	if !slices.Equal(got, []int{1}) {
		t.Fatalf("got %v", got)
	}
	if p.reads != 5 {
		t.Fatalf("reads = %d; Sorted must consume the whole upstream", p.reads)
	}
}
