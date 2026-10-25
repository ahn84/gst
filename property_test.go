package stream

import (
	"errors"
	"slices"
	"testing"
)

// bytesToInts gives fuzz inputs a small value range so duplicates are common.
func bytesToInts(data []byte) []int {
	out := make([]int, len(data))
	for i, b := range data {
		out[i] = int(b % 16)
	}
	return out
}

func isSubsequence(sub, full []int) bool {
	i := 0
	for _, v := range full {
		if i < len(sub) && sub[i] == v {
			i++
		}
	}
	return i == len(sub)
}

func FuzzMapValueComposition(f *testing.F) {
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		in := bytesToInts(data)
		g := func(v int) int { return v*3 + 1 }
		h := func(v int) int { return v ^ 5 }
		got, _ := From(in).MapValue(g).MapValue(h).Collect()
		var want []int
		for _, v := range in {
			want = append(want, h(g(v)))
		}
		if !slices.Equal(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
}

func FuzzFilterValuePreservesOrder(f *testing.F) {
	f.Add([]byte{5, 2, 8, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		in := bytesToInts(data)
		got, _ := From(in).FilterValue(isEven).Collect()
		if !isSubsequence(got, in) || slices.ContainsFunc(got, func(v int) bool { return !isEven(v) }) {
			t.Fatalf("got %v from %v", got, in)
		}
	})
}

func FuzzLimit(f *testing.F) {
	f.Add([]byte{1, 2, 3}, 2)
	f.Add([]byte{1}, -3)
	f.Fuzz(func(t *testing.T, data []byte, n int) {
		in := bytesToInts(data)
		got, _ := From(in).Limit(n).Collect()
		if len(got) > max(n, 0) || !slices.Equal(got, in[:len(got)]) {
			t.Fatalf("Limit(%d) of %v = %v", n, in, got)
		}
		if len(got) != min(max(n, 0), len(in)) {
			t.Fatalf("Limit(%d) of %v = %v", n, in, got)
		}
	})
}

func FuzzDistinctBy(f *testing.F) {
	f.Add([]byte{1, 1, 2, 17, 2})
	f.Fuzz(func(t *testing.T, data []byte) {
		in := bytesToInts(data)
		got, _ := From(in).DistinctBy(func(v int) int { return v % 4 }).Collect()
		seen := map[int]bool{}
		for _, v := range got {
			if seen[v%4] {
				t.Fatalf("key %d repeated in %v", v%4, got)
			}
			seen[v%4] = true
		}
		if !isSubsequence(got, in) {
			t.Fatalf("%v is not a subsequence of %v", got, in)
		}
	})
}

// FuzzNoWorkAfterError checks, for every operator, that a source error at an
// arbitrary position is reported once and nothing past it is read.
func FuzzNoWorkAfterError(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4}, uint8(2))
	f.Fuzz(func(t *testing.T, data []byte, at uint8) {
		in := bytesToInts(data)
		pos := int(at) % (len(in) + 1)
		events := append(ok(in[:pos]...), event{err: errX})
		events = append(events, ok(in[pos:]...)...)
		for _, tc := range allOps {
			p := newProbe(events...)
			_, errs := drain(tc.op(p.stream()))
			if len(errs) != 1 || !errors.Is(errs[0], errX) || p.reads != pos+1 {
				t.Fatalf("%s: errs=%v reads=%d, want 1 error and %d reads", tc.name, errs, p.reads, pos+1)
			}
		}
	})
}
