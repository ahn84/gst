package stream

import (
	"errors"
	"slices"
	"testing"
)

func isEven(v int) bool { return v%2 == 0 }

func TestFilterValue(t *testing.T) {
	for _, tc := range []struct {
		name string
		pred func(int) bool
		want []int
	}{
		{"all", func(int) bool { return true }, []int{1, 2, 3, 4}},
		{"none", func(int) bool { return false }, nil},
		{"some", isEven, []int{2, 4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Of(1, 2, 3, 4).FilterValue(tc.pred).Collect()
			if err != nil || !slices.Equal(got, tc.want) {
				t.Fatalf("got %v, %v; want %v", got, err, tc.want)
			}
		})
	}
}

func TestFilterError(t *testing.T) {
	p := newProbe(ok(1, 2, 3, 4)...)
	vals, errs := drain(p.stream().Filter(func(v int) (bool, error) {
		if v == 3 {
			return true, errX
		}
		return isEven(v), nil
	}))
	if !slices.Equal(vals, []int{2}) || len(errs) != 1 || !errors.Is(errs[0], errX) || p.reads != 3 {
		t.Fatalf("vals=%v errs=%v reads=%d", vals, errs, p.reads)
	}
}

func TestLimitNonPositiveNeverStarts(t *testing.T) {
	for _, n := range []int{0, -1} {
		p := newProbe(ok(1, 2)...)
		got, err := p.stream().Limit(n).Collect()
		if err != nil || len(got) != 0 || p.started != 0 {
			t.Fatalf("Limit(%d): got %v, %v, started=%d", n, got, err, p.started)
		}
	}
}

func TestLimit(t *testing.T) {
	p := newProbe(ok(1, 2, 3, 4)...)
	got, err := p.stream().Limit(2).Collect()
	if err != nil || !slices.Equal(got, []int{1, 2}) {
		t.Fatalf("got %v, %v", got, err)
	}
	if p.reads != 2 || p.closed != 1 {
		t.Fatalf("reads=%d closed=%d; must not read the third value", p.reads, p.closed)
	}
}

func TestLimitErrors(t *testing.T) {
	before := newProbe(event{v: 1}, event{err: errX}, event{v: 3})
	if _, err := before.stream().Limit(2).Collect(); !errors.Is(err, errX) {
		t.Fatalf("error before limit: got %v", err)
	}
	after := newProbe(event{v: 1}, event{v: 2}, event{err: errX})
	got, err := after.stream().Limit(2).Collect()
	if err != nil || !slices.Equal(got, []int{1, 2}) || after.reads != 2 {
		t.Fatalf("error after limit observed: %v, %v, reads=%d", got, err, after.reads)
	}
}

func TestLimitLongerThanInput(t *testing.T) {
	got, _ := Of(1, 2).Limit(5).Collect()
	if !slices.Equal(got, []int{1, 2}) {
		t.Fatalf("got %v", got)
	}
}

func TestSkip(t *testing.T) {
	for _, tc := range []struct {
		n    int
		want []int
	}{
		{0, []int{1, 2, 3}},
		{-2, []int{1, 2, 3}},
		{2, []int{3}},
		{5, nil},
	} {
		got, err := Of(1, 2, 3).Skip(tc.n).Collect()
		if err != nil || !slices.Equal(got, tc.want) {
			t.Fatalf("Skip(%d): got %v, %v; want %v", tc.n, got, err, tc.want)
		}
	}
}

func TestSkipErrorWhileSkipping(t *testing.T) {
	p := newProbe(event{v: 1}, event{err: errX}, event{v: 3})
	if _, err := p.stream().Skip(2).Collect(); !errors.Is(err, errX) {
		t.Fatalf("got %v", err)
	}
}

func TestTakeWhile(t *testing.T) {
	p := newProbe(ok(2, 4, 5, 6, 8)...)
	var visited []int
	got, err := p.stream().TakeWhile(func(v int) (bool, error) {
		visited = append(visited, v)
		return isEven(v), nil
	}).Collect()
	if err != nil || !slices.Equal(got, []int{2, 4}) {
		t.Fatalf("got %v, %v", got, err)
	}
	if !slices.Equal(visited, []int{2, 4, 5}) || p.reads != 3 || p.closed != 1 {
		t.Fatalf("visited=%v reads=%d closed=%d", visited, p.reads, p.closed)
	}
}

func TestTakeWhileError(t *testing.T) {
	vals, errs := drain(Of(1, 2, 3).TakeWhile(func(v int) (bool, error) {
		if v == 2 {
			return true, errX
		}
		return true, nil
	}))
	if !slices.Equal(vals, []int{1}) || len(errs) != 1 || !errors.Is(errs[0], errX) {
		t.Fatalf("vals=%v errs=%v", vals, errs)
	}
}
