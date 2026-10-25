package stream

import (
	"errors"
	"slices"
	"strconv"
	"testing"
)

func TestCollectDropsPartialOnError(t *testing.T) {
	got, err := newProbe(event{v: 1}, event{v: 2}, event{err: errX}).stream().Collect()
	if got != nil || !errors.Is(err, errX) {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestCount(t *testing.T) {
	if n, err := Of(1, 2, 3).Count(); n != 3 || err != nil {
		t.Fatalf("got %d, %v", n, err)
	}
	if n, err := newProbe(event{v: 1}, event{err: errX}).stream().Count(); n != 0 || !errors.Is(err, errX) {
		t.Fatalf("got %d, %v", n, err)
	}
}

func TestFirst(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		v, ok, err := Of[int]().First()
		if v != 0 || ok || err != nil {
			t.Fatalf("got %v %v %v", v, ok, err)
		}
	})
	t.Run("success", func(t *testing.T) {
		p := newProbe(ok(7, 8, 9)...)
		v, ok, err := p.stream().First()
		if v != 7 || !ok || err != nil {
			t.Fatalf("got %v %v %v", v, ok, err)
		}
		if p.reads != 1 || p.closed != 1 {
			t.Fatalf("reads=%d closed=%d", p.reads, p.closed)
		}
	})
	t.Run("error", func(t *testing.T) {
		v, ok, err := newProbe(event{v: 5, err: errX}, event{v: 6}).stream().First()
		if v != 0 || ok || !errors.Is(err, errX) {
			t.Fatalf("got %v %v %v", v, ok, err)
		}
	})
}

func TestForEach(t *testing.T) {
	p := newProbe(ok(1, 2, 3)...)
	var seen []int
	err := p.stream().ForEach(func(v int) error {
		seen = append(seen, v)
		if v == 2 {
			return errX
		}
		return nil
	})
	if !errors.Is(err, errX) || !slices.Equal(seen, []int{1, 2}) || p.reads != 2 {
		t.Fatalf("err=%v seen=%v reads=%d", err, seen, p.reads)
	}
	if err := Of(1).ForEach(func(int) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := newProbe(event{err: errY}).stream().ForEach(func(int) error { return nil }); !errors.Is(err, errY) {
		t.Fatalf("upstream error: got %v", err)
	}
}

func TestReduceTypeChanging(t *testing.T) {
	got, err := Of(1, 2, 3).Reduce("", func(acc string, v int) (string, error) {
		return acc + strconv.Itoa(v), nil
	})
	if got != "123" || err != nil {
		t.Fatalf("got %q, %v", got, err)
	}
	if got, err := Of[int]().Reduce(42, func(a, v int) (int, error) { return a + v, nil }); got != 42 || err != nil {
		t.Fatalf("empty: got %v, %v", got, err)
	}
}

func TestReduceErrorsReturnZero(t *testing.T) {
	got, err := Of(1, 2, 3).Reduce("seed", func(acc string, v int) (string, error) {
		if v == 2 {
			return "partial", errX
		}
		return acc + "!", nil
	})
	if got != "" || !errors.Is(err, errX) {
		t.Fatalf("callback error: got %q, %v", got, err)
	}
	got, err = newProbe(event{v: 1}, event{err: errY}).stream().Reduce("seed", func(acc string, _ int) (string, error) {
		return acc + "!", nil
	})
	if got != "" || !errors.Is(err, errY) {
		t.Fatalf("upstream error: got %q, %v", got, err)
	}
}

func TestGroupBy(t *testing.T) {
	got, err := Of("apple", "bob", "avocado", "banana", "cherry").GroupBy(func(s string) byte { return s[0] })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || !slices.Equal(got['a'], []string{"apple", "avocado"}) ||
		!slices.Equal(got['b'], []string{"bob", "banana"}) || !slices.Equal(got['c'], []string{"cherry"}) {
		t.Fatalf("got %v", got)
	}
	if m, err := newProbe(event{v: 1}, event{err: errX}).stream().GroupBy(func(v int) int { return v }); m != nil || !errors.Is(err, errX) {
		t.Fatalf("error: got %v, %v", m, err)
	}
}
