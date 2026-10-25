package stream

import (
	"errors"
	"slices"
	"testing"
)

func TestOnErrorResume(t *testing.T) {
	src := newProbe(event{v: 1}, event{v: 2}, event{err: errX}, event{v: 3})
	var got []error
	s := src.stream().OnErrorResume(func(err error) Stream[int] {
		got = append(got, err)
		return Of(10, 11)
	})
	vals, errs := drain(s)
	if !slices.Equal(vals, []int{1, 2, 10, 11}) || len(errs) != 0 {
		t.Fatalf("vals=%v errs=%v; original error must not be re-emitted", vals, errs)
	}
	if len(got) != 1 || !errors.Is(got[0], errX) {
		t.Fatalf("fallback calls = %v", got)
	}
	if src.reads != 3 || src.closed != 1 {
		t.Fatalf("original upstream resumed: reads=%d closed=%d", src.reads, src.closed)
	}
}

func TestOnErrorResumeNotCalledWithoutError(t *testing.T) {
	calls := 0
	fallback := func(error) Stream[int] { calls++; return Of(0) }

	got, _ := Of(1, 2).OnErrorResume(fallback).Collect()
	if !slices.Equal(got, []int{1, 2}) || calls != 0 {
		t.Fatalf("got %v calls=%d", got, calls)
	}
	// Stopping before the error is reached must not invoke the fallback.
	p := newProbe(event{v: 1}, event{err: errX})
	if v, _, _ := p.stream().OnErrorResume(fallback).First(); v != 1 || calls != 0 {
		t.Fatalf("v=%d calls=%d", v, calls)
	}
}

func TestOnErrorResumeReplacementError(t *testing.T) {
	replacement := newProbe(event{v: 10}, event{err: errY}, event{v: 11})
	vals, errs := drain(newProbe(event{err: errX}).stream().OnErrorResume(func(error) Stream[int] {
		return replacement.stream()
	}))
	if !slices.Equal(vals, []int{10}) || len(errs) != 1 || !errors.Is(errs[0], errY) {
		t.Fatalf("vals=%v errs=%v", vals, errs)
	}
	if replacement.reads != 2 {
		t.Fatalf("replacement reads = %d", replacement.reads)
	}
}

func TestOnErrorResumeEarlyStopInReplacement(t *testing.T) {
	replacement := newProbe(ok(10, 11, 12)...)
	s := newProbe(event{err: errX}).stream().OnErrorResume(func(error) Stream[int] { return replacement.stream() })
	got, _ := s.Limit(2).Collect()
	if !slices.Equal(got, []int{10, 11}) || replacement.reads != 2 || replacement.closed != 1 {
		t.Fatalf("got %v reads=%d closed=%d", got, replacement.reads, replacement.closed)
	}
}
