package stream

import (
	"cmp"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

var (
	sinkInt   int
	sinkInts  []int
	sinkBytes [][64]byte
	sinkErr   error
)

func ints(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

func randomInts(n, cardinality int) []int {
	r := rand.New(rand.NewPCG(1, 2))
	out := make([]int, n)
	for i := range out {
		out[i] = r.IntN(cardinality)
	}
	return out
}

var sizes = []int{100, 10_000, 1_000_000}

// Each benchmark reports elements/s so the stream and direct-loop variants
// can be compared as throughput ratios.
func reportItems(b *testing.B, n int) {
	b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "items/s")
}

func BenchmarkFilterMapCount(b *testing.B) {
	for _, n := range sizes {
		in := ints(n)
		b.Run(fmt.Sprintf("stream/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sinkInt, sinkErr = From(in).
					FilterValue(isEven).
					MapValue(func(v int) int { return v * 3 }).
					Count()
			}
			reportItems(b, n)
		})
		b.Run(fmt.Sprintf("loop/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				c := 0
				for _, v := range in {
					if isEven(v) {
						sinkInt = v * 3
						c++
					}
				}
				sinkInt = c
			}
			reportItems(b, n)
		})
	}
}

func BenchmarkFilterMapCollect(b *testing.B) {
	const n = 10_000
	in := ints(n)
	b.Run("int/stream", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sinkInts, sinkErr = From(in).FilterValue(isEven).MapValue(func(v int) int { return v * 3 }).Collect()
		}
		reportItems(b, n)
	})
	b.Run("int/loop", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var out []int
			for _, v := range in {
				if isEven(v) {
					out = append(out, v*3)
				}
			}
			sinkInts = out
		}
		reportItems(b, n)
	})
	toBlock := func(v int) [64]byte { return [64]byte{byte(v)} }
	b.Run("64B/stream", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sinkBytes, sinkErr = From(in).FilterValue(isEven).MapValue(toBlock).Collect()
		}
		reportItems(b, n)
	})
	b.Run("64B/loop", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var out [][64]byte
			for _, v := range in {
				if isEven(v) {
					out = append(out, toBlock(v))
				}
			}
			sinkBytes = out
		}
		reportItems(b, n)
	})
}

func BenchmarkLimitEarlyTermination(b *testing.B) {
	in := ints(1_000_000)
	b.ReportAllocs()
	for b.Loop() {
		sinkInts, sinkErr = From(in).Limit(10).Collect()
	}
	if len(sinkInts) != 10 {
		b.Fatal(len(sinkInts))
	}
}

func BenchmarkDistinctBy(b *testing.B) {
	const n = 100_000
	for _, card := range []int{16, n} {
		in := randomInts(n, card)
		b.Run(fmt.Sprintf("cardinality=%d", card), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sinkInt, sinkErr = From(in).DistinctBy(func(v int) int { return v }).Count()
			}
			reportItems(b, n)
		})
	}
}

func BenchmarkSorted(b *testing.B) {
	const n = 100_000
	for name, in := range map[string][]int{"random": randomInts(n, n), "presorted": ints(n)} {
		b.Run(name+"/stream", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sinkInt, sinkErr = From(in).Sorted(cmp.Compare[int]).Count()
			}
			reportItems(b, n)
		})
		b.Run(name+"/loop", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				buf := slices.Clone(in)
				slices.SortFunc(buf, cmp.Compare[int])
				sinkInt = len(buf)
			}
			reportItems(b, n)
		})
	}
}

func BenchmarkFlatMap(b *testing.B) {
	const total = 100_000
	for _, child := range []int{4, 10_000} {
		outer := ints(total / child)
		inner := ints(child)
		b.Run(fmt.Sprintf("child=%d", child), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sinkInt, sinkErr = From(outer).FlatMap(func(int) Stream[int] { return From(inner) }).Count()
			}
			reportItems(b, total)
		})
	}
}

func BenchmarkEarlyError(b *testing.B) {
	const n = 100_000
	in := ints(n)
	errBench := errors.New("bench")
	for name, at := range map[string]int{"first": 0, "middle": n / 2, "last": n - 1} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sinkInts, sinkErr = From(in).Map(func(v int) (int, error) {
					if v == at {
						return 0, errBench
					}
					return v, nil
				}).Collect()
			}
		})
	}
}
