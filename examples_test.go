package stream_test

import (
	"cmp"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strconv"
	"strings"

	stream "github.com/ahn84/gst"
)

type User struct {
	Name   string
	Active bool
}

func loadUser(path string) (User, error) {
	if path == "missing" {
		return User{}, fmt.Errorf("missing user: %s", path)
	}
	return User{Name: path, Active: path != "carol"}, nil
}

func Example() {
	paths := []string{"bob", "alice", "carol", "bob"}

	names, err := stream.From(paths).
		Map(loadUser).
		FilterValue(func(u User) bool { return u.Active }).
		MapValue(func(u User) string { return u.Name }).
		DistinctBy(func(name string) string { return name }).
		Sorted(cmp.Compare[string]).
		Collect()
	fmt.Println(names, err)
	// Output: [alice bob] <nil>
}

func Example_failFast() {
	names, err := stream.Of("alice", "missing", "bob").
		Map(loadUser).
		MapValue(func(u User) string { return u.Name }).
		Collect()
	fmt.Println(names == nil, err)
	// Output: true missing user: missing
}

func ExampleStream_Seq() {
	for n, err := range stream.Of("1", "2", "x", "4").Map(strconv.Atoi).Seq() {
		if err != nil {
			fmt.Println("error:", err)
			break
		}
		fmt.Println(n)
	}
	// Output:
	// 1
	// 2
	// error: strconv.Atoi: parsing "x": invalid syntax
}

func ExampleFromSeq() {
	keys := stream.FromSeq(maps.Keys(map[string]int{"b": 2, "a": 1}))
	sorted, _ := keys.Sorted(strings.Compare).Collect()
	fmt.Println(sorted)
	// Output: [a b]
}

func ExampleFromSeq2() {
	errStop := errors.New("stop")
	src := func(yield func(int, error) bool) {
		_ = yield(1, nil) && yield(2, nil) && yield(0, errStop) && yield(3, nil)
	}
	for v, err := range stream.FromSeq2(src).Seq() {
		fmt.Println(v, err)
	}
	// Output:
	// 1 <nil>
	// 2 <nil>
	// 0 stop
}

func ExampleStream_FlatMapValue() {
	words, _ := stream.Of("a b", "c").
		FlatMapValue(func(line string) iter.Seq[string] { return strings.FieldsSeq(line) }).
		Collect()
	fmt.Println(words)
	// Output: [a b c]
}

func ExampleStream_Limit() {
	naturals := func(yield func(int) bool) {
		for i := 0; ; i++ {
			if !yield(i) {
				return
			}
		}
	}
	firstSquares, _ := stream.FromSeq(naturals).MapValue(func(n int) int { return n * n }).Limit(4).Collect()
	fmt.Println(firstSquares)
	// Output: [0 1 4 9]
}

func ExampleStream_Reduce() {
	csv, _ := stream.Of(1, 2, 3).Reduce("", func(acc string, n int) (string, error) {
		if acc != "" {
			acc += ","
		}
		return acc + strconv.Itoa(n), nil
	})
	fmt.Println(csv)
	// Output: 1,2,3
}

func ExampleStream_GroupBy() {
	byLen, _ := stream.Of("go", "rust", "c", "zig", "java").GroupBy(func(s string) int { return len(s) })
	for _, k := range slices.Sorted(maps.Keys(byLen)) {
		fmt.Println(k, byLen[k])
	}
	// Output:
	// 1 [c]
	// 2 [go]
	// 3 [zig]
	// 4 [rust java]
}

func ExampleStream_First() {
	v, ok, err := stream.Of(3, 8, 10).FilterValue(func(n int) bool { return n%2 == 0 }).First()
	fmt.Println(v, ok, err)
	// Output: 8 true <nil>
}

func ExampleStream_OnErrorResume() {
	vals, err := stream.Of("1", "x", "3").
		Map(strconv.Atoi).
		OnErrorResume(func(error) stream.Stream[int] { return stream.Of(-1) }).
		Collect()
	fmt.Println(vals, err)
	// Output: [1 -1] <nil>
}
