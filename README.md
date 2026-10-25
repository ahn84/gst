# stream

Lazy, synchronous, error-aware generic streams for Go 1.27+.

```go
import stream "github.com/ahn84/gst"
```

A `Stream[T]` wraps an `iter.Seq2[T, error]`. Successful elements flow as
`(value, nil)`; a failure flows as `(zero, err)` and ends the stream. Pipelines
are lazy, run on the caller's goroutine, and stop upstream the moment the
consumer stops. The library uses only the standard library and creates no
goroutines or channels.

## Quick start

```go
names, err := stream.From(paths).
    Map(loadUser).                                       // func(string) (User, error)
    FilterValue(func(u User) bool { return u.Active }).
    MapValue(func(u User) string { return u.Name }).
    DistinctBy(func(name string) string { return name }).
    Sorted(cmp.Compare[string]).
    Limit(10).
    Collect()
if err != nil {
    return err // the first error from loadUser, or from the source
}
```

Type-changing operators (`Map`, `MapValue`, `FlatMap`, `FlatMapValue`,
`Reduce`, `GroupBy`, `DistinctBy`) are Go 1.27 generic methods. Go allows
generic methods only on concrete types, not in interfaces, so `Stream` is a
struct and there is no fluent stream interface.

Range over a stream directly with `Seq`:

```go
for n, err := range stream.Of("1", "2", "x").Map(strconv.Atoi).Seq() {
    if err != nil {
        return err
    }
    fmt.Println(n)
}
```

## API

| Kind | Operations |
| --- | --- |
| Constructors | `From(slice)`, `Of(values...)`, `FromSeq(iter.Seq[T])`, `FromSeq2(iter.Seq2[T, error])` |
| Transform | `Map`, `MapValue`, `FlatMap`, `FlatMapValue`, `Peek`, `PeekValue` |
| Select | `Filter`, `FilterValue`, `Limit`, `Skip`, `TakeWhile` |
| Stateful | `DistinctBy`, `Sorted` |
| Recover | `OnErrorResume` |
| Terminal | `Collect`, `Count`, `First`, `ForEach`, `Reduce`, `GroupBy`, or range over `Seq()` |

The `...Value` variants take callbacks that cannot fail.

## Errors are fail-fast

- The first error, whether from the source or from a callback, is delivered
  exactly once and ends the traversal. No callback runs and no source element
  is read after it.
- Errors are passed through unwrapped, so `errors.Is` and `errors.As` work.
- `FromSeq2` normalizes sources that misbehave by emitting more after an
  error: everything after the first error is ignored.
- Terminals do not return partial results on failure:

| Terminal | On error returns |
| --- | --- |
| `Collect` | `nil, err` |
| `Count` | `0, err` |
| `First` | `zero, false, err` |
| `ForEach` | `err` (effects of actions already run remain) |
| `Reduce` | `zero R, err`. If the seed is a pointer, map or slice, mutations made before the error remain visible through it. |
| `GroupBy` | `nil, err` |

Panics in callbacks or sources are not recovered.

`OnErrorResume(f)` replaces the rest of the stream with `f(err)` at the
first error. It does not resume the original source.

## Streams may be one-shot

A `Stream` is a recipe, not a container. Every terminal operation re-runs
the whole pipeline from the source, including `Peek` side effects.

- `From(slice)` can be traversed repeatedly. The slice is not copied, so
  changes to it are visible.
- Streams over cursors, channels, scanners or stateful generators are usually
  one-shot. A second traversal may yield nothing or fail.
- `DistinctBy` and `Sorted` keep fresh state for each traversal.
- Traversing concurrently is safe only when the source and every callback
  support it.

## Early termination and cleanup

Breaking out of a `range`, `First`, `Limit`, `TakeWhile` and errors all
return through every stage, so `defer` cleanup in your source iterator runs.
`Limit(n)` with `n <= 0` never starts the source. A source that ignores
`yield`'s `false` return breaks the Go iterator protocol, and the library
cannot stop it.

There is no built-in `context.Context` support. To cancel, have your source
iterator watch the context and yield `ctx.Err()`.

## Memory

| Operation | Memory per traversal |
| --- | --- |
| stateless operators | O(1) |
| `DistinctBy` | O(distinct keys) |
| `Sorted` | O(N), plus O(N log N) time. It reads the entire upstream before yielding anything, so it cannot handle infinite streams, and a later `Limit` saves no work. |
| `Collect`, `GroupBy` | O(N) |

## Development

Requires Go 1.27+. With `GOTOOLCHAIN=auto` (the default), an older `go`
downloads 1.27 automatically.

```sh
go vet ./...
go test -race -cover ./...
go test -run TestLimit ./...                       # a single test
go test -run '^$' -fuzz '^FuzzLimit$' -fuzztime 10s .
go test -run '^$' -bench . -benchmem ./...
```
