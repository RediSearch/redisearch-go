# question

When I use num filter with open interval, for example (1,49]
```golang
query := redisearch.NewQuery("*").AddFilter(
    redisearch.Filter{
        Field: "uid",
        Options: redisearch.NumericFilterOptions{
            Min:          1,
            ExclusiveMin: true,
            Max:          49,
            ExclusiveMax: false,
        },
    },
)
```
But I meet this error
> total: 0, err:Unknown argument `49` at position 5 for <main>

Then I debugged it，So I find the final command sent to redis server is
```redis
*8
$9
FT.SEARCH
$4
tags
$1
*
$6
FILTER
$3
uid
$1
(
$1
1
$2
49
```
So I think the error is here
```redis
$1
(
$1
1
```
This means that `( 1`

So I typed in the command line
> ft.search tags * filter uid ( 1 49

I meet the same problem
> Unknown argument `49` at position 5 for <main>

But if I take `(` and `1` togather  
> ft.search tags * filter uid (1 49

Oh,It's ok.


# solution