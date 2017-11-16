#/bin/sh
# Generate documentation in markdown from the package

go get github.com/robertkrimen/godocdown/godocdown

godocdown redisearch
