MonetDB-Go
==========

MonetDB driver for Go.


## Installation

To install the `monetdb` package to your `$GOPATH`, simply use
the `go` tool. Make sure you have [Git](http://git-scm.com/downloads) installed.

```bash
$ go get github.com/MonetDB/MonetDB-Go@v2.0.1
```

## Usage

This Go MonetDB driver implements Go's
[`database/sql/driver`](http://golang.org/pkg/database/sql/driver/) interface.
Once you import it, you can use the standard Go database API to access MonetDB.

```go
import (
	"database/sql"
	_ "github.com/MonetDB/MonetDB-Go/v2"
)
```

Then use `monetdb` as the driver name and Data Source Name (DSN) as specified
in the next section.

```go
db, err := sql.Open("monetdb", "username:password@hostname:50000/database")
```

## Data Source Name (DSN)

The format of the DSN is the following

```
[username[:password]@]hostname[:port]/database
```


If the `port` is blank, then the default port `50000` will be used.

## API Documentation

https://pkg.go.dev/github.com/MonetDB/MonetDB-Go

## Testing

To run all short tests, go to the directory where the codes is checked out and run the following command:
```bash
go test -test.short ./...
```
These short tests don't require a running MonetDB server. When you want to run all the test, you need a running MonetDB server on the machine where you run the tests. You can start a docker container with the following command:
```bash
docker run --rm -p 50000:50000 -e MDB_DB_ADMIN_PASS=monetdb monetdb/monetdb
```
Another option is to use docker compose. Create a docker compose file like this:
```yaml
version: '3.0'

services:
  monetdb:
    image: monetdb/monetdb
    environment:
    - MDB_DB_ADMIN_PASS=monetdb
    ports:
    - 50000:50000/tcp
```
If you want to use a different MonetDB instance, you need to change the DSN in the tests to connect to it. Then you can run the tests with the following command, use the "-v" option for verbose output. 
```bash
go test ./...
```
if you only want to run the integration tests, you can add another parameter to the test command. The parameter matches the name of the test function, not the filename of the source file:
```bash
go test -v -run Integration ./...
```
The go testing framework can generate a code coverage report using the following commands:
```bash
go test ./... -coverprofile=cover.out
go tool cover -html=cover.out -o cover.html
```
The [go coverage action](https://github.com/gwatts/go-coverage-action) cannot be run in github actions, because of permission problems.
