# Implementation

## Go sql library

The [go sql library](https://pkg.go.dev/database/sql) and the [go sql driver library](https://pkg.go.dev/database/sql/driver) 

## MonetDB driver

The current version, v1.1.0, implemented the Go v1.7 library interfaces. The latest version (go 1.21.5) has more functionality, specifically the column types and supporting the context interface.

### Mapi

[Mapi](https://www.monetdb.org/documentation-Jun2023/user-guide/client-interfaces/libraries-drivers/mapi-library/) is the API that provides the communication protocol with the MonetDB database. To improve the mapi protocol implementation in this library, check the documentation in the [PHP](https://github.com/MonetDB/MonetDB-PHP/tree/master/protocol_doc) driver.

### Refactoring

We could also use the new [builtin min function](https://pkg.go.dev/builtin#min)

We will create resultset, resultset schema and resultset metadata types. The code in the current implementation, for example the description type and the statement.storeResult function, will be moved to this go source file. We will move all monetdb specific code out of the implementation of the sql driver interfaces. With the resultset types implemented, it will be relatively easy to implement the columnt type interfaces. 

We will also move the code for handling the connection config out of the driver.go source file. We want to separate the MonetDB specific implementation details from the more generic sql library code as much as possible. This will make the it easier to understand the implementation and easier to implement new interfaces.

We will prefix the error messages with the package name. That way it is clear where the error message comes from, the driver implementation or the mapi library.

#### Todo
- [X] in driver.go move parsedsn function call inside newConn
- [X] move tests from driver_test.go to new file after change to driver.open
- [X] move config type from driver.go
- [X] Conn struct doesn't need a config field
- [ ] set_autocommit (see: [pymonetdb](https://github.com/MonetDB/pymonetdb/blob/master/pymonetdb/sql/connections.py#L156C16-L156C16))
- [ ] change_replysize
- [ ] set_timezone
- [ ] set_uploader
- [ ] set_downloader

### Logging

## driver package and sql package latest version

We need to add context everywhere. We need to implement the RowsColumnType interfaces. We can implement transaction isolation level.

### context library

The [go context library](https://pkg.go.dev/context)

### New interfaces

### Testing

The [go testing library](https://pkg.go.dev/testing)
