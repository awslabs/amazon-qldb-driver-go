The Amazon QLDB team is pleased to announce the release of v2.0.0 of the Go QLDB driver.

## Changes
* Bumped Ion Go dependency to `v1.1.3` allow support for unmarshalling Ion timestamps to Go time objects.
* Logging interface now provides the LogLevel as a parameter.
* Expose transaction ID in Transaction interface.

## :boom: Breaking changes

* The `Logger` interface's `Log` method now takes in a `LogLevel` to specify the logging verbosity. Any instances of `Logger.Log()` will need to be updated accordingly.

* `Result` and `BufferedResult` have changed from struct types to interface types. As a consequence of this change, the `Transaction` interface's `Execute()` and `BufferResult()` methods respectively return `Result` and `BufferedResult` rather than `*Result` and `*BufferedResult`. Any logic dereferencing a `Result` or `BufferedResult` will need to be updated accordingly.

* The `Transaction` interface has a new `ID()` method for exposing the transaction ID. Any implementations of this interface will need a new `ID() string` method defined.
