# Amazon QLDB Go Driver

This is the Go driver for [Amazon Quantum Ledger Database (QLDB)](https://aws.amazon.com/qldb/), which allows Golang developers 
to write software that makes use of Amazon QLDB.

For getting started with the driver, see [Go and Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started.go.html).

## Requirements

### Basic Configuration

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html) for information on connecting to AWS.

### Required Golang versions

qldbdriver 0.1.0 requires Golang 1.14 or later.

Please see the link below for more detail to install Golang:

[Golang Download](https://golang.org/dl/)

## Getting Started

Please see the [Quickstart guide for the Amazon QLDB Driver for Go](TODO).

First, install the driver using go get:

```go get github.com/awslabs/amazon-qldb-driver-go```

For more instructions on working with the golang driver, please refer to the instructions below.

### See Also

1. [Getting Started with Amazon QLDB Go Driver](TODO) A guide that gets you started with executing transactions with the QLDB Go driver.
2. QLDB Golang driver accepts and returns [Amazon ION](http://amzn.github.io/ion-docs/) Documents. Amazon Ion is a richly-typed, self-describing, hierarchical data serialization format offering interchangeable binary and text representations. For more information read the [ION docs](https://readthedocs.org/projects/ion-python/).
3. Amazon QLDB supports a subset of the [PartiQL](https://partiql.org/) query language. PartiQL provides SQL-compatible query access across multiple data stores containing structured data, semistructured data, and nested data. For more information read the [docs](https://docs.aws.amazon.com/qldb/latest/developerguide/ql-reference.html).
4. Refer the section [Common Errors while using the Amazon QLDB Drivers](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-errors.html) which describes runtime errors that can be thrown by the Amazon QLDB Driver when calling the qldb-session APIs.

## Development

### Setup
Assuming that you have Golang installed, use the below command to clone the driver repository.

```
$ git clone https://github.com/awslabs/amazon-qldb-driver-go.git
$ cd amazon-qldb-driver-go
```
Changes can now be made in the repository.
### Running Tests

All the tests can be run by running the command:
```
go test -v ./...
```

The unit tests can be run by moving to the qldbdriver folder and then using the command:

```
$ go test -v
```

Similarly, the integration tests can be run by moving to the integration folder and then using the above command.
Please make sure to setup and configure an AWS account to run the integration tests.
## Getting Help

Please use these community resources for getting help.
* Ask a question on StackOverflow and tag it with the [amazon-qldb](https://stackoverflow.com/questions/tagged/amazon-qldb) tag.
* Open a support ticket with [AWS Support](http://docs.aws.amazon.com/awssupport/latest/user/getting-started.html).
* If you think you may have found a bug, please open an [issue](https://github.com/awslabs/amazon-qldb-driver-go/issues/new).

## Opening Issues

If you encounter a bug with the Amazon QLDB Go Driver, we would like to hear about it. Please search the [existing issues](https://github.com/awslabs/amazon-qldb-driver-go/issues) and see if others are also experiencing the issue before opening a new issue. When opening a new issue, we will need the version of Amazon QLDB Go Driver, Go language version, and OS you’re using. Please also include reproduction case for the issue when appropriate.

The GitHub issues are intended for bug reports and feature requests. For help and questions with using AWS SDK for GO please make use of the resources listed in the [Getting Help](https://github.com/awslabs/amazon-qldb-driver-go#getting-help) section. Keeping the list of open issues lean will help us respond in a timely manner.

## Documentation 

The Amazon QLDB Go Driver adheres to GoDoc standards and the documentation can be found at:

```
TODO: Attach Link
```

If you wish to use GoDoc locally, please refer the [GoDoc documetation](https://godoc.org/golang.org/x/tools/cmd/godoc)
## License

This library is licensed under the Apache 2.0 License.
