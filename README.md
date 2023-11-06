# Amazon QLDB Go Driver

[![license](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/awslabs/amazon-qldb-driver-go/blob/master/LICENSE)
[![AWS Provider](https://img.shields.io/badge/provider-AWS-orange?logo=amazon-aws&color=ff9900)](https://aws.amazon.com/qldb/)

[![Go Build](https://github.com/awslabs/amazon-qldb-driver-go/actions/workflows/go.yml/badge.svg)](https://github.com/awslabs/amazon-qldb-driver-go/actions/workflows/go.yml)
[![CodeQL](https://github.com/awslabs/amazon-qldb-driver-go/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/awslabs/amazon-qldb-driver-go/actions/workflows/codeql-analysis.yml)

This is the Go driver for [Amazon Quantum Ledger Database (QLDB)](https://aws.amazon.com/qldb/), which allows Golang developers 
to write software that makes use of Amazon QLDB.

For getting started with the driver, see [Go and Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started.golang.html).

## Requirements

### Basic Configuration

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html) for information on connecting to AWS.

### Required Golang versions

QldbDriver requires Golang 1.20 or later.

Please see the link below for more detail to install Golang:

* [Golang Download](https://golang.org/dl/)

## Getting Started

Please see the [Quickstart guide for the Amazon QLDB Driver for Go](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-quickstart-golang.html).

First, ensure your project is using [Go modules](https://blog.golang.org/using-go-modules) to install dependencies in your project.

In your project directory, install the driver using go get:

```go get github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver```

For more instructions on working with the golang driver, please refer to the instructions below.

### See Also

1. [Getting Started with Amazon QLDB Go Driver](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started.golang.html) A guide that gets you started with executing transactions with the QLDB Go driver.
2. For a quick start on how to interact with the driver, please refer to [Go Driver Quick Start](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-quickstart-golang.html).
3. [QLDB Go Driver Cookbook](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-cookbook-golang.html) The cookbook provides code samples for some simple QLDB Go driver use cases. 
4. QLDB Golang driver accepts and returns [Amazon ION](http://amzn.github.io/ion-docs/) Documents. Amazon Ion is a richly-typed, self-describing, hierarchical data serialization format offering interchangeable binary and text representations. For more information read the [ION docs](https://readthedocs.org/projects/ion-python/).
5. Amazon QLDB supports a subset of the [PartiQL](https://partiql.org/) query language. PartiQL provides SQL-compatible query access across multiple data stores containing structured data, semistructured data, and nested data. For more information read the [docs](https://docs.aws.amazon.com/qldb/latest/developerguide/ql-reference.html).
6. Refer the section [Common Errors while using the Amazon QLDB Drivers](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-errors.html) which describes runtime errors that can be thrown by the Amazon QLDB Driver when calling the qldb-session APIs.
7. **Driver Recommendations** &mdash; Check them out in the [Best Practices](https://docs.aws.amazon.com/qldb/latest/developerguide/driver.best-practices.html) 
in the QLDB documentation.
8. **Exception handling when using QLDB Drivers** &mdash; Refer to the section [Common Errors while using the Amazon 
QLDB Drivers](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-errors.html) 
which describes runtime exceptions that can be thrown by the Amazon QLDB Driver when calling the qldb-session APIs.

## Development

### Setup
Assuming that you have Golang installed, use the below command to clone the driver repository.

```
$ git clone https://github.com/awslabs/amazon-qldb-driver-go.git
$ cd amazon-qldb-driver-go
```
Changes can now be made in the repository.
### Running Tests

All the tests can be run by running the following command in the qldbdriver folder. Please make sure to setup and configure an AWS account to run the integration tests.
```
go test -v
```

To only run the unit tests:

```
go test -v -short
```

To only run the integration tests:

```
go test -run Integration
```

### Documentation 

The Amazon QLDB Go Driver adheres to GoDoc standards and the documentation can be found [here](https://pkg.go.dev/github.com/awslabs/amazon-qldb-driver-go/qldbdriver?tab=doc).

You can generate the docstring HTML locally by running the following in the root directory of this repository:
```godoc -http=:6060```

## Getting Help

Please use these community resources for getting help.
* Ask a question on StackOverflow and tag it with the [amazon-qldb](https://stackoverflow.com/questions/tagged/amazon-qldb) tag.
* Open a support ticket with [AWS Support](http://docs.aws.amazon.com/awssupport/latest/user/getting-started.html).
* Make a new thread at [AWS QLDB Forum](https://forums.aws.amazon.com/forum.jspa?forumID=353&start=0).
* If you think you may have found a bug, please open an [issue](https://github.com/awslabs/amazon-qldb-driver-go/issues/new).

## Opening Issues

If you encounter a bug with the Amazon QLDB Go Driver, we would like to hear about it. Please search the [existing issues](https://github.com/awslabs/amazon-qldb-driver-go/issues) and see if others are also experiencing the issue before opening a new issue. When opening a new issue, we will need the version of Amazon QLDB Go Driver, Go language version, and OS you’re using. Please also include reproduction case for the issue when appropriate.

The GitHub issues are intended for bug reports and feature requests. For help and questions with using AWS QLDB GO Driver please make use of the resources listed in the [Getting Help](https://github.com/awslabs/amazon-qldb-driver-go#getting-help) section. Keeping the list of open issues lean will help us respond in a timely manner.

## License

This library is licensed under the Apache 2.0 License.
