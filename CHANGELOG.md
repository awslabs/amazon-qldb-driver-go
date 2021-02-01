# 1.1.0

## :tada: Enhancements

* Added `IOUsage` and `TimingInformation` structs to provide server-side execution statistics
    * `IOUsage` provides `GetReadIOs` method
    * `TimingInformation` provides `GetProcessingTimeMilliseconds` method
    * Added `GetConsumedIOs` and `GetTimingInformation` methods in `BufferedResult` and `Result` structs
    * `GetConsumedIOs` and `GetTimingInformation` methods are stateful, meaning the statistics returned by them reflect the state at the time of method execution.

# 1.0.1 (2020-10-27)

* Fixed the dates in this CHANGELOG.md file
* Updated the driver version in the user agent string to 1.0.1.

# 1.0.0 (2020-10-20)

The release candidate (v1.0.0-rc.1) has been selected as a final release of v1.0.0 with the following change:

## :boom: Breaking changes

* `QLDBDriverError` struct is no longer exported and has been updated to `qldbDriverError`.

# 1.0.0-rc.1 (2020-10-06)

## :tada: Enhancements

* Improved the iterator pattern for `Result.Next`. More details can be found in the [release notes](https://github.com/awslabs/amazon-qldb-driver-go/releases/tag/v1.0.0-rc.1)
* Removed panics in the driver. We can handle more errors gracefully now.
  
## :boom: Breaking changes

* Updated `QldbDriver.New` function to return `(QLDBDriver, error)`.
* Renamed `QldbDriver.Close` function to `QldbDriver.Shutdown`.
* Removed `QLDBDriver.ExecuteWithRetryPolicy` function.
* Removed `RetryPolicyContext` struct. `BackoffStrategy.Delay` function now takes in an `int` parameter as retry attempt.
* The `SleepBase` and `SleepCap` fields in struct type `ExponentialBackoffStrategy` have been updated to type `time.Duration`.

# 0.1.0 (2020-08-06)

* Preview release of the driver.
