# 1.0.0 (2020-11-20)

The release candidate (v1.0.0-rc.1) has been selected as a final release of v1.0.0.

## :boom: Breaking changes

* `QLDBDriverError` struct is no longer exported and has been updated to `qldbDriverError`.

# 1.0.0-rc.1 (2020-11-06)

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
