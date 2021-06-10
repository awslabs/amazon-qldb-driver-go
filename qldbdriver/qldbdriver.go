/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
the License. A copy of the License is located at

http://www.apache.org/licenses/LICENSE-2.0

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
and limitations under the License.
*/

// Package qldbdriver is the Golang driver for working with Amazon Quantum Ledger Database.
package qldbdriver

import (
	"context"
	"sync"
	"time"

	"github.com/amzn/ion-go/ion"
	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/aws/aws-sdk-go/service/qldbsession/qldbsessioniface"
)

// DriverOptions can be used to configure the driver during construction.
type DriverOptions struct {
	// The policy guiding retry attempts upon a recoverable error.
	// Default: MaxRetryLimit: 4, ExponentialBackoff: SleepBase: 10ms, SleepCap: 5000ms.
	RetryPolicy RetryPolicy
	// The maximum amount of concurrent transactions this driver will permit. Default: 50.
	MaxConcurrentTransactions int
	// The logger that the driver will use for any logging messages. Default: "log" package.
	Logger Logger
	// The verbosity level of the logs that the logger should receive. Default: qldbdriver.LogInfo.
	LoggerVerbosity LogLevel
}

// QLDBDriver is used to execute statements against QLDB. Call constructor qldbdriver.New for a valid QLDBDriver.
type QLDBDriver struct {
	ledgerName                string
	qldbSession               qldbsessioniface.QLDBSessionAPI
	maxConcurrentTransactions int
	logger                    *qldbLogger
	isClosed                  bool
	semaphore                 *semaphore
	sessionPool               chan *session
	retryPolicy               RetryPolicy
	lock                      sync.Mutex
}

type semaphore struct {
	values chan struct{}
}

// New creates a QLBDDriver using the parameters and options, and verifies the configuration.
//
// Note that qldbSession.Client.Config.MaxRetries will be set to 0. This property should not be modified.
// DriverOptions.RetryLimit is unrelated to this property, but should be used if it is desired to modify the amount of retires for statement executions.
func New(ledgerName string, qldbSession *qldbsession.QLDBSession, fns ...func(*DriverOptions)) (*QLDBDriver, error) {
	if qldbSession == nil {
		return nil, &qldbDriverError{"Provided QLDBSession is nil."}
	}

	retryPolicy := RetryPolicy{
		MaxRetryLimit: 4,
		Backoff:       ExponentialBackoffStrategy{SleepBase: time.Duration(10) * time.Millisecond, SleepCap: time.Duration(5000) * time.Millisecond}}
	options := &DriverOptions{RetryPolicy: retryPolicy, MaxConcurrentTransactions: 50, Logger: defaultLogger{}, LoggerVerbosity: LogInfo}

	for _, fn := range fns {
		fn(options)
	}
	if options.MaxConcurrentTransactions < 1 {
		return nil, &qldbDriverError{"MaxConcurrentTransactions must be 1 or greater."}
	}

	logger := &qldbLogger{options.Logger, options.LoggerVerbosity}

	driverQldbSession := *qldbSession
	if qldbSession.Client != nil {
		client := *qldbSession.Client

		qldbSDKRetries := 0
		client.Config.MaxRetries = &qldbSDKRetries

		driverQldbSession.Client = &client
	}

	semaphore := makeSemaphore(options.MaxConcurrentTransactions)
	sessionPool := make(chan *session, options.MaxConcurrentTransactions)
	isClosed := false

	return &QLDBDriver{ledgerName, &driverQldbSession, options.MaxConcurrentTransactions, logger, isClosed,
		semaphore, sessionPool, options.RetryPolicy, sync.Mutex{}}, nil
}

// SetRetryPolicy sets the driver's retry policy for Execute.
func (driver *QLDBDriver) SetRetryPolicy(rp RetryPolicy) {
	driver.retryPolicy = rp
}

// Execute a provided function within the context of a new QLDB transaction.
//
// The provided function might be executed more than once and is not expected to run concurrently.
// It is recommended for it to be idempotent, so that it doesn't have unintended side effects in the case of retries.
func (driver *QLDBDriver) Execute(ctx context.Context, fn func(txn Transaction) (interface{}, error)) (interface{}, error) {
	if driver.isClosed {
		return nil, &qldbDriverError{"Cannot invoke methods on a closed QLDBDriver."}
	}

	retryAttempt := 0

	session, err := driver.getSession(ctx)
	if err != nil {
		return nil, err
	}

	var result interface{}
	var txnErr *txnError
	for {
		result, txnErr = session.execute(ctx, fn)
		if txnErr != nil {
			// If initial session is invalid, always retry once
			if txnErr.canRetry && txnErr.isISE && retryAttempt == 0 {
				driver.logger.log(LogDebug, "Initial session received from pool invalid. Retrying...")
				session, err = driver.createSession(ctx)
				if err != nil {
					return nil, err
				}
				retryAttempt++
				continue
			}
			// Do not retry
			if !txnErr.canRetry || retryAttempt >= driver.retryPolicy.MaxRetryLimit {
				if txnErr.abortSuccess {
					driver.releaseSession(session)
				} else {
					driver.semaphore.release()
				}
				return nil, txnErr.unwrap()
			}
			// Retry
			retryAttempt++
			driver.logger.logf(LogInfo, "A recoverable error has occurred. Attempting retry #%d.", retryAttempt)
			driver.logger.logf(LogDebug, "Errored Transaction ID: %s. Error cause: '%v'", txnErr.transactionID, txnErr)
			if txnErr.isISE {
				driver.logger.log(LogDebug, "Replacing expired session...")
				session, err = driver.createSession(ctx)
				if err != nil {
					return nil, err
				}
			} else {
				if !txnErr.abortSuccess {
					driver.logger.log(LogDebug, "Retrying with a different session...")
					driver.semaphore.release()
					session, err = driver.getSession(ctx)
					if err != nil {
						return nil, err
					}
				}
			}

			delay := driver.retryPolicy.Backoff.Delay(retryAttempt)
			sleepWithContext(ctx, delay)
			continue
		}
		driver.releaseSession(session)
		break
	}
	return result, nil
}

// GetTableNames returns a list of the names of active tables in the ledger.
func (driver *QLDBDriver) GetTableNames(ctx context.Context) ([]string, error) {
	const tableNameQuery string = "SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'"
	type tableName struct {
		Name string `ion:"name"`
	}

	executeResult, err := driver.Execute(ctx, func(txn Transaction) (interface{}, error) {
		result, err := txn.Execute(tableNameQuery)
		if err != nil {
			return nil, err
		}
		tableNames := make([]string, 0)
		for result.Next(txn) {
			nameStruct := new(tableName)
			err = ion.Unmarshal(result.GetCurrentData(), &nameStruct)
			if err != nil {
				return nil, err
			}
			tableNames = append(tableNames, nameStruct.Name)
		}
		if result.Err() != nil {
			return nil, result.Err()
		}
		return tableNames, nil
	})
	if err != nil {
		return nil, err
	}
	return executeResult.([]string), nil
}

// Shutdown the driver, cleaning up allocated resources.
func (driver *QLDBDriver) Shutdown(ctx context.Context) {
	driver.lock.Lock()
	defer driver.lock.Unlock()
	if !driver.isClosed {
		driver.isClosed = true
		for len(driver.sessionPool) > 0 {
			session := <-driver.sessionPool
			err := session.endSession(ctx)
			if err != nil {
				driver.logger.logf(LogDebug, "Encountered error trying to end session: '%v'", err.Error())
			}
		}
		close(driver.sessionPool)
	}
}

func (driver *QLDBDriver) getSession(ctx context.Context) (*session, error) {
	driver.logger.logf(LogDebug, "Getting session. Existing sessions available: %v", len(driver.sessionPool))
	isPermitAcquired := driver.semaphore.tryAcquire()
	if isPermitAcquired {
		if len(driver.sessionPool) > 0 {
			session := <-driver.sessionPool
			driver.logger.log(LogDebug, "Reusing session from pool.")
			return session, nil
		}
		return driver.createSession(ctx)
	}
	return nil, &qldbDriverError{"MaxConcurrentTransactions limit exceeded."}
}

func (driver *QLDBDriver) createSession(ctx context.Context) (*session, error) {
	driver.logger.log(LogDebug, "Creating a new session")
	communicator, err := startSession(ctx, driver.ledgerName, driver.qldbSession, driver.logger)
	if err != nil {
		driver.semaphore.release()
		return nil, err
	}
	return &session{communicator, driver.logger}, nil
}

func (driver *QLDBDriver) releaseSession(session *session) {
	driver.sessionPool <- session
	driver.semaphore.release()
	driver.logger.logf(LogDebug, "Session returned to pool; size of pool is now %v", len(driver.sessionPool))
}

func sleepWithContext(ctx context.Context, delay time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}

func makeSemaphore(size int) *semaphore {
	smphr := &semaphore{make(chan struct{}, size)}
	for counter := 0; counter < size; counter++ {
		smphr.values <- struct{}{}
	}
	return smphr
}

func (smphr *semaphore) tryAcquire() bool {
	select {
	case _, ok := <-smphr.values:
		return ok
	default:
		return false
	}
}

func (smphr *semaphore) release() {
	smphr.values <- struct{}{}
}
