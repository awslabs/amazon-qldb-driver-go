/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
the License. A copy of the License is located at

http://www.apache.org/licenses/LICENSE-2.0

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
and limitations under the License.
*/

// The Golang driver for working with Amazon Quantum Ledger Database.
package qldbdriver

import (
	"context"
	"fmt"
	"time"

	"github.com/amzn/ion-go/ion"
	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/aws/aws-sdk-go/service/qldbsession/qldbsessioniface"
	"github.com/youtube/vitess/go/sync2"
)

// DriverOptions can be used to configure the driver during construction.
type DriverOptions struct {
	// The amount of times a transaction will automatically retry upon a recoverable error. Default: 4.
	RetryLimit uint8
	// The maximum amount of concurrent transactions this driver will permit. Default: 50.
	MaxConcurrentTransactions uint16
	// The logger that the driver will use for any logging messages. Default: "log" package.
	Logger Logger
	// The verbosity level of the logs that the logger should receive. Default: qldbdriver.LogInfo.
	LoggerVerbosity LogLevel
}

// QLDBDriver is used to execute statements against QLDB. Call constructor qldbdriver.New for a valid QLDBDriver.
type QLDBDriver struct {
	ledgerName                string
	qldbSession               qldbsessioniface.QLDBSessionAPI
	retryLimit                uint8
	maxConcurrentTransactions uint16
	logger                    *qldbLogger
	isClosed                  bool
	semaphore                 *sync2.Semaphore
	sessionPool               chan *session
}

// New creates a QLBDDriver using the parameters and options, and verifies the configuration.
//
// Note that qldbSession.Client.Config.MaxRetries will be set to 0. This property should not be modified.
// DriverOptions.RetryLimit is unrelated to this property, but should be used if it is desired to modify the amount of retires for statement executions.
func New(ledgerName string, qldbSession *qldbsession.QLDBSession, fns ...func(*DriverOptions)) *QLDBDriver {
	options := &DriverOptions{4, 50, defaultLogger{}, LogInfo}
	for _, fn := range fns {
		fn(options)
	}
	if options.MaxConcurrentTransactions < 1 {
		panic("MaxConcurrentTransactions must be 1 or greater.")
	}

	logger := &qldbLogger{options.Logger, options.LoggerVerbosity}
	qldbSDKRetries := 0
	qldbSession.Client.Config.MaxRetries = &qldbSDKRetries

	semaphore := sync2.NewSemaphore(int(options.MaxConcurrentTransactions), 0)
	sessionPool := make(chan *session, options.MaxConcurrentTransactions)
	isClosed := false

	return &QLDBDriver{ledgerName, qldbSession, options.RetryLimit, options.MaxConcurrentTransactions, logger, isClosed,
		semaphore, sessionPool}
}

// Execute a provided function within the context of a new QLDB transaction.
//
// The provided function might be executed more than once.
// It is recommended for it to be idempotent, so that it doesn't have unintended side effects in the case of retries.
func (driver *QLDBDriver) Execute(ctx context.Context, fn func(txn Transaction) (interface{}, error)) (interface{}, error) {
	return driver.ExecuteWithRetryPolicy(ctx, fn, RetryPolicy{MaxRetryLimit: 4, Backoff: ExponentialBackoffStrategy{SleepBaseInMillis: 10, SleepCapInMillis: 5000}})
}

// Execute a provided function within the context of a new QLDB transaction with a custom RetryPolicy.
//
// The provided function might be executed more than once.
// It is recommended for it to be idempotent, so that it doesn't have unintended side effects in the case of retries.
func (driver *QLDBDriver) ExecuteWithRetryPolicy(ctx context.Context, fn func(txn Transaction) (interface{}, error), retryPolicy RetryPolicy) (interface{}, error) {
	if driver.isClosed {
		panic("Cannot invoke methods on a closed QLDBDriver.")
	}

	retryAttempt := 0

	session, err := driver.getSession(ctx)
	if err != nil {
		return nil, err
	}

	for true {
		result, txnErr := session.execute(ctx, fn)
		if txnErr != nil {
			// If initial session is invalid, always retry once
			if txnErr.canRetry && txnErr.isISE && retryAttempt == 0 {
				driver.logger.log("Initial session received from pool invalid. Retrying...", LogDebug)
				session, err = driver.createSession(ctx)
				if err != nil {
					return nil, err
				}
				retryAttempt++
				continue
			}
			// Do not retry
			if !txnErr.canRetry || retryAttempt >= retryPolicy.MaxRetryLimit {
				if txnErr.abortSuccess {
					driver.releaseSession(session)
				} else {
					driver.semaphore.Release()
				}
				return nil, txnErr.unwrap()
			}
			// Retry
			retryAttempt++
			driver.logger.log(fmt.Sprintf("A recoverable error has occurred. Attempting retry #%d.", retryAttempt), LogInfo)
			driver.logger.log(fmt.Sprintf("Errored Transaction ID: %s. Error cause: %v", txnErr.transactionID, txnErr), LogDebug)
			if txnErr.isISE {
				driver.logger.log("Replacing expired session...", LogDebug)
				session, err = driver.createSession(ctx)
				if err != nil {
					return nil, err
				}
			} else {
				if !txnErr.abortSuccess {
					driver.logger.log("Retrying with a different session...", LogDebug)
					driver.semaphore.Release()
					session, err = driver.getSession(ctx)
					if err != nil {
						return nil, err
					}
				}
			}
			time.Sleep(retryPolicy.Backoff.Delay(RetryPolicyContext{RetryAttempt: retryAttempt, RetriedError: txnErr.unwrap()}))
			continue
		}
		driver.releaseSession(session)
		return result, nil
	}
	return nil, &QLDBDriverError{"Unexpected error encountered in Execute."}
}

// Return a list of the names of active tables in the ledger.
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
		for result.HasNext() {
			ionBinary, err := result.Next(txn)
			if err != nil {
				return nil, err
			}
			nameStruct := new(tableName)
			ionErr := ion.Unmarshal(ionBinary, &nameStruct)
			if ionErr != nil {
				return nil, ionErr
			}
			tableNames = append(tableNames, nameStruct.Name)
		}
		return tableNames, nil
	})
	if err != nil {
		return nil, err
	}
	return executeResult.([]string), nil
}

// Close the driver, cleaning up allocated resources.
func (driver *QLDBDriver) Close(ctx context.Context) {
	if driver.isClosed == false {
		driver.isClosed = true
		for len(driver.sessionPool) > 0 {
			session := <-driver.sessionPool
			err := session.endSession(ctx)
			if err != nil {
				driver.logger.log(fmt.Sprint("Encountered error trying to end session ", err), LogDebug)
			}
		}
		close(driver.sessionPool)
	}
}

func (driver *QLDBDriver) getSession(ctx context.Context) (*session, error) {
	driver.logger.log(fmt.Sprint("Getting session. Existing sessions available:", len(driver.sessionPool)), LogDebug)
	isPermitAcquired := driver.semaphore.TryAcquire()
	if isPermitAcquired {
		defer func(driver *QLDBDriver) {
			if r := recover(); r != nil {
				driver.logger.log(fmt.Sprint("Encountered panic with message ", r), LogDebug)
				driver.semaphore.Release()
				panic(r)
			}
		}(driver)
		if len(driver.sessionPool) > 0 {
			session := <-driver.sessionPool
			driver.logger.log("Reusing session from pool.", LogDebug)
			return session, nil
		}
		return driver.createSession(ctx)
	}
	return nil, &QLDBDriverError{"MaxConcurrentTransactions limit exceeded."}
}

func (driver *QLDBDriver) createSession(ctx context.Context) (*session, error) {
	driver.logger.log("Creating a new session", LogDebug)
	communicator, err := startSession(ctx, driver.ledgerName, driver.qldbSession, driver.logger)
	if err != nil {
		driver.semaphore.Release()
		return nil, err
	}
	return &session{communicator, driver.logger}, nil
}

func (driver *QLDBDriver) releaseSession(session *session) {
	driver.sessionPool <- session
	driver.semaphore.Release()
	driver.logger.log(fmt.Sprint("Session returned to pool; size of pool is now ", len(driver.sessionPool)), LogDebug)
}
