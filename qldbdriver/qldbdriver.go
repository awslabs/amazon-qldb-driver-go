/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
the License. A copy of the License is located at

http://www.apache.org/licenses/LICENSE-2.0

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
and limitations under the License.
*/

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

const poolTimeoutDefault = time.Duration(10) * time.Second

//DriverOptions can be used to configure the driver during construction
type DriverOptions struct {
	_                         struct{}
	RetryLimit                uint8
	MaxConcurrentTransactions uint16
	Logger                    Logger
	LoggerVerbosity           LogLevel
}

//QLDBDriver is used to connect and execute statements and lambdas on QLDB
type QLDBDriver struct {
	ledgerName  string
	qldbSession qldbsessioniface.QLDBSessionAPI
	// Todo: New retry protocol
	retryLimit                uint8
	maxConcurrentTransactions uint16
	logger                    *qldbLogger
	isClosed                  bool
	semaphore                 *sync2.Semaphore
	sessionPool               chan *session
}

//New creates a QLDBDriver and verifies the options
func New(ledgerName string, qldbSession *qldbsession.QLDBSession, fns ...func(*DriverOptions)) *QLDBDriver {
	// Todo: Change default verbosity to LogInfo
	options := &DriverOptions{RetryLimit: 4, MaxConcurrentTransactions: 50, Logger: defaultLogger{}, LoggerVerbosity: LogDebug}
	for _, fn := range fns {
		fn(options)
	}
	// Todo: Verify options are valid after fn
	if options.MaxConcurrentTransactions < 1 {
		panic("MaxConcurrentTransactions must be 1 or greater.")
	}

	logger := &qldbLogger{options.Logger, options.LoggerVerbosity}
	qldbSDKRetries := 0
	qldbSession.Client.Config.MaxRetries = &qldbSDKRetries

	semaphore := sync2.NewSemaphore(int(options.MaxConcurrentTransactions), poolTimeoutDefault)
	sessionPool := make(chan *session, options.MaxConcurrentTransactions)
	isClosed := false

	return &QLDBDriver{ledgerName, qldbSession, options.RetryLimit, options.MaxConcurrentTransactions, logger, isClosed,
		semaphore, sessionPool}
}

//Execute executes the provided function
func (driver *QLDBDriver) Execute(ctx context.Context, fn func(txn Transaction) (interface{}, error)) (interface{}, error) {
	if driver.isClosed {
		panic("Cannot invoke methods on a closed QLDBDriver.")
	}
	session, err := driver.getSession(ctx)
	if err != nil {
		return nil, err
	}
	defer driver.releaseSession(session)
	return session.execute(ctx, fn)
}

//GetTableNames returns the list of Active tables for the ledger
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

//Close closes the driver from usage.
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
	isPermitAcquired := driver.semaphore.Acquire()
	if isPermitAcquired {
		defer func(driver *QLDBDriver) {
			if r := recover(); r != nil {
				driver.logger.log(fmt.Sprint("Encountered panic with message ", r), LogDebug)
				driver.semaphore.Release()
				panic(r)
			}
		}(driver)
		for len(driver.sessionPool) > 0 {
			session := <-driver.sessionPool
			_, err := session.communicator.abortTransaction(ctx)
			if err != nil {
				driver.logger.log("Reusing session from pool", LogDebug)
				return session, nil
			}
			driver.logger.log("Inactive session discarded", LogDebug)
		}
		return createSession(ctx, driver)
	}
	return nil, &QLDBDriverError{"MaxConcurrentTransactions limit exceeded"}
}

func createSession(ctx context.Context, driver *QLDBDriver) (*session, error) {
	driver.logger.log("Creating a new session", LogDebug)
	communicator, err := startSession(ctx, driver.ledgerName, driver.qldbSession, driver.logger)
	if err != nil {
		driver.semaphore.Release()
		return nil, err
	}
	return &session{communicator, driver.retryLimit, driver.logger}, nil
}

func (driver *QLDBDriver) releaseSession(session *session) {
	driver.sessionPool <- session
	driver.semaphore.Release()
	driver.logger.log(fmt.Sprint("Session returned to pool; size of pool is now ", len(driver.sessionPool)), LogDebug)
}
