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
	"github.com/amzn/ion-go/ion"
	"github.com/aws/aws-sdk-go/service/qldbsession"
)

type DriverOptions struct {
	_               struct{}
	RetryLimit      uint8
	PoolLimit       uint64
	Logger          Logger
	LoggerVerbosity LogLevel
}

type QLDBDriver struct {
	ledgerName  string
	qldbSession *qldbsession.QLDBSession
	// Todo: New retry protocol
	retryLimit uint8
	poolLimit  uint64
	logger     *qldbLogger
}

func New(ledgerName string, qldbSession *qldbsession.QLDBSession, fns ...func(*DriverOptions)) *QLDBDriver {
	// Todo: Change default verbosity to LogInfo
	options := &DriverOptions{RetryLimit: 4, PoolLimit: 10, Logger: defaultLogger{}, LoggerVerbosity: LogDebug}
	for _, fn := range fns {
		fn(options)
	}
	logger := &qldbLogger{options.Logger, options.LoggerVerbosity}
	// Todo: Verify options are valid after fn
	return &QLDBDriver{ledgerName, qldbSession, options.RetryLimit, options.PoolLimit, logger}
}

func (driver *QLDBDriver) Execute(ctx context.Context, fn func(txn Transaction) (interface{}, error)) (interface{}, error) {
	session, err := driver.getSession(ctx)
	if err != nil {
		return nil, err
	}
	defer session.endSession(ctx)
	return session.execute(ctx, fn)
}

func (driver *QLDBDriver) GetTableNames(ctx context.Context) ([]string, error) {
	const tableNameQuery string = "SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'"
	type tableName struct {
		Name string `json:"name"`
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

func (driver *QLDBDriver) Close(ctx context.Context) {
	panic("not yet implemented")
}

func (driver *QLDBDriver) getSession(ctx context.Context) (*session, error) {
	communicator, err := startSession(ctx, driver.ledgerName, driver.qldbSession, driver.logger)
	if err != nil {
		return nil, err
	}
	return &session{communicator, driver.retryLimit, driver.logger}, nil
}
