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
	"errors"
)

type Transaction interface {
	Execute(statement string, parameters ...interface{}) (*Result, error)
	BufferResult(result *Result) (*BufferedResult, error)
	Abort() error
}

type transaction struct {
	communicator *communicator
	id           *string
	logger       *qldbLogger
}

func (txn *transaction) execute(ctx context.Context, statement string, parameters ...interface{}) (*Result, error) {
	// Todo: parameters
	executeResult, error := txn.communicator.executeStatement(ctx, &statement, txn.id)
	if error != nil {
		return nil, error
	}
	return &Result{ctx, txn.communicator, txn.id, executeResult.FirstPage.Values, executeResult.FirstPage.NextPageToken, 0, txn.logger}, nil
}

func (txn *transaction) commit(ctx context.Context) error {
	panic("not yet implemented")
}

func (txn *transaction) abort(ctx context.Context) error {
	panic("not yet implemented")
}

type transactionExecutor struct {
	ctx context.Context
	txn *transaction
}

func (executor *transactionExecutor) Execute(statement string, parameters ...interface{}) (*Result, error) {
	return executor.txn.execute(executor.ctx, statement, parameters...)
}

func (executor *transactionExecutor) BufferResult(result *Result) (*BufferedResult, error) {
	bufferedResults := make([][]byte, 0)
	for result.HasNext() {
		ionBinary, err := result.Next(executor)
		if err != nil {
			return nil, err
		}
		bufferedResults = append(bufferedResults, ionBinary)
	}
	return &BufferedResult{bufferedResults, 0}, nil
}

func (executor *transactionExecutor) Abort() error {
	executor.txn.abort(executor.ctx)
	return errors.New("transaction aborted")
}
