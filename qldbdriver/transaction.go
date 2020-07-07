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
	"reflect"
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
	commitHash   *qldbHash
}

func (txn *transaction) execute(ctx context.Context, statement string, parameters ...interface{}) (*Result, error) {
	executeHash, err := toQLDBHash(statement)
	if err != nil {
		return nil, err
	}
	txn.commitHash = txn.commitHash.dot(executeHash)

	// Todo: parameters
	executeResult, error := txn.communicator.executeStatement(ctx, &statement, txn.id)
	if error != nil {
		return nil, error
	}
	return &Result{ctx, txn.communicator, txn.id, executeResult.FirstPage.Values, executeResult.FirstPage.NextPageToken, 0, txn.logger}, nil
}

func (txn *transaction) commit(ctx context.Context) error {
	commitResult, err := txn.communicator.commitTransaction(ctx, txn.id, txn.commitHash.hash)
	if err != nil {
		return err
	}

	if !reflect.DeepEqual(commitResult.CommitDigest, txn.commitHash.hash) {
		return errors.New("Transaction's commit digest did not match returned value from QLDB. " +
			"Please retry with a new transaction. Transaction ID: " + *txn.id)
	}

	return nil
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
	_, _ = executor.txn.communicator.abortTransaction(executor.ctx)
	return errors.New("transaction aborted")
}
