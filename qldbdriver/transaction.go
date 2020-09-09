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

	"github.com/amzn/ion-go/ion"
	"github.com/aws/aws-sdk-go/service/qldbsession"
)

// Transaction represents an active QLDB transaction.
type Transaction interface {
	Execute(statement string, parameters ...interface{}) (*Result, error)
	BufferResult(result *Result) (*BufferedResult, error)
	Abort() error
}

type transaction struct {
	communicator qldbService
	id           *string
	logger       *qldbLogger
	commitHash   *qldbHash
}

func (txn *transaction) execute(ctx context.Context, statement string, parameters ...interface{}) (*Result, error) {
	executeHash, err := toQLDBHash(statement)
	if err != nil {
		return nil, err
	}
	valueHolders := make([]*qldbsession.ValueHolder, len(parameters))
	for i, parameter := range parameters {
		parameterHash, err := toQLDBHash(parameter)
		if err != nil {
			return nil, err
		}
		executeHash = executeHash.dot(parameterHash)
		// Can ignore error here since toQLDBHash calls MarshalBinary already
		ionBinary, _ := ion.MarshalBinary(parameter)
		valueHolder := qldbsession.ValueHolder{IonBinary: ionBinary}
		valueHolders[i] = &valueHolder
	}
	txn.commitHash = txn.commitHash.dot(executeHash)

	executeResult, err := txn.communicator.executeStatement(ctx, &statement, valueHolders, txn.id)
	if err != nil {
		return nil, err
	}
	return &Result{ctx, txn.communicator, txn.id, executeResult.FirstPage.Values, executeResult.FirstPage.NextPageToken, 0, txn.logger}, nil
}

func (txn *transaction) commit(ctx context.Context) error {
	commitResult, err := txn.communicator.commitTransaction(ctx, txn.id, txn.commitHash.hash)
	if err != nil {
		return err
	}

	if !reflect.DeepEqual(commitResult.CommitDigest, txn.commitHash.hash) {
		return &QLDBDriverError{
			errorMessage: "Transaction's commit digest did not match returned value from QLDB. Please retry with a new transaction.",
		}
	}

	return nil
}

type transactionExecutor struct {
	ctx context.Context
	txn *transaction
}

// Execute a statement with any parameters within this transaction.
func (executor *transactionExecutor) Execute(statement string, parameters ...interface{}) (*Result, error) {
	return executor.txn.execute(executor.ctx, statement, parameters...)
}

// Buffer a Result into a BufferedResult to use outside the context of this transaction.
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

// Abort the transaction, discarding any previous statement executions within this transaction.
func (executor *transactionExecutor) Abort() error {
	return errors.New("transaction aborted")
}
