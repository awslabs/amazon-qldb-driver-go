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

import "context"

type session struct {
	communicator *communicator
	retryLimit   uint8
	logger       *qldbLogger
}

func (session *session) endSession(ctx context.Context) error {
	_, err := session.communicator.endSession(ctx)
	return err
}

func (session *session) execute(ctx context.Context, fn func(txn Transaction) (interface{}, error)) (interface{}, error) {
	txn, err := session.startTransaction(ctx)
	if err != nil {
		return nil, err
	}
	txnExecutor := &transactionExecutor{ctx, txn}
	return fn(txnExecutor)
}

func (session *session) startTransaction(ctx context.Context) (*transaction, error) {
	result, err := session.communicator.startTransaction(ctx)
	if err != nil {
		return nil, err
	}

	txnHash, err := toQLDBHash(*result.TransactionId)
	if err != nil {
		return nil, err
	}

	return &transaction{session.communicator, result.TransactionId, session.logger, txnHash}, nil
}
