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
	"regexp"

	"github.com/aws/aws-sdk-go-v2/service/qldbsession/types"
	"github.com/aws/smithy-go"
)

var regex = regexp.MustCompile(`Transaction\s.*\shas\sexpired`)

type session struct {
	communicator qldbService
	logger       *qldbLogger
}

func (session *session) endSession(ctx context.Context) error {
	_, err := session.communicator.endSession(ctx)
	return err
}

func (session *session) execute(ctx context.Context, fn func(txn Transaction) (interface{}, error)) (interface{}, *txnError) {
	txn, err := session.startTransaction(ctx)
	if err != nil {
		return nil, session.wrapError(ctx, err, "")
	}

	result, err := fn(&transactionExecutor{ctx, txn})
	if err != nil {
		return nil, session.wrapError(ctx, err, *txn.id)
	}

	err = txn.commit(ctx)
	if err != nil {
		return nil, session.wrapError(ctx, err, *txn.id)
	}

	return result, nil
}

func (session *session) wrapError(ctx context.Context, err error, transID string) *txnError {
	var ise *types.InvalidSessionException
	var occ *types.OccConflictException
	var apiErr smithy.APIError
	switch {
	case errors.As(err, &ise):
		match := regex.MatchString(ise.ErrorMessage())
		return &txnError{
			transactionID: transID,
			message:       "Invalid Session Exception.",
			err:           err,
			canRetry:      !match,
			abortSuccess:  false,
			isISE:         true,
		}
	case errors.As(err, &occ):
		return &txnError{
			transactionID: transID,
			message:       "OCC Conflict Exception.",
			err:           err,
			canRetry:      true,
			abortSuccess:  true,
			isISE:         false,
		}
	case errors.As(err, &apiErr):
		code := apiErr.ErrorCode()
		if code == "InternalFailure" || code == "ServiceUnavailable" {
			return &txnError{
				transactionID: transID,
				message:       "Service unavailable or internal error.",
				err:           err,
				canRetry:      true,
				abortSuccess:  session.tryAbort(ctx),
				isISE:         false,
			}
		}
	}
	return &txnError{
		transactionID: transID,
		message:       "",
		err:           err,
		canRetry:      false,
		abortSuccess:  session.tryAbort(ctx),
		isISE:         false,
	}
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

func (session *session) tryAbort(ctx context.Context) bool {
	_, err := session.communicator.abortTransaction(ctx)
	if err != nil {
		session.logger.logf(LogDebug, "Failed to abort the transaction.\nCaused by '%v'", err.Error())
		return false
	}
	return true
}
