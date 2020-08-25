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
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSessionStartTransaction(t *testing.T) {
	testCommunicator := communicator{
		service:      nil,
		sessionToken: &mockSessionToken,
		logger:       mockLogger,
	}

	t.Run("error", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testStartTransaction, mockError)
		testCommunicator.service = mockSession
		session := session{&testCommunicator, mockLogger}

		result, err := session.startTransaction(context.Background())

		assert.Equal(t, mockError, err)
		assert.Nil(t, result)
	})

	t.Run("success", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testStartTransaction, nil)
		testCommunicator.service = mockSession
		session := session{&testCommunicator, mockLogger}

		result, err := session.startTransaction(context.Background())

		assert.Nil(t, err)
		assert.Equal(t, testTxnID, *result.id)
	})
}

func TestSessionEndSession(t *testing.T) {
	testCommunicator := communicator{
		service:      nil,
		sessionToken: &mockSessionToken,
		logger:       mockLogger,
	}

	t.Run("error", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testEndSession, mockError)
		testCommunicator.service = mockSession
		session := session{&testCommunicator, mockLogger}

		err := session.endSession(context.Background())

		assert.Equal(t, mockError, err)
	})

	t.Run("success", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testEndSession, nil)
		testCommunicator.service = mockSession
		session := session{&testCommunicator, mockLogger}

		err := session.endSession(context.Background())

		assert.Nil(t, err)
	})
}

func TestSessionExecute(t *testing.T) {
	testCommunicator := communicator{
		service:      nil,
		sessionToken: &mockSessionToken,
		logger:       mockLogger,
	}

	t.Run("success", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testStartTransaction, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testExecute, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testCommit, nil).Once()
		testCommunicator.service = mockSession

		session := session{&testCommunicator, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, err)
		assert.Equal(t, 3, result)
	})

	t.Run("startTxnUnknownError", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testStartTransaction, mockError)
		testCommunicator.service = mockSession
		session := session{&testCommunicator, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, mockError, err.err)
	})

	t.Run("startTxnISE", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testStartTransaction, testISE)
		testCommunicator.service = mockSession
		session := session{&testCommunicator, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, testISE, err.err)
	})

	t.Run("startTxn500", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testStartTransaction, test500)
		testCommunicator.service = mockSession
		session := session{&testCommunicator, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.IsType(t, &txnError{}, err)
		assert.Equal(t, test500, err.err)
		assert.Equal(t, "", err.transactionID)
	})

	t.Run("executeUnknownError", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testStartTransaction, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testExecute, mockError).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testAbortTransaction, nil).Once()
		testCommunicator.service = mockSession
		session := session{&testCommunicator, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, mockError, err.err)
		assert.True(t, err.abortSuccess)
	})

	t.Run("executeISE", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testStartTransaction, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testExecute, testISE).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testAbortTransaction, nil).Once()
		testCommunicator.service = mockSession
		session := session{&testCommunicator, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, testISE, err.err)
		assert.False(t, err.abortSuccess)
	})

	t.Run("execute500", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testStartTransaction, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testExecute, test500).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testAbortTransaction, nil).Once()
		testCommunicator.service = mockSession
		session := session{&testCommunicator, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.IsType(t, &txnError{}, err)
		assert.Equal(t, test500, err.err)
		assert.Equal(t, testTxnID, err.transactionID)
		assert.True(t, err.abortSuccess)
	})

	t.Run("executeBadReq", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testStartTransaction, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testExecute, testBadReq).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testAbortTransaction, nil).Once()
		testCommunicator.service = mockSession
		session := session{&testCommunicator, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, testBadReq, err.err)
		assert.True(t, err.abortSuccess)
	})

	t.Run("commitUnknownError", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testStartTransaction, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testExecute, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testCommit, mockError).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testAbortTransaction, nil).Once()
		testCommunicator.service = mockSession
		session := session{&testCommunicator, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, mockError, err.err)
		assert.True(t, err.abortSuccess)
	})

	t.Run("commit500", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testStartTransaction, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testExecute, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testCommit, test500).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testAbortTransaction, nil).Once()
		testCommunicator.service = mockSession
		session := session{&testCommunicator, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.IsType(t, &txnError{}, err)
		assert.Equal(t, test500, err.err)
		assert.Equal(t, testTxnID, err.transactionID)
	})

	t.Run("commitOCC", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testStartTransaction, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testExecute, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).
			Return(&testCommit, testOCC).Once()
		testCommunicator.service = mockSession
		session := session{&testCommunicator, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.IsType(t, &txnError{}, err)
	})
}

var testTxnID = "testTransactionIdddddd"
var testAbortTransaction = qldbsession.SendCommandOutput{
	AbortTransaction: &qldbsession.AbortTransactionResult{},
}

var testStartTransaction = qldbsession.SendCommandOutput{
	StartTransaction: &qldbsession.StartTransactionResult{
		TransactionId: &testTxnID,
	},
}

var testEndSession = qldbsession.SendCommandOutput{
	EndSession: &qldbsession.EndSessionResult{},
}

var testExecute = qldbsession.SendCommandOutput{
	ExecuteStatement: &qldbsession.ExecuteStatementResult{
		FirstPage: &qldbsession.Page{},
	},
}

var testHash = []byte{73, 10, 104, 87, 43, 252, 182, 60, 142, 193, 0, 77, 158, 129, 52, 84, 126, 196, 120, 55, 241, 253, 113, 114, 114, 53, 233, 223, 234, 227, 191, 172}
var testCommit = qldbsession.SendCommandOutput{
	CommitTransaction: &qldbsession.CommitTransactionResult{
		TransactionId: &testTxnID,
		CommitDigest:  testHash,
	},
}

var testISE = awserr.New(qldbsession.ErrCodeInvalidSessionException, "Invalid session", nil)
var testOCC = awserr.New(qldbsession.ErrCodeOccConflictException, "OCC", nil)
var testBadReq = awserr.New(qldbsession.ErrCodeBadRequestException, "Bad request", nil)
var test500 = awserr.New(http.StatusText(http.StatusInternalServerError), "Five Hundred", nil)
