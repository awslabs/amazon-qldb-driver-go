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
	t.Run("error", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, mockError)
		session := session{mockSessionService, mockLogger}

		result, err := session.startTransaction(context.Background())

		assert.Equal(t, mockError, err)
		assert.Nil(t, result)
	})

	t.Run("success", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, nil)
		session := session{mockSessionService, mockLogger}

		result, err := session.startTransaction(context.Background())

		assert.Nil(t, err)
		assert.Equal(t, mockTransactionId, *result.id)
	})
}

func TestSessionEndSession(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("endSession", mock.Anything).Return(&mockEndSessionResult, mockError)
		session := session{mockSessionService, mockLogger}

		err := session.endSession(context.Background())

		assert.Equal(t, mockError, err)
	})

	t.Run("success", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("endSession", mock.Anything).Return(&mockEndSessionResult, nil)
		session := session{mockSessionService, mockLogger}

		err := session.endSession(context.Background())

		assert.Nil(t, err)
	})
}

func TestSessionExecute(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, nil)
		mockSessionService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockExecuteResult, nil)
		mockSessionService.On("commitTransaction", mock.Anything, mock.Anything, mock.Anything).
			Return(&mockCommitTransactionResult, nil)
		session := session{mockSessionService, mockLogger}

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

	t.Run("startTxnUnknownErrorAbortSuccess", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, mockError)
		mockSessionService.On("abortTransaction", mock.Anything).Return(&mockAbortTransactionResult, nil)
		session := session{mockSessionService, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, mockError, err.err)
		assert.False(t, err.isISE)
		assert.False(t, err.canRetry)
		assert.True(t, err.abortSuccess)
	})

	t.Run("startTxnUnknownErrorAbortErr", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, mockError)
		mockSessionService.On("abortTransaction", mock.Anything).Return(&mockAbortTransactionResult, mockError)
		session := session{mockSessionService, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, mockError, err.err)
		assert.False(t, err.isISE)
		assert.False(t, err.canRetry)
		assert.False(t, err.abortSuccess)
	})

	t.Run("startTxnISE", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, testISE)
		session := session{mockSessionService, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, testISE, err.err)
		assert.True(t, err.isISE)
		assert.True(t, err.canRetry)
		assert.False(t, err.abortSuccess)
	})

	t.Run("startTxn500AbortSuccess", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, test500)
		mockSessionService.On("abortTransaction", mock.Anything).Return(&mockAbortTransactionResult, nil)
		session := session{mockSessionService, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, test500, err.err)
		assert.Equal(t, "", err.transactionID)
		assert.False(t, err.isISE)
		assert.True(t, err.canRetry)
		assert.True(t, err.abortSuccess)
	})

	t.Run("startTxn500AbortError", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, test500)
		mockSessionService.On("abortTransaction", mock.Anything).Return(&mockAbortTransactionResult, mockError)
		session := session{mockSessionService, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, test500, err.err)
		assert.Equal(t, "", err.transactionID)
		assert.False(t, err.isISE)
		assert.True(t, err.canRetry)
		assert.False(t, err.abortSuccess)
	})

	t.Run("executeUnknownErrorAbortSuccess", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, nil)
		mockSessionService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockExecuteResult, mockError)
		mockSessionService.On("abortTransaction", mock.Anything).Return(&mockAbortTransactionResult, nil)
		session := session{mockSessionService, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, mockError, err.err)
		assert.False(t, err.isISE)
		assert.False(t, err.canRetry)
		assert.True(t, err.abortSuccess)
	})

	t.Run("executeUnknownErrorAbortError", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, nil)
		mockSessionService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockExecuteResult, mockError)
		mockSessionService.On("abortTransaction", mock.Anything).Return(&mockAbortTransactionResult, mockError)
		session := session{mockSessionService, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, mockError, err.err)
		assert.False(t, err.isISE)
		assert.False(t, err.canRetry)
		assert.False(t, err.abortSuccess)
	})

	t.Run("executeISE", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, nil)
		mockSessionService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockExecuteResult, testISE)
		session := session{mockSessionService, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, testISE, err.err)
		assert.True(t, err.isISE)
		assert.True(t, err.canRetry)
		assert.False(t, err.abortSuccess)
	})

	t.Run("execute500AbortSuccess", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, nil)
		mockSessionService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockExecuteResult, test500)
		mockSessionService.On("abortTransaction", mock.Anything).Return(&mockAbortTransactionResult, nil)
		session := session{mockSessionService, mockLogger}

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
		assert.Equal(t, mockTransactionId, err.transactionID)
		assert.False(t, err.isISE)
		assert.True(t, err.canRetry)
		assert.True(t, err.abortSuccess)
	})

	t.Run("execute500AbortError", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, nil)
		mockSessionService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockExecuteResult, test500)
		mockSessionService.On("abortTransaction", mock.Anything).Return(&mockAbortTransactionResult, mockError)
		session := session{mockSessionService, mockLogger}

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
		assert.Equal(t, mockTransactionId, err.transactionID)
		assert.False(t, err.isISE)
		assert.True(t, err.canRetry)
		assert.False(t, err.abortSuccess)
	})

	t.Run("executeBadReqAbortSuccess", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, nil)
		mockSessionService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockExecuteResult, testBadReq)
		mockSessionService.On("abortTransaction", mock.Anything).Return(&mockAbortTransactionResult, nil)
		session := session{mockSessionService, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, testBadReq, err.err)
		assert.False(t, err.isISE)
		assert.False(t, err.canRetry)
		assert.True(t, err.abortSuccess)
	})

	t.Run("executeBadReqAbortError", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, nil)
		mockSessionService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockExecuteResult, testBadReq)
		mockSessionService.On("abortTransaction", mock.Anything).Return(&mockAbortTransactionResult, mockError)
		session := session{mockSessionService, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, testBadReq, err.err)
		assert.False(t, err.isISE)
		assert.False(t, err.canRetry)
		assert.False(t, err.abortSuccess)
	})

	t.Run("commitUnknownErrorAbortSuccess", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, nil)
		mockSessionService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockExecuteResult, nil)
		mockSessionService.On("commitTransaction", mock.Anything, mock.Anything, mock.Anything).
			Return(&mockCommitTransactionResult, mockError)
		mockSessionService.On("abortTransaction", mock.Anything).Return(&mockAbortTransactionResult, nil)
		session := session{mockSessionService, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, mockError, err.err)
		assert.False(t, err.isISE)
		assert.False(t, err.canRetry)
		assert.True(t, err.abortSuccess)
	})

	t.Run("commitUnknownErrorAbortError", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, nil)
		mockSessionService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockExecuteResult, nil)
		mockSessionService.On("commitTransaction", mock.Anything, mock.Anything, mock.Anything).
			Return(&mockCommitTransactionResult, mockError)
		mockSessionService.On("abortTransaction", mock.Anything).Return(&mockAbortTransactionResult, mockError)
		session := session{mockSessionService, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, mockError, err.err)
		assert.False(t, err.isISE)
		assert.False(t, err.canRetry)
		assert.False(t, err.abortSuccess)
	})

	t.Run("commit500AbortSuccess", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, nil)
		mockSessionService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockExecuteResult, nil)
		mockSessionService.On("commitTransaction", mock.Anything, mock.Anything, mock.Anything).
			Return(&mockCommitTransactionResult, test500)
		mockSessionService.On("abortTransaction", mock.Anything).Return(&mockAbortTransactionResult, nil)
		session := session{mockSessionService, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, test500, err.err)
		assert.Equal(t, mockTransactionId, err.transactionID)
		assert.False(t, err.isISE)
		assert.True(t, err.canRetry)
		assert.True(t, err.abortSuccess)
	})

	t.Run("commit500AbortError", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, nil)
		mockSessionService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockExecuteResult, nil)
		mockSessionService.On("commitTransaction", mock.Anything, mock.Anything, mock.Anything).
			Return(&mockCommitTransactionResult, test500)
		mockSessionService.On("abortTransaction", mock.Anything).Return(&mockAbortTransactionResult, mockError)
		session := session{mockSessionService, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, test500, err.err)
		assert.Equal(t, mockTransactionId, err.transactionID)
		assert.False(t, err.isISE)
		assert.True(t, err.canRetry)
		assert.False(t, err.abortSuccess)
	})

	t.Run("commitOCC", func(t *testing.T) {
		mockSessionService := new(mockSessionService)
		mockSessionService.On("startTransaction", mock.Anything).Return(&mockStartTransactionResult, nil)
		mockSessionService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockExecuteResult, nil)
		mockSessionService.On("commitTransaction", mock.Anything, mock.Anything, mock.Anything).
			Return(&mockCommitTransactionResult, testOCC)
		session := session{mockSessionService, mockLogger}

		result, err := session.execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err := txn.Execute("SELECT v FROM table")
			if err != nil {
				return nil, err
			}
			return 3, nil
		})

		assert.Nil(t, result)
		assert.Equal(t, testOCC, err.err)
		assert.False(t, err.isISE)
		assert.True(t, err.canRetry)
		assert.True(t, err.abortSuccess)
	})
}

var mockTransactionId = "testTransactionIdddddd"
var mockAbortTransactionResult = qldbsession.AbortTransactionResult{}
var mockStartTransactionResult = qldbsession.StartTransactionResult{TransactionId: &mockTransactionId}
var mockEndSessionResult = qldbsession.EndSessionResult{}
var mockExecuteResult = qldbsession.ExecuteStatementResult{
	FirstPage: &qldbsession.Page{},
}
var mockHash = []byte{73, 10, 104, 87, 43, 252, 182, 60, 142, 193, 0, 77, 158, 129, 52, 84, 126, 196, 120, 55, 241, 253, 113, 114, 114, 53, 233, 223, 234, 227, 191, 172}
var mockCommitTransactionResult = qldbsession.CommitTransactionResult{
	TransactionId: &mockTransactionId,
	CommitDigest:  mockHash,
}

var testISE = awserr.New(qldbsession.ErrCodeInvalidSessionException, "Invalid session", nil)
var testOCC = awserr.New(qldbsession.ErrCodeOccConflictException, "OCC", nil)
var testBadReq = awserr.New(qldbsession.ErrCodeBadRequestException, "Bad request", nil)
var test500 = awserr.New(http.StatusText(http.StatusInternalServerError), "Five Hundred", nil)

type mockSessionService struct {
	mock.Mock
}

func (m mockSessionService) abortTransaction(ctx context.Context) (*qldbsession.AbortTransactionResult, error) {
	args := m.Called(ctx)
	return args.Get(0).(*qldbsession.AbortTransactionResult), args.Error(1)
}

func (m mockSessionService) commitTransaction(ctx context.Context, txnId *string, commitDigest []byte) (*qldbsession.CommitTransactionResult, error) {
	args := m.Called(ctx, txnId, commitDigest)
	return args.Get(0).(*qldbsession.CommitTransactionResult), args.Error(1)
}

func (m mockSessionService) executeStatement(ctx context.Context, statement *string, parameters []*qldbsession.ValueHolder, txnId *string) (*qldbsession.ExecuteStatementResult, error) {
	args := m.Called(ctx, statement, parameters, txnId)
	return args.Get(0).(*qldbsession.ExecuteStatementResult), args.Error(1)
}

func (m mockSessionService) endSession(ctx context.Context) (*qldbsession.EndSessionResult, error) {
	args := m.Called(ctx)
	return args.Get(0).(*qldbsession.EndSessionResult), args.Error(1)
}

func (m mockSessionService) fetchPage(ctx context.Context, pageToken *string, txnId *string) (*qldbsession.FetchPageResult, error) {
	panic("not used")
}

func (m mockSessionService) startTransaction(ctx context.Context) (*qldbsession.StartTransactionResult, error) {
	args := m.Called(ctx)
	return args.Get(0).(*qldbsession.StartTransactionResult), args.Error(1)
}
