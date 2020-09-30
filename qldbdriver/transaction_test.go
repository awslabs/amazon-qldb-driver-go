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
	"testing"

	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTransaction(t *testing.T) {
	t.Run("execute", func(t *testing.T) {
		mockHash, _ := toQLDBHash(mockTxnId)
		mockNextPageToken := "mockToken"
		var mockPageValues []*qldbsession.ValueHolder
		mockFirstPage := qldbsession.Page{
			NextPageToken: &mockNextPageToken,
			Values:        mockPageValues,
		}
		mockExecuteResult := qldbsession.ExecuteStatementResult{
			FirstPage: &mockFirstPage,
		}

		testTransaction := &transaction{
			communicator: nil,
			id:           &mockTxnId,
			logger:       nil,
			commitHash:   mockHash,
		}

		t.Run("success", func(t *testing.T) {
			mockService := new(mockTransactionService)
			mockService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&mockExecuteResult, nil)
			testTransaction.communicator = mockService

			result, err := testTransaction.execute(context.Background(), "mockStatement", "mockParam1", "mockParam2")
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, testTransaction.communicator, result.communicator)
			assert.Equal(t, testTransaction.id, result.txnId)
			assert.Equal(t, &mockNextPageToken, result.pageToken)
			assert.Equal(t, mockPageValues, result.pageValues)
		})

		t.Run("error", func(t *testing.T) {
			mockService := new(mockTransactionService)
			mockService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&mockExecuteResult, mockError)
			testTransaction.communicator = mockService

			result, err := testTransaction.execute(context.Background(), "mockStatement", "mockParam1", "mockParam2")
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Equal(t, mockError, err)
		})
	})

	t.Run("commit", func(t *testing.T) {
		mockTxnId := "mockId"

		mockHash1 := make([]byte, 1)
		mockHash1[0] = 0
		mockHash2 := make([]byte, 1)
		mockHash2[0] = 1
		mockCommitTransactionResult := qldbsession.CommitTransactionResult{
			CommitDigest: mockHash1,
		}

		testTransaction := &transaction{
			communicator: nil,
			id:           &mockTxnId,
			logger:       nil,
			commitHash:   &qldbHash{hash: mockHash1},
		}

		t.Run("success", func(t *testing.T) {
			mockService := new(mockTransactionService)
			mockService.On("commitTransaction", mock.Anything, mock.Anything, mock.Anything).Return(&mockCommitTransactionResult, nil)
			testTransaction.communicator = mockService

			assert.NoError(t, testTransaction.commit(context.Background()))
		})

		t.Run("error", func(t *testing.T) {
			mockService := new(mockTransactionService)
			mockService.On("commitTransaction", mock.Anything, mock.Anything, mock.Anything).Return(&mockCommitTransactionResult, mockError)
			testTransaction.communicator = mockService

			assert.Equal(t, mockError, testTransaction.commit(context.Background()))
		})

		t.Run("digest mismatch", func(t *testing.T) {
			mockService := new(mockTransactionService)
			mockService.On("commitTransaction", mock.Anything, mock.Anything, mock.Anything).Return(&mockCommitTransactionResult, nil)
			testTransaction.communicator = mockService
			mockCommitTransactionResult.CommitDigest = mockHash2

			assert.Error(t, testTransaction.commit(context.Background()))
		})
	})
}

func TestTransactionExecutor(t *testing.T) {
	mockId := "txnId"
	mockHash, _ := toQLDBHash(mockTxnId)

	mockTransaction := transaction{
		communicator: nil,
		id:           &mockId,
		logger:       mockLogger,
		commitHash:   mockHash,
	}

	testExecutor := transactionExecutor{
		ctx: context.Background(),
		txn: &mockTransaction,
	}

	t.Run("execute", func(t *testing.T) {
		mockNextPageToken := "mockToken"
		var mockPageValues []*qldbsession.ValueHolder
		mockFirstPage := qldbsession.Page{
			NextPageToken: &mockNextPageToken,
			Values:        mockPageValues,
		}
		mockExecuteResult := qldbsession.ExecuteStatementResult{
			FirstPage: &mockFirstPage,
		}

		t.Run("success", func(t *testing.T) {
			mockService := new(mockTransactionService)
			mockService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&mockExecuteResult, nil)
			mockTransaction.communicator = mockService

			result, err := testExecutor.Execute("mockStatement", "mockParam1", "mockParam2")
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, mockTransaction.communicator, result.communicator)
			assert.Equal(t, mockTransaction.id, result.txnId)
			assert.Equal(t, &mockNextPageToken, result.pageToken)
			assert.Equal(t, mockPageValues, result.pageValues)
		})

		t.Run("error", func(t *testing.T) {
			mockService := new(mockTransactionService)
			mockService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&mockExecuteResult, mockError)
			mockTransaction.communicator = mockService

			result, err := testExecutor.Execute("mockStatement", "mockParam1", "mockParam2")
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Equal(t, mockError, err)
		})
	})

	t.Run("BufferResult", func(t *testing.T) {
		mockIonBinary := make([]byte, 1)
		mockIonBinary[0] = 1
		mockValueHolder := &qldbsession.ValueHolder{IonBinary: mockIonBinary}
		mockPageValues := make([]*qldbsession.ValueHolder, 1)
		// Has only one value
		mockPageValues[0] = mockValueHolder

		mockNextIonBinary := make([]byte, 1)
		mockNextIonBinary[0] = 2
		mockNextValueHolder := &qldbsession.ValueHolder{IonBinary: mockNextIonBinary}
		mockNextPageValues := make([]*qldbsession.ValueHolder, 1)
		// Has only one value
		mockNextPageValues[0] = mockNextValueHolder
		mockFetchPageResult := qldbsession.FetchPageResult{Page: &qldbsession.Page{Values: mockNextPageValues}}

		mockPageToken := "mockToken"

		testResult := Result{
			ctx:          context.Background(),
			communicator: nil,
			txnId:        &mockId,
			pageValues:   mockPageValues,
			pageToken:    &mockPageToken,
			index:        0,
			logger:       mockLogger,
		}

		t.Run("success", func(t *testing.T) {
			mockService := new(mockTransactionService)
			mockService.On("fetchPage", mock.Anything, mock.Anything, mock.Anything).Return(&mockFetchPageResult, nil)
			testResult.communicator = mockService

			bufferedResult, err := testExecutor.BufferResult(&testResult)
			assert.Nil(t, err)
			assert.True(t, bufferedResult.Next())
			assert.Equal(t, mockIonBinary, bufferedResult.GetCurrentData())
			assert.True(t, bufferedResult.Next())
			assert.Equal(t, mockNextIonBinary, bufferedResult.GetCurrentData())
		})

		t.Run("error", func(t *testing.T) {
			mockService := new(mockTransactionService)
			mockService.On("fetchPage", mock.Anything, mock.Anything, mock.Anything).Return(&mockFetchPageResult, mockError)
			testResult.communicator = mockService
			// Reset Result state
			testResult.pageValues = mockPageValues
			testResult.pageToken = &mockPageToken
			testResult.index = 0

			bufferedResult, err := testExecutor.BufferResult(&testResult)
			assert.Nil(t, bufferedResult)
			assert.Equal(t, mockError, err)
		})
	})

	t.Run("Abort", func(t *testing.T) {
		abort := testExecutor.Abort()
		assert.Error(t, abort)
	})
}

type mockTransactionService struct {
	mock.Mock
}

func (m mockTransactionService) abortTransaction(ctx context.Context) (*qldbsession.AbortTransactionResult, error) {
	args := m.Called(ctx)
	return args.Get(0).(*qldbsession.AbortTransactionResult), args.Error(1)
}

func (m mockTransactionService) commitTransaction(ctx context.Context, txnId *string, commitDigest []byte) (*qldbsession.CommitTransactionResult, error) {
	args := m.Called(ctx, txnId, commitDigest)
	return args.Get(0).(*qldbsession.CommitTransactionResult), args.Error(1)
}

func (m mockTransactionService) executeStatement(ctx context.Context, statement *string, parameters []*qldbsession.ValueHolder, txnId *string) (*qldbsession.ExecuteStatementResult, error) {
	args := m.Called(ctx, statement, parameters, txnId)
	return args.Get(0).(*qldbsession.ExecuteStatementResult), args.Error(1)
}

func (m mockTransactionService) endSession(ctx context.Context) (*qldbsession.EndSessionResult, error) {
	panic("not used")
}

func (m mockTransactionService) fetchPage(ctx context.Context, pageToken *string, txnId *string) (*qldbsession.FetchPageResult, error) {
	args := m.Called(ctx, pageToken, txnId)
	return args.Get(0).(*qldbsession.FetchPageResult), args.Error(1)
}

func (m mockTransactionService) startTransaction(ctx context.Context) (*qldbsession.StartTransactionResult, error) {
	panic("not used")
}
