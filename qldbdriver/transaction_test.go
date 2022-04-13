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

	"github.com/aws/aws-sdk-go-v2/service/qldbsession/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestTransaction(t *testing.T) {
	t.Run("execute", func(t *testing.T) {
		mockHash, _ := toQLDBHash(mockTxnID)
		mockNextPageToken := "mockToken"
		var mockPageValues []types.ValueHolder
		mockFirstPage := types.Page{
			NextPageToken: &mockNextPageToken,
			Values:        mockPageValues,
		}

		readIOs := int64(1)
		writeIOs := int64(2)
		processingTimeMilliseconds := int64(3)
		qldbsessionTimingInformation := generateQldbsessionTimingInformation(processingTimeMilliseconds)
		qldbsessionConsumedIOs := generateQldbsessionIOUsage(readIOs, writeIOs)

		executeResult := types.ExecuteStatementResult{
			FirstPage: &mockFirstPage,
		}

		executeResultWithQueryStats := executeResult
		executeResultWithQueryStats.TimingInformation = qldbsessionTimingInformation
		executeResultWithQueryStats.ConsumedIOs = qldbsessionConsumedIOs

		testTransaction := &transaction{
			communicator: nil,
			id:           &mockTxnID,
			logger:       nil,
			commitHash:   mockHash,
		}

		t.Run("success", func(t *testing.T) {
			mockService := new(mockTransactionService)
			mockService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&executeResult, nil)
			testTransaction.communicator = mockService

			result, err := testTransaction.execute(context.Background(), "mockStatement", "mockParam1", "mockParam2")
			assert.NoError(t, err)
			assert.NotNil(t, result)

			assert.Equal(t, testTransaction.communicator, result.communicator)
			assert.Equal(t, testTransaction.id, result.txnID)
			assert.Equal(t, &mockNextPageToken, result.pageToken)
			assert.Equal(t, mockPageValues, result.pageValues)
			assert.Equal(t, int64(0), *result.GetConsumedIOs().GetReadIOs())
			assert.Equal(t, int64(0), *result.GetConsumedIOs().getWriteIOs())
			assert.Equal(t, int64(0), *result.GetTimingInformation().GetProcessingTimeMilliseconds())
		})

		t.Run("success and execute statement result contains query stats", func(t *testing.T) {
			mockService := new(mockTransactionService)
			mockService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&executeResultWithQueryStats, nil)
			testTransaction.communicator = mockService

			result, err := testTransaction.execute(context.Background(), "mockStatement", "mockParam1", "mockParam2")
			assert.NoError(t, err)
			assert.NotNil(t, result)

			assert.Equal(t, testTransaction.communicator, result.communicator)
			assert.Equal(t, testTransaction.id, result.txnID)
			assert.Equal(t, &mockNextPageToken, result.pageToken)
			assert.Equal(t, mockPageValues, result.pageValues)
			assert.Equal(t, readIOs, *result.GetConsumedIOs().GetReadIOs())
			assert.Equal(t, writeIOs, *result.GetConsumedIOs().getWriteIOs())
			assert.Equal(t, processingTimeMilliseconds, *result.GetTimingInformation().GetProcessingTimeMilliseconds())
		})

		t.Run("error", func(t *testing.T) {
			mockService := new(mockTransactionService)
			mockService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&mockExecuteResult, errMock)
			testTransaction.communicator = mockService

			result, err := testTransaction.execute(context.Background(), "mockStatement", "mockParam1", "mockParam2")
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Equal(t, errMock, err)
		})
	})

	t.Run("commit", func(t *testing.T) {
		mockTxnID := "mockId"

		mockHash1 := make([]byte, 1)
		mockHash1[0] = 0
		mockHash2 := make([]byte, 1)
		mockHash2[0] = 1
		mockCommitTransactionResult := types.CommitTransactionResult{
			CommitDigest: mockHash1,
		}

		testTransaction := &transaction{
			communicator: nil,
			id:           &mockTxnID,
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
			mockService.On("commitTransaction", mock.Anything, mock.Anything, mock.Anything).Return(&mockCommitTransactionResult, errMock)
			testTransaction.communicator = mockService

			assert.Equal(t, errMock, testTransaction.commit(context.Background()))
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
	mockID := "txnID"
	mockHash, _ := toQLDBHash(mockTxnID)

	mockTransaction := transaction{
		communicator: nil,
		id:           &mockID,
		logger:       mockLogger,
		commitHash:   mockHash,
	}

	testExecutor := transactionExecutor{
		ctx: context.Background(),
		txn: &mockTransaction,
	}

	t.Run("execute", func(t *testing.T) {
		mockNextPageToken := "mockToken"
		var mockPageValues []types.ValueHolder
		mockFirstPage := types.Page{
			NextPageToken: &mockNextPageToken,
			Values:        mockPageValues,
		}
		mockExecuteResult := types.ExecuteStatementResult{
			FirstPage: &mockFirstPage,
		}

		t.Run("success", func(t *testing.T) {
			mockService := new(mockTransactionService)
			mockService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&mockExecuteResult, nil)
			mockTransaction.communicator = mockService

			res, err := testExecutor.Execute("mockStatement", "mockParam1", "mockParam2")
			assert.NoError(t, err)
			assert.NotNil(t, res)

			result, ok := res.(*result)
			require.True(t, ok)

			assert.Equal(t, mockTransaction.communicator, result.communicator)
			assert.Equal(t, mockTransaction.id, result.txnID)
			assert.Equal(t, &mockNextPageToken, result.pageToken)
			assert.Equal(t, mockPageValues, result.pageValues)
		})

		t.Run("error", func(t *testing.T) {
			mockService := new(mockTransactionService)
			mockService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&mockExecuteResult, errMock)
			mockTransaction.communicator = mockService

			result, err := testExecutor.Execute("mockStatement", "mockParam1", "mockParam2")
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Equal(t, errMock, err)
		})

		t.Run("execute result does not contain IOUsage and TimingInformation", func(t *testing.T) {
			mockService := new(mockTransactionService)
			mockService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&mockExecuteResult, nil)
			mockTransaction.communicator = mockService

			res, err := testExecutor.Execute("mockStatement", "mockParam1", "mockParam2")
			assert.NoError(t, err)
			assert.NotNil(t, res)

			result, ok := res.(*result)
			require.True(t, ok)

			assert.Equal(t, int64(0), *result.ioUsage.GetReadIOs())
			assert.Equal(t, int64(0), *result.ioUsage.getWriteIOs())
			assert.Equal(t, int64(0), *result.timingInfo.GetProcessingTimeMilliseconds())
		})

		t.Run("execute result contains IOUsage and TimingInformation", func(t *testing.T) {
			mockService := new(mockTransactionService)

			readIOs := int64(1)
			writeIOs := int64(2)
			timingInfo := int64(3)

			timingInformation := generateQldbsessionTimingInformation(timingInfo)
			consumedIOs := generateQldbsessionIOUsage(readIOs, writeIOs)

			mockExecuteResultWithQueryStats := mockExecuteResult
			mockExecuteResultWithQueryStats.TimingInformation = timingInformation
			mockExecuteResultWithQueryStats.ConsumedIOs = consumedIOs

			mockService.On("executeStatement", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&mockExecuteResultWithQueryStats, nil)
			mockTransaction.communicator = mockService

			res, err := testExecutor.Execute("mockStatement", "mockParam1", "mockParam2")
			assert.NoError(t, err)
			assert.NotNil(t, res)

			result, ok := res.(*result)
			require.True(t, ok)

			assert.Equal(t, &readIOs, result.ioUsage.readIOs)
			assert.Equal(t, &writeIOs, result.ioUsage.writeIOs)
			assert.Equal(t, &timingInfo, result.timingInfo.processingTimeMilliseconds)
		})
	})

	t.Run("BufferResult", func(t *testing.T) {
		mockIonBinary := make([]byte, 1)
		mockIonBinary[0] = 1
		mockValueHolder := types.ValueHolder{IonBinary: mockIonBinary}
		mockPageValues := make([]types.ValueHolder, 1)
		// Has only one value
		mockPageValues[0] = mockValueHolder

		mockNextIonBinary := make([]byte, 1)
		mockNextIonBinary[0] = 2
		mockNextValueHolder := types.ValueHolder{IonBinary: mockNextIonBinary}
		mockNextPageValues := make([]types.ValueHolder, 1)
		// Has only one value
		mockNextPageValues[0] = mockNextValueHolder
		mockFetchPageResult := types.FetchPageResult{Page: &types.Page{Values: mockNextPageValues}}

		mockPageToken := "mockToken"
		readIOs := int64(1)
		writeIOs := int64(2)
		processingTime := int64(3)

		testResult := result{
			ctx:          context.Background(),
			communicator: nil,
			txnID:        &mockID,
			pageValues:   mockPageValues,
			pageToken:    &mockPageToken,
			index:        0,
			logger:       mockLogger,
			ioUsage:      newIOUsage(readIOs, writeIOs),
			timingInfo:   newTimingInformation(processingTime),
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
			assert.Equal(t, processingTime, *bufferedResult.GetTimingInformation().GetProcessingTimeMilliseconds())
			assert.Equal(t, readIOs, *bufferedResult.GetConsumedIOs().GetReadIOs())
			assert.Equal(t, writeIOs, *bufferedResult.GetConsumedIOs().getWriteIOs())
		})

		t.Run("error", func(t *testing.T) {
			mockService := new(mockTransactionService)
			mockService.On("fetchPage", mock.Anything, mock.Anything, mock.Anything).Return(&mockFetchPageResult, errMock)
			testResult.communicator = mockService
			// Reset result state
			testResult.pageValues = mockPageValues
			testResult.pageToken = &mockPageToken
			testResult.index = 0

			bufferedResult, err := testExecutor.BufferResult(&testResult)
			assert.Nil(t, bufferedResult)
			assert.Equal(t, errMock, err)
		})
	})

	t.Run("Abort", func(t *testing.T) {
		abort := testExecutor.Abort()
		assert.Error(t, abort)
	})

	t.Run("Transaction ID", func(t *testing.T) {
		id := testExecutor.ID()
		assert.Equal(t, mockID, id)
	})
}

type mockTransactionService struct {
	mock.Mock
}

func (m *mockTransactionService) abortTransaction(ctx context.Context) (*types.AbortTransactionResult, error) {
	args := m.Called(ctx)
	return args.Get(0).(*types.AbortTransactionResult), args.Error(1)
}

func (m *mockTransactionService) commitTransaction(ctx context.Context, txnID *string, commitDigest []byte) (*types.CommitTransactionResult, error) {
	args := m.Called(ctx, txnID, commitDigest)
	return args.Get(0).(*types.CommitTransactionResult), args.Error(1)
}

func (m *mockTransactionService) executeStatement(ctx context.Context, statement *string, parameters []types.ValueHolder, txnID *string) (*types.ExecuteStatementResult, error) {
	args := m.Called(ctx, statement, parameters, txnID)
	return args.Get(0).(*types.ExecuteStatementResult), args.Error(1)
}

func (m *mockTransactionService) endSession(ctx context.Context) (*types.EndSessionResult, error) {
	panic("not used")
}

func (m *mockTransactionService) fetchPage(ctx context.Context, pageToken *string, txnID *string) (*types.FetchPageResult, error) {
	args := m.Called(ctx, pageToken, txnID)
	return args.Get(0).(*types.FetchPageResult), args.Error(1)
}

func (m *mockTransactionService) startTransaction(ctx context.Context) (*types.StartTransactionResult, error) {
	panic("not used")
}
