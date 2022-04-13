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
)

func TestResult(t *testing.T) {
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

	readIOs := int64(1)
	writeIOs := int64(2)
	processingTimeMilliseconds := int64(3)
	qldbsessionTimingInformation := generateQldbsessionTimingInformation(processingTimeMilliseconds)
	qldbsessionConsumedIOs := generateQldbsessionIOUsage(readIOs, writeIOs)

	res := &result{
		ctx:          nil,
		communicator: nil,
		txnID:        nil,
		pageValues:   mockPageValues,
		pageToken:    nil,
		index:        0,
		logger:       nil,
		ioUsage:      newIOUsage(0, 0),
		timingInfo:   newTimingInformation(0),
	}

	fetchPageResult := types.FetchPageResult{Page: &types.Page{Values: mockNextPageValues}}
	fetchPageResultWithStats := fetchPageResult
	fetchPageResultWithStats.TimingInformation = qldbsessionTimingInformation
	fetchPageResultWithStats.ConsumedIOs = qldbsessionConsumedIOs

	t.Run("Next", func(t *testing.T) {
		t.Run("pageToken is nil", func(t *testing.T) {
			res.index = 0
			res.pageToken = nil

			assert.True(t, res.Next(&transactionExecutor{nil, nil}))
			assert.Equal(t, mockIonBinary, res.GetCurrentData())

			// No more values
			assert.False(t, res.Next(&transactionExecutor{nil, nil}))
			assert.Nil(t, res.GetCurrentData())
			assert.NoError(t, res.Err())
		})

		t.Run("pageToken present", func(t *testing.T) {
			mockToken := "mockToken"

			t.Run("success", func(t *testing.T) {
				res.index = 0
				res.pageToken = &mockToken
				mockService := new(mockResultService)
				mockService.On("fetchPage", mock.Anything, mock.Anything, mock.Anything).Return(&fetchPageResult, nil)
				res.communicator = mockService

				// Default page
				assert.True(t, res.Next(&transactionExecutor{nil, nil}))
				assert.Equal(t, mockIonBinary, res.GetCurrentData())

				// Fetched page
				assert.True(t, res.Next(&transactionExecutor{nil, nil}))
				assert.Equal(t, mockNextIonBinary, res.GetCurrentData())

				// No more results
				assert.False(t, res.Next(&transactionExecutor{nil, nil}))
				assert.Nil(t, res.GetCurrentData())
				assert.NoError(t, res.Err())
			})

			t.Run("query stats are updated", func(t *testing.T) {
				res.index = 0
				res.pageToken = &mockToken
				mockService := new(mockResultService)
				mockService.On("fetchPage", mock.Anything, mock.Anything, mock.Anything).Return(&fetchPageResultWithStats, nil)
				res.communicator = mockService

				// Default page
				assert.True(t, res.Next(&transactionExecutor{nil, nil}))
				assert.Equal(t, int64(0), *res.ioUsage.GetReadIOs())
				assert.Equal(t, int64(0), *res.ioUsage.getWriteIOs())
				assert.Equal(t, int64(0), *res.timingInfo.GetProcessingTimeMilliseconds())

				// Fetched page
				assert.True(t, res.Next(&transactionExecutor{nil, nil}))
				assert.Equal(t, readIOs, *res.ioUsage.GetReadIOs())
				assert.Equal(t, writeIOs, *res.ioUsage.getWriteIOs())
				assert.Equal(t, processingTimeMilliseconds, *res.timingInfo.GetProcessingTimeMilliseconds())
			})

			t.Run("fail", func(t *testing.T) {
				res.index = 0
				res.pageToken = &mockToken
				res.pageValues = mockPageValues
				mockService := new(mockResultService)
				mockService.On("fetchPage", mock.Anything, mock.Anything, mock.Anything).Return(&fetchPageResult, errMock)
				res.communicator = mockService

				// Default page
				assert.True(t, res.Next(&transactionExecutor{nil, nil}))
				assert.Equal(t, mockIonBinary, res.GetCurrentData())

				// Fetched page
				assert.False(t, res.Next(&transactionExecutor{nil, nil}))
				assert.Nil(t, res.GetCurrentData())
				assert.Equal(t, errMock, res.Err())
			})
		})
	})

	t.Run("updateMetrics", func(t *testing.T) {
		t.Run("res does not have metrics and fetch page does not have metrics", func(t *testing.T) {
			res := result{ioUsage: newIOUsage(0, 0), timingInfo: newTimingInformation(0)}
			res.updateMetrics(&fetchPageResult)

			assert.Equal(t, int64(0), *res.GetConsumedIOs().GetReadIOs())
			assert.Equal(t, int64(0), *res.GetConsumedIOs().getWriteIOs())
			assert.Equal(t, int64(0), *res.GetTimingInformation().GetProcessingTimeMilliseconds())
		})

		t.Run("res does not have metrics and fetch page has metrics", func(t *testing.T) {
			result := result{ioUsage: newIOUsage(0, 0), timingInfo: newTimingInformation(0)}
			result.updateMetrics(&fetchPageResultWithStats)

			assert.Equal(t, readIOs, *result.GetConsumedIOs().GetReadIOs())
			assert.Equal(t, writeIOs, *result.GetConsumedIOs().getWriteIOs())
			assert.Equal(t, processingTimeMilliseconds, *result.GetTimingInformation().GetProcessingTimeMilliseconds())
		})

		t.Run("res has metrics and fetch page does not have metrics", func(t *testing.T) {
			result := result{ioUsage: newIOUsage(readIOs, writeIOs), timingInfo: newTimingInformation(processingTimeMilliseconds)}
			result.updateMetrics(&fetchPageResult)

			assert.Equal(t, readIOs, *result.GetConsumedIOs().GetReadIOs())
			assert.Equal(t, writeIOs, *result.GetConsumedIOs().getWriteIOs())
			assert.Equal(t, processingTimeMilliseconds, *result.GetTimingInformation().GetProcessingTimeMilliseconds())
		})

		t.Run("res has metrics and fetch page has metrics", func(t *testing.T) {
			result := result{ioUsage: newIOUsage(readIOs, writeIOs), timingInfo: newTimingInformation(processingTimeMilliseconds)}

			readIOsBeforeUpdate := result.GetConsumedIOs().GetReadIOs()
			writeIOsBeforeUpdate := result.GetConsumedIOs().getWriteIOs()
			processingTimeMillisecondsBeforeUpdate := result.GetTimingInformation().GetProcessingTimeMilliseconds()

			result.updateMetrics(&fetchPageResultWithStats)

			assert.Equal(t, int64(1), *readIOsBeforeUpdate)
			assert.Equal(t, int64(2), *writeIOsBeforeUpdate)
			assert.Equal(t, int64(3), *processingTimeMillisecondsBeforeUpdate)

			assert.Equal(t, int64(2), *result.GetConsumedIOs().GetReadIOs())
			assert.Equal(t, int64(4), *result.GetConsumedIOs().getWriteIOs())
			assert.Equal(t, int64(6), *result.GetTimingInformation().GetProcessingTimeMilliseconds())
		})
	})
}

func TestBufferedResult(t *testing.T) {
	byteSlice1 := make([]byte, 1)
	byteSlice1[0] = 1
	byteSlice2 := make([]byte, 1)
	byteSlice2[0] = 2
	byteSliceSlice := make([][]byte, 2)
	byteSliceSlice[0] = byteSlice1
	byteSliceSlice[1] = byteSlice2

	readIOs := int64(1)
	writeIOs := int64(2)
	processingTimeMilliseconds := int64(3)
	result := bufferedResult{
		values:     byteSliceSlice,
		index:      0,
		ioUsage:    newIOUsage(readIOs, writeIOs),
		timingInfo: newTimingInformation(processingTimeMilliseconds)}

	t.Run("Next", func(t *testing.T) {
		result.index = 0

		assert.True(t, result.Next())
		assert.Equal(t, byteSlice1, result.GetCurrentData())

		assert.True(t, result.Next())
		assert.Equal(t, byteSlice2, result.GetCurrentData())

		// End of slice
		assert.False(t, result.Next())
		assert.Nil(t, result.GetCurrentData())

		assert.Equal(t, processingTimeMilliseconds, *result.GetTimingInformation().GetProcessingTimeMilliseconds())
		assert.Equal(t, readIOs, *result.GetConsumedIOs().GetReadIOs())
		assert.Equal(t, writeIOs, *result.GetConsumedIOs().getWriteIOs())
	})
}

type mockResultService struct {
	mock.Mock
}

func (m *mockResultService) abortTransaction(ctx context.Context) (*types.AbortTransactionResult, error) {
	panic("not used")
}

func (m *mockResultService) commitTransaction(ctx context.Context, txnID *string, commitDigest []byte) (*types.CommitTransactionResult, error) {
	panic("not used")
}

func (m *mockResultService) executeStatement(ctx context.Context, statement *string, parameters []types.ValueHolder, txnID *string) (*types.ExecuteStatementResult, error) {
	panic("not used")
}

func (m *mockResultService) endSession(ctx context.Context) (*types.EndSessionResult, error) {
	panic("not used")
}

func (m *mockResultService) fetchPage(ctx context.Context, pageToken *string, txnID *string) (*types.FetchPageResult, error) {
	args := m.Called(ctx, pageToken, txnID)
	return args.Get(0).(*types.FetchPageResult), args.Error(1)
}

func (m *mockResultService) startTransaction(ctx context.Context) (*types.StartTransactionResult, error) {
	panic("not used")
}

func generateQldbsessionIOUsage(readIOs int64, writeIOs int64) *types.IOUsage {
	return &types.IOUsage{
		ReadIOs:  readIOs,
		WriteIOs: writeIOs,
	}
}

func generateQldbsessionTimingInformation(processingTimeMilliseconds int64) *types.TimingInformation {
	return &types.TimingInformation{
		ProcessingTimeMilliseconds: processingTimeMilliseconds,
	}
}
