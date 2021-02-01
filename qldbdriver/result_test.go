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

func TestResult(t *testing.T) {
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

	result := &Result{
		ctx:          nil,
		communicator: nil,
		txnID:        nil,
		pageValues:   mockPageValues,
		pageToken:    nil,
		index:        0,
		logger:       nil,
	}

	mockReadIOs := int64(1)
	mockWriteIOs := int64(2)
	mockTimingInfo := int64(3)
	qldbsessionTimingInformation := generateQldbsessionTimingInformation(&mockTimingInfo)
	qldbsessionConsumedIOs := generateQldbsessionIOUsage(&mockReadIOs, &mockWriteIOs)
	timingInformation := generateTimingInformation(&mockTimingInfo)
	consumedIOs := generateIOUsage(&mockReadIOs, &mockWriteIOs)

	mockFetchPageResult := qldbsession.FetchPageResult{Page: &qldbsession.Page{Values: mockNextPageValues}}
	mockFetchPageResultWithStats := mockFetchPageResult
	mockFetchPageResultWithStats.TimingInformation = qldbsessionTimingInformation
	mockFetchPageResultWithStats.ConsumedIOs = qldbsessionConsumedIOs

	t.Run("Next", func(t *testing.T) {
		t.Run("pageToken is nil", func(t *testing.T) {
			result.index = 0
			result.pageToken = nil

			assert.True(t, result.Next(&transactionExecutor{nil, nil}))
			assert.Equal(t, mockIonBinary, result.GetCurrentData())

			// No more values
			assert.False(t, result.Next(&transactionExecutor{nil, nil}))
			assert.Nil(t, result.GetCurrentData())
			assert.NoError(t, result.Err())
		})

		t.Run("pageToken present", func(t *testing.T) {
			mockToken := "mockToken"

			t.Run("success", func(t *testing.T) {
				result.index = 0
				result.pageToken = &mockToken
				mockService := new(mockResultService)
				mockService.On("fetchPage", mock.Anything, mock.Anything, mock.Anything).Return(&mockFetchPageResult, nil)
				result.communicator = mockService

				// Default page
				assert.True(t, result.Next(&transactionExecutor{nil, nil}))
				assert.Equal(t, mockIonBinary, result.GetCurrentData())

				// Fetched page
				assert.True(t, result.Next(&transactionExecutor{nil, nil}))
				assert.Equal(t, mockNextIonBinary, result.GetCurrentData())

				// No more results
				assert.False(t, result.Next(&transactionExecutor{nil, nil}))
				assert.Nil(t, result.GetCurrentData())
				assert.NoError(t, result.Err())
			})

			t.Run("query stats are updated", func(t *testing.T) {
				result.index = 0
				result.pageToken = &mockToken
				mockService := new(mockResultService)
				mockService.On("fetchPage", mock.Anything, mock.Anything, mock.Anything).Return(&mockFetchPageResultWithStats, nil)
				result.communicator = mockService

				var mockTimingInformation *TimingInformation = nil
				var mockConsumedIOs *IOUsage = nil

				// Default page
				assert.True(t, result.Next(&transactionExecutor{nil, nil}))
				assert.Equal(t, mockTimingInformation, result.timingInformation)
				assert.Equal(t, mockConsumedIOs, result.consumedIOs)

				// Fetched page
				assert.True(t, result.Next(&transactionExecutor{nil, nil}))
				assert.Equal(t, mockTimingInfo, *result.timingInformation.GetProcessingTimeMilliseconds())
				assert.Equal(t, mockReadIOs, *result.consumedIOs.GetReadIOs())
				assert.Equal(t, mockWriteIOs, *result.consumedIOs.getWriteIOs())

			})

			t.Run("fail", func(t *testing.T) {
				result.index = 0
				result.pageToken = &mockToken
				result.pageValues = mockPageValues
				mockService := new(mockResultService)
				mockService.On("fetchPage", mock.Anything, mock.Anything, mock.Anything).Return(&mockFetchPageResult, errMock)
				result.communicator = mockService

				// Default page
				assert.True(t, result.Next(&transactionExecutor{nil, nil}))
				assert.Equal(t, mockIonBinary, result.GetCurrentData())

				// Fetched page
				assert.False(t, result.Next(&transactionExecutor{nil, nil}))
				assert.Nil(t, result.GetCurrentData())
				assert.Equal(t, errMock, result.Err())
			})
		})
	})

	t.Run("updateMetrics", func(t *testing.T) {
		t.Run("result does not have metrics and fetch page has does not have metrics", func(t *testing.T) {
			result := Result{consumedIOs: nil, timingInformation: nil}
			result.updateMetrics(&mockFetchPageResult)

			var mockTimingInformation *TimingInformation = nil
			var mockConsumedIOs *IOUsage = nil

			assert.Equal(t, mockConsumedIOs, result.GetConsumedIOs())
			assert.Equal(t, mockTimingInformation, result.GetTimingInformation())
		})

		t.Run("result does not have metrics and fetch page has metrics", func(t *testing.T) {
			result := Result{consumedIOs: nil, timingInformation: nil}
			result.updateMetrics(&mockFetchPageResultWithStats)

			assert.Equal(t, mockReadIOs, *result.GetConsumedIOs().GetReadIOs())
			assert.Equal(t, mockWriteIOs, *result.GetConsumedIOs().getWriteIOs())
			assert.Equal(t, mockTimingInfo, *result.GetTimingInformation().GetProcessingTimeMilliseconds())
		})

		t.Run("result has metrics and fetch page does not have metrics", func(t *testing.T) {
			result := Result{consumedIOs: consumedIOs, timingInformation: timingInformation}
			result.updateMetrics(&mockFetchPageResult)

			assert.Equal(t, mockReadIOs, *result.GetConsumedIOs().GetReadIOs())
			assert.Equal(t, mockWriteIOs, *result.GetConsumedIOs().getWriteIOs())
			assert.Equal(t, mockTimingInfo, *result.GetTimingInformation().GetProcessingTimeMilliseconds())
		})

		t.Run("result has metrics and fetch page has metrics", func(t *testing.T) {
			result := Result{consumedIOs: consumedIOs, timingInformation: timingInformation}
			result.updateMetrics(&mockFetchPageResultWithStats)

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

	mockReadIOs := int64(1)
	mockWriteIOs := int64(2)
	mockTimingInfo := int64(3)
	IOUsage := generateIOUsage(&mockReadIOs, &mockWriteIOs)
	TimingInformation := generateTimingInformation(&mockTimingInfo)
	result := BufferedResult{values: byteSliceSlice, index: 0, consumedIOs: IOUsage, timingInformation: TimingInformation}

	t.Run("Next", func(t *testing.T) {
		result.index = 0

		assert.True(t, result.Next())
		assert.Equal(t, byteSlice1, result.GetCurrentData())

		assert.True(t, result.Next())
		assert.Equal(t, byteSlice2, result.GetCurrentData())

		// End of slice
		assert.False(t, result.Next())
		assert.Nil(t, result.GetCurrentData())

		assert.Equal(t, mockTimingInfo, *result.GetTimingInformation().GetProcessingTimeMilliseconds())
		assert.Equal(t, mockReadIOs, *result.GetConsumedIOs().GetReadIOs())
		assert.Equal(t, mockWriteIOs, *result.GetConsumedIOs().getWriteIOs())
	})
}

type mockResultService struct {
	mock.Mock
}

func (m *mockResultService) abortTransaction(ctx context.Context) (*qldbsession.AbortTransactionResult, error) {
	panic("not used")
}

func (m *mockResultService) commitTransaction(ctx context.Context, txnID *string, commitDigest []byte) (*qldbsession.CommitTransactionResult, error) {
	panic("not used")
}

func (m *mockResultService) executeStatement(ctx context.Context, statement *string, parameters []*qldbsession.ValueHolder, txnID *string) (*qldbsession.ExecuteStatementResult, error) {
	panic("not used")
}

func (m *mockResultService) endSession(ctx context.Context) (*qldbsession.EndSessionResult, error) {
	panic("not used")
}

func (m *mockResultService) fetchPage(ctx context.Context, pageToken *string, txnID *string) (*qldbsession.FetchPageResult, error) {
	args := m.Called(ctx, pageToken, txnID)
	return args.Get(0).(*qldbsession.FetchPageResult), args.Error(1)
}

func (m *mockResultService) startTransaction(ctx context.Context) (*qldbsession.StartTransactionResult, error) {
	panic("not used")
}

func generateIOUsage(readIOs *int64, writeIOs *int64) *IOUsage {
	return &IOUsage{
		readIOs:  readIOs,
		writeIOs: writeIOs,
	}
}

func generateTimingInformation(processingTimeMilliseconds *int64) *TimingInformation {
	return &TimingInformation{
		processingTimeMilliseconds: processingTimeMilliseconds,
	}
}

func generateQldbsessionIOUsage(readIOs *int64, writeIOs *int64) *qldbsession.IOUsage {
	return &qldbsession.IOUsage{
		ReadIOs:  readIOs,
		WriteIOs: writeIOs,
	}
}

func generateQldbsessionTimingInformation(processingTimeMilliseconds *int64) *qldbsession.TimingInformation {
	return &qldbsession.TimingInformation{
		ProcessingTimeMilliseconds: processingTimeMilliseconds,
	}
}
