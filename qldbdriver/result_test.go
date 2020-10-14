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

			mockFetchPageResult := qldbsession.FetchPageResult{Page: &qldbsession.Page{Values: mockNextPageValues}}

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

			t.Run("fail", func(t *testing.T) {
				result.index = 0
				result.pageToken = &mockToken
				result.pageValues = mockPageValues
				mockService := new(mockResultService)
				mockService.On("fetchPage", mock.Anything, mock.Anything, mock.Anything).Return(&mockFetchPageResult, mockError)
				result.communicator = mockService

				// Default page
				assert.True(t, result.Next(&transactionExecutor{nil, nil}))
				assert.Equal(t, mockIonBinary, result.GetCurrentData())

				// Fetched page
				assert.False(t, result.Next(&transactionExecutor{nil, nil}))
				assert.Nil(t, result.GetCurrentData())
				assert.Equal(t, mockError, result.Err())
			})
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
	result := BufferedResult{values: byteSliceSlice, index: 0}

	t.Run("Next", func(t *testing.T) {
		result.index = 0

		assert.True(t, result.Next())
		assert.Equal(t, byteSlice1, result.GetCurrentData())

		assert.True(t, result.Next())
		assert.Equal(t, byteSlice2, result.GetCurrentData())

		// End of slice
		assert.False(t, result.Next())
		assert.Nil(t, result.GetCurrentData())
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
