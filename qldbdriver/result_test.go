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
	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
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

	// Mock the AWS SDK
	testCommunicator := communicator{
		service:      nil,
		sessionToken: &mockSessionToken,
		logger:       mockLogger,
	}

	result := &Result{
		ctx:          nil,
		communicator: &testCommunicator,
		txnId:        nil,
		pageValues:   mockPageValues,
		pageToken:    nil,
		index:        0,
		logger:       nil,
	}

	t.Run("HasNext", func(t *testing.T) {
		t.Run("pageToken is nil", func(t *testing.T) {
			result.index = 0
			result.pageToken = nil

			assert.True(t, result.HasNext())
			// Consume first value
			result.Next(&transactionExecutor{nil, nil})
			// No second value or page to fetch
			assert.False(t, result.HasNext())
		})

		t.Run("pageToken present", func(t *testing.T) {
			mockToken := "mockToken"
			// Reset index
			result.index = 0
			result.pageToken = &mockToken

			assert.True(t, result.HasNext())
			// Consume first value
			result.Next(&transactionExecutor{nil, nil})
			// No second value but has page to fetch
			assert.True(t, result.HasNext())
		})
	})

	t.Run("Next", func(t *testing.T) {
		t.Run("pageToken is nil", func(t *testing.T) {
			result.index = 0
			result.pageToken = nil

			ionBinary, err := result.Next(&transactionExecutor{nil, nil})
			assert.Nil(t, err)
			assert.Equal(t, mockIonBinary, ionBinary)
			// No more values
			ionBinary, err = result.Next(&transactionExecutor{nil, nil})
			assert.Nil(t, ionBinary)
			assert.Error(t, err)
		})

		t.Run("pageToken present", func(t *testing.T) {
			mockToken := "mockToken"

			mockFetchPageResult := qldbsession.FetchPageResult{Page: &qldbsession.Page{Values: mockNextPageValues}}
			mockFetchPageCommand := qldbsession.SendCommandOutput{FetchPage: &mockFetchPageResult}

			t.Run("success", func(t *testing.T) {
				result.index = 0
				result.pageToken = &mockToken
				mockSession := new(mockQLDBSession)
				mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockFetchPageCommand, nil)
				testCommunicator.service = mockSession

				// Default page
				ionBinary, err := result.Next(&transactionExecutor{nil, nil})
				assert.Nil(t, err)
				assert.Equal(t, mockIonBinary, ionBinary)
				// Fetched page
				ionBinary, err = result.Next(&transactionExecutor{nil, nil})
				assert.Nil(t, err)
				assert.Equal(t, mockNextIonBinary, ionBinary)
				// No more results
				ionBinary, err = result.Next(&transactionExecutor{nil, nil})
				assert.Nil(t, ionBinary)
				assert.Error(t, err)
			})

			t.Run("fail", func(t *testing.T) {
				result.index = 0
				result.pageToken = &mockToken
				result.pageValues = mockPageValues
				mockSession := new(mockQLDBSession)
				mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommand, mockError)
				testCommunicator.service = mockSession

				// Default page
				ionBinary, err := result.Next(&transactionExecutor{nil, nil})
				assert.Nil(t, err)
				assert.Equal(t, mockIonBinary, ionBinary)
				// Fetched page
				ionBinary, err = result.Next(&transactionExecutor{nil, nil})
				assert.Nil(t, ionBinary)
				assert.Equal(t, mockError, err)
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

	t.Run("HasNext", func(t *testing.T) {
		// byteSlice1
		assert.True(t, result.HasNext())
		result.Next()
		// byteSlice2
		assert.True(t, result.HasNext())
		result.Next()
		// End of slice
		assert.False(t, result.HasNext())
	})

	t.Run("Next", func(t *testing.T) {
		result.index = 0

		byteSlice, err := result.Next()
		assert.Nil(t, err)
		assert.Equal(t, byteSlice1, byteSlice)
		byteSlice, err = result.Next()
		assert.Nil(t, err)
		assert.Equal(t, byteSlice2, byteSlice)
		// End of slice
		byteSlice, err = result.Next()
		assert.Nil(t, byteSlice)
		assert.Error(t, err)
	})
}
