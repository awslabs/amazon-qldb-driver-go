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
	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestTransaction(t *testing.T) {
	// Mock the AWS SDK
	testCommunicator := communicator{
		service:      nil,
		sessionToken: &mockSessionToken,
		logger:       mockLogger,
	}

	mockTxnId := "mockId"

	mockHash1 := make([]byte, 1)
	mockHash1[0] = 0
	mockHash2 := make([]byte, 1)
	mockHash2[0] = 1
	mockCommitTransactionResult := qldbsession.CommitTransactionResult{
		CommitDigest: mockHash1,
	}
	mockCommitTransactionCommand := qldbsession.SendCommandOutput{CommitTransaction: &mockCommitTransactionResult}

	t.Run("execute", func(t *testing.T) {
		// Todo: Implement upon completion of ionhash
	})

	t.Run("commit", func(t *testing.T) {
		testTransaction := &transaction{
			communicator: &testCommunicator,
			id:           &mockTxnId,
			logger:       nil,
			commitHash:   &qldbHash{hash: mockHash1},
		}

		t.Run("success", func(t *testing.T) {
			mockSession := new(mockQLDBSession)
			mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockCommitTransactionCommand, nil)
			testCommunicator.service = mockSession

			assert.Nil(t, testTransaction.commit(context.Background()))
		})

		t.Run("error", func(t *testing.T) {
			mockSession := new(mockQLDBSession)
			mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockCommitTransactionCommand, mockError)
			testCommunicator.service = mockSession

			assert.Equal(t, mockError, testTransaction.commit(context.Background()))
		})

		t.Run("digest mismatch", func(t *testing.T) {
			mockSession := new(mockQLDBSession)
			mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockCommitTransactionCommand, nil)
			testCommunicator.service = mockSession
			mockCommitTransactionResult.CommitDigest = mockHash2

			assert.Error(t, testTransaction.commit(context.Background()))
		})
	})
}

func TestTransactionExecutor(t *testing.T) {
	// Mock the AWS SDK
	testCommunicator := communicator{
		service:      nil,
		sessionToken: &mockSessionToken,
		logger:       mockLogger,
	}

	mockId := "txnId"

	mockNextIonBinary := make([]byte, 1)
	mockNextIonBinary[0] = 2
	mockNextValueHolder := &qldbsession.ValueHolder{IonBinary: mockNextIonBinary}
	mockNextPageValues := make([]*qldbsession.ValueHolder, 1)
	// Has only one value
	mockNextPageValues[0] = mockNextValueHolder
	mockFetchPageResult := qldbsession.FetchPageResult{Page: &qldbsession.Page{Values: mockNextPageValues}}

	mockTransactionExecutorCommand := qldbsession.SendCommandOutput{
		AbortTransaction: &qldbsession.AbortTransactionResult{},
		FetchPage:        &mockFetchPageResult,
	}

	mockTransaction := transaction{
		communicator: &testCommunicator,
		id:           &mockId,
		logger:       mockLogger,
		commitHash:   nil,
	}

	testExecutor := transactionExecutor{
		ctx: context.Background(),
		txn: &mockTransaction,
	}

	t.Run("execute", func(t *testing.T) {
		// Todo: Implement upon completion of ionhash
	})

	t.Run("BufferResult", func(t *testing.T) {
		mockIonBinary := make([]byte, 1)
		mockIonBinary[0] = 1
		mockValueHolder := &qldbsession.ValueHolder{IonBinary: mockIonBinary}
		mockPageValues := make([]*qldbsession.ValueHolder, 1)
		// Has only one value
		mockPageValues[0] = mockValueHolder

		mockPageToken := "mockToken"

		testResult := Result{
			ctx:          context.Background(),
			communicator: &testCommunicator,
			txnId:        &mockId,
			pageValues:   mockPageValues,
			pageToken:    &mockPageToken,
			index:        0,
			logger:       mockLogger,
		}

		t.Run("success", func(t *testing.T) {
			mockSession := new(mockQLDBSession)
			mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockTransactionExecutorCommand, nil)
			testCommunicator.service = mockSession

			bufferedResult, err := testExecutor.BufferResult(&testResult)
			assert.Nil(t, err)
			value, _ := bufferedResult.Next()
			assert.Equal(t, mockIonBinary, value)
			value, _ = bufferedResult.Next()
			assert.Equal(t, mockNextIonBinary, value)
		})

		t.Run("error", func(t *testing.T) {
			mockSession := new(mockQLDBSession)
			mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockTransactionExecutorCommand, mockError)
			testCommunicator.service = mockSession
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
		t.Run("successful", func(t *testing.T) {
			mockSession := new(mockQLDBSession)
			mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockTransactionExecutorCommand, nil)
			testCommunicator.service = mockSession

			abort := testExecutor.Abort()
			assert.Error(t, abort)
			// No error from SDK
			assert.NotEqual(t, mockError, abort)
		})

		t.Run("error", func(t *testing.T) {
			mockSession := new(mockQLDBSession)
			mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockTransactionExecutorCommand, mockError)
			testCommunicator.service = mockSession

			abort := testExecutor.Abort()
			assert.Error(t, abort)
			// Should ignore error from SDK
			assert.NotEqual(t, mockError, abort)
		})
	})
}
