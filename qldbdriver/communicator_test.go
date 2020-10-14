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
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestStartSession(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommand, mockError)
		communicator, err := startSession(context.Background(), "ledgerName", mockSession, mockLogger)

		assert.Equal(t, err, mockError)
		assert.Nil(t, communicator)
	})

	t.Run("success", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommand, nil)
		communicator, err := startSession(context.Background(), "ledgerName", mockSession, mockLogger)
		assert.NoError(t, err)

		assert.Equal(t, communicator.sessionToken, &mockSessionToken)
		assert.NoError(t, err)
	})
}

func TestAbortTransaction(t *testing.T) {
	testCommunicator := communicator{
		service:      nil,
		sessionToken: &mockSessionToken,
		logger:       mockLogger,
	}

	t.Run("error", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommand, mockError)
		testCommunicator.service = mockSession
		result, err := testCommunicator.abortTransaction(context.Background())

		assert.Equal(t, err, mockError)
		assert.Nil(t, result)
	})

	t.Run("success", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommand, nil)
		testCommunicator.service = mockSession
		result, err := testCommunicator.abortTransaction(context.Background())

		assert.Equal(t, result, &mockAbortTransaction)
		assert.NoError(t, err)
	})
}

func TestCommitTransaction(t *testing.T) {
	testCommunicator := communicator{
		service:      nil,
		sessionToken: &mockSessionToken,
		logger:       mockLogger,
	}

	t.Run("error", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommand, mockError)
		testCommunicator.service = mockSession
		result, err := testCommunicator.commitTransaction(context.Background(), nil, nil)

		assert.Equal(t, err, mockError)
		assert.Nil(t, result)
	})

	t.Run("success", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommand, nil)
		testCommunicator.service = mockSession
		result, err := testCommunicator.commitTransaction(context.Background(), nil, nil)

		assert.Equal(t, result, &mockCommitTransaction)
		assert.NoError(t, err)
	})
}

func TestExecuteStatement(t *testing.T) {
	testCommunicator := communicator{
		service:      nil,
		sessionToken: &mockSessionToken,
		logger:       mockLogger,
	}

	t.Run("error", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommand, mockError)
		testCommunicator.service = mockSession
		result, err := testCommunicator.executeStatement(context.Background(), nil, nil, nil)

		assert.Equal(t, err, mockError)
		assert.Nil(t, result)
	})

	t.Run("success", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommand, nil)
		testCommunicator.service = mockSession
		result, err := testCommunicator.executeStatement(context.Background(), nil, nil, nil)

		assert.Equal(t, result, &mockExecuteStatement)
		assert.NoError(t, err)
	})
}

func TestEndSession(t *testing.T) {
	testCommunicator := communicator{
		service:      nil,
		sessionToken: &mockSessionToken,
		logger:       mockLogger,
	}

	t.Run("error", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommand, mockError)
		testCommunicator.service = mockSession
		result, err := testCommunicator.endSession(context.Background())

		assert.Equal(t, err, mockError)
		assert.Nil(t, result)
	})

	t.Run("success", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommand, nil)
		testCommunicator.service = mockSession
		result, err := testCommunicator.endSession(context.Background())

		assert.Equal(t, result, &mockEndSession)
		assert.NoError(t, err)
	})
}

func TestFetchPage(t *testing.T) {
	testCommunicator := communicator{
		service:      nil,
		sessionToken: &mockSessionToken,
		logger:       mockLogger,
	}

	t.Run("error", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommand, mockError)
		testCommunicator.service = mockSession
		result, err := testCommunicator.fetchPage(context.Background(), nil, nil)

		assert.Equal(t, err, mockError)
		assert.Nil(t, result)
	})

	t.Run("success", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommand, nil)
		testCommunicator.service = mockSession
		result, err := testCommunicator.fetchPage(context.Background(), nil, nil)

		assert.Equal(t, result, &mockFetchPage)
		assert.NoError(t, err)
	})
}

func TestStartTransaction(t *testing.T) {
	testCommunicator := communicator{
		service:      nil,
		sessionToken: &mockSessionToken,
		logger:       mockLogger,
	}

	t.Run("error", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommand, mockError)
		testCommunicator.service = mockSession
		result, err := testCommunicator.startTransaction(context.Background())

		assert.Equal(t, err, mockError)
		assert.Nil(t, result)
	})

	t.Run("success", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommand, nil)
		testCommunicator.service = mockSession
		result, err := testCommunicator.startTransaction(context.Background())

		assert.Equal(t, result, &mockStartTransaction)
		assert.NoError(t, err)
	})
}

func TestSendCommand(t *testing.T) {
	testCommunicator := communicator{
		service:      nil,
		sessionToken: &mockSessionToken,
		logger:       mockLogger,
	}
	mockSession := new(mockQLDBSession)
	mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommand, mockError)
	testCommunicator.service = mockSession
	result, err := testCommunicator.sendCommand(context.Background(), &qldbsession.SendCommandInput{})

	assert.Equal(t, result, &mockSendCommand)
	assert.Equal(t, err, mockError)
}

var mockLogger = &qldbLogger{defaultLogger{}, LogOff}
var mockError = errors.New("mock")

var mockSessionToken = "token"
var mockStartSession = qldbsession.StartSessionResult{SessionToken: &mockSessionToken}
var mockAbortTransaction = qldbsession.AbortTransactionResult{}
var mockCommitTransaction = qldbsession.CommitTransactionResult{}
var mockExecuteStatement = qldbsession.ExecuteStatementResult{}
var mockEndSession = qldbsession.EndSessionResult{}
var mockFetchPage = qldbsession.FetchPageResult{}
var mockStartTransaction = qldbsession.StartTransactionResult{}
var mockSendCommand = qldbsession.SendCommandOutput{
	AbortTransaction:  &mockAbortTransaction,
	CommitTransaction: &mockCommitTransaction,
	EndSession:        &mockEndSession,
	ExecuteStatement:  &mockExecuteStatement,
	FetchPage:         &mockFetchPage,
	StartSession:      &mockStartSession,
	StartTransaction:  &mockStartTransaction,
}

type mockQLDBSession struct {
	mock.Mock
}

func (m *mockQLDBSession) SendCommandWithContext(ctx aws.Context, input *qldbsession.SendCommandInput, options ...request.Option) (*qldbsession.SendCommandOutput, error) {
	args := m.Called(ctx, input, options)
	return args.Get(0).(*qldbsession.SendCommandOutput), args.Error(1)
}

func (m *mockQLDBSession) SendCommand(input *qldbsession.SendCommandInput) (*qldbsession.SendCommandOutput, error) {
	panic("unused method")
}

func (m *mockQLDBSession) SendCommandRequest(input *qldbsession.SendCommandInput) (*request.Request, *qldbsession.SendCommandOutput) {
	panic("unused method")
}
