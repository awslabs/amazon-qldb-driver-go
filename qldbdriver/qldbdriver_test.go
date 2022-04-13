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
	"time"

	"github.com/amzn/ion-go/ion"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/qldbsession"
	"github.com/aws/aws-sdk-go-v2/service/qldbsession/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Run("0 max transactions error", func(t *testing.T) {
		cfg, err := config.LoadDefaultConfig(context.TODO())
		require.NoError(t, err)
		qldbSession := qldbsession.NewFromConfig(cfg)

		_, err = New(mockLedgerName,
			qldbSession,
			func(options *DriverOptions) {
				options.LoggerVerbosity = LogOff
				options.MaxConcurrentTransactions = 0
			})
		assert.Error(t, err)
	})

	t.Run("Invalid QLDBSession error", func(t *testing.T) {
		_, err := New(mockLedgerName,
			nil,
			func(options *DriverOptions) {
				options.LoggerVerbosity = LogOff
			})
		assert.Error(t, err)
	})

	t.Run("New default success", func(t *testing.T) {
		cfg, err := config.LoadDefaultConfig(context.TODO())
		require.NoError(t, err)
		qldbSession := qldbsession.NewFromConfig(cfg)

		createdDriver, err := New(mockLedgerName,
			qldbSession,
			func(options *DriverOptions) {
				options.LoggerVerbosity = LogOff
			})
		require.NoError(t, err)

		assert.Equal(t, createdDriver.ledgerName, mockLedgerName)
		assert.Equal(t, createdDriver.maxConcurrentTransactions, defaultMaxConcurrentTransactions)
		assert.Equal(t, createdDriver.retryPolicy.MaxRetryLimit, defaultRetry)
		assert.Equal(t, createdDriver.isClosed, false)
		assert.Equal(t, cap(createdDriver.sessionPool), defaultMaxConcurrentTransactions)

		driverQldbSession := createdDriver.qldbSession

		assert.Equal(t, qldbSession, driverQldbSession)
	})

	t.Run("Retry limit overflow handled", func(t *testing.T) {
		cfg, err := config.LoadDefaultConfig(context.TODO())
		require.NoError(t, err)
		qldbSession := qldbsession.NewFromConfig(cfg)

		createdDriver, err := New(mockLedgerName,
			qldbSession,
			func(options *DriverOptions) {
				options.LoggerVerbosity = LogOff
				options.MaxConcurrentTransactions = 65534
			})
		require.NoError(t, err)
		assert.Equal(t, 65534, createdDriver.maxConcurrentTransactions)
	})

	t.Run("Protected against QLDBSession mutation", func(t *testing.T) {
		cfg, err := config.LoadDefaultConfig(context.TODO())
		require.NoError(t, err)
		qldbSession := qldbsession.NewFromConfig(cfg)

		createdDriver, err := New(mockLedgerName,
			qldbSession,
			func(options *DriverOptions) {
				options.LoggerVerbosity = LogOff
			})
		require.NoError(t, err)

		driverQldbSession := createdDriver.qldbSession

		qldbSession = nil
		assert.NotNil(t, driverQldbSession)
	})
}

func TestExecute(t *testing.T) {
	testDriver := QLDBDriver{
		ledgerName:                mockLedgerName,
		qldbSession:               nil,
		maxConcurrentTransactions: 10,
		logger:                    mockLogger,
		isClosed:                  false,
		semaphore:                 makeSemaphore(10),
		sessionPool:               make(chan *session, 10),
		retryPolicy: RetryPolicy{
			MaxRetryLimit: 4,
			Backoff: ExponentialBackoffStrategy{
				SleepBase: time.Duration(10) * time.Millisecond,
				SleepCap:  time.Duration(5000) * time.Millisecond}},
	}

	t.Run("Execute with closed driver error", func(t *testing.T) {
		testDriver.isClosed = true

		_, err := testDriver.Execute(context.Background(), nil)
		assert.Error(t, err)

		testDriver.isClosed = false
	})

	t.Run("error", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, errMock)
		testDriver.qldbSession = mockSession

		result, err := testDriver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			// Note : We are using a select * without specifying a where condition for the purpose of this test.
			//        However, we do not recommend using such a query in a normal/production context.
			innerResult, innerErr := txn.Execute("SELECT * FROM someTable")
			if innerErr != nil {
				return nil, innerErr
			}
			return innerResult, innerErr
		})
		assert.Equal(t, err, errMock)
		assert.Nil(t, result)
	})

	t.Run("success", func(t *testing.T) {
		mockTables := make([]string, 1)
		mockTables = append(mockTables, "table1")
		mockSession := new(mockQLDBSession)

		mockSendCommandWithTxID.CommitTransaction.CommitDigest = []byte{167, 123, 231, 255, 170, 172, 35, 142, 73, 31, 239, 199, 252, 120, 175, 217, 235, 220, 184, 200, 85, 203, 140, 230, 151, 221, 131, 255, 163, 151, 170, 210}

		mockSession.On("SendCommand", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		testDriver.qldbSession = mockSession

		executeResult, err := testDriver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			tableNames := make([]string, 1)
			tableNames = append(tableNames, "table1")
			return tableNames, nil
		})

		assert.Equal(t, mockTables, executeResult.([]string))
		assert.Nil(t, err)
	})

	t.Run("error get session", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, errMock)
		testDriver.qldbSession = mockSession
		testDriver.sessionPool = make(chan *session, 10)

		result, err := testDriver.Execute(context.Background(), nil)

		assert.Nil(t, result)
		assert.Equal(t, err, errMock)
	})

	t.Run("error session execute", func(t *testing.T) {
		mockSendCommandForSession := qldbsession.SendCommandOutput{
			AbortTransaction:  &mockAbortTransaction,
			CommitTransaction: &mockCommitTransaction,
			EndSession:        &mockEndSession,
			ExecuteStatement:  &mockExecuteStatement,
			FetchPage:         &mockFetchPage,
			StartSession:      &mockStartSession,
			StartTransaction:  &mockStartTransactionWithID,
		}

		startSession := &types.StartSessionRequest{LedgerName: &mockLedgerName}
		startSessionRequest := &qldbsession.SendCommandInput{StartSession: startSession}

		startTransaction := &types.StartTransactionRequest{}
		startTransactionRequest := &qldbsession.SendCommandInput{StartTransaction: startTransaction}
		startTransactionRequest.SessionToken = &mockDriverSessionToken

		abortTransaction := &types.AbortTransactionRequest{}
		abortTransactionRequest := &qldbsession.SendCommandInput{AbortTransaction: abortTransaction}
		abortTransactionRequest.SessionToken = &mockDriverSessionToken

		testOCCError := &types.OccConflictException{Message: &ErrMessageOccConflictException}

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, startSessionRequest, mock.Anything).Return(&mockSendCommandForSession, nil)
		mockSession.On("SendCommand", mock.Anything, startTransactionRequest, mock.Anything).Return(&mockSendCommandForSession, testOCCError)
		mockSession.On("SendCommand", mock.Anything, abortTransactionRequest, mock.Anything).Return(&mockSendCommandForSession, nil)
		testDriver.qldbSession = mockSession

		testDriver.sessionPool = make(chan *session, 10)
		testDriver.semaphore = makeSemaphore(10)

		result, err := testDriver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			tableNames := make([]string, 1)
			tableNames = append(tableNames, "table1")
			return tableNames, nil
		})

		assert.Nil(t, result)

		var occ *types.OccConflictException
		assert.True(t, errors.As(err, &occ))
		assert.Equal(t, testOCCError, err)
		mockSession.AssertNumberOfCalls(t, "SendCommand", 6)
	})

	t.Run("success execute without retry", func(t *testing.T) {
		mockSendCommandWithTxID.CommitTransaction.CommitDigest = []byte{167, 123, 231, 255, 170, 172, 35, 142, 73, 31, 239, 199, 252, 120, 175, 217, 235, 220, 184, 200, 85, 203, 140, 230, 151, 221, 131, 255, 163, 151, 170, 210}

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		testDriver.qldbSession = mockSession

		testDriver.sessionPool = make(chan *session, 10)
		testDriver.semaphore = makeSemaphore(10)

		result, err := testDriver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			tableNames := make([]string, 1)
			tableNames = append(tableNames, "table1")
			return tableNames, nil
		})

		expectedTables := make([]string, 1)
		expectedTables = append(expectedTables, "table1")

		assert.Equal(t, expectedTables, result.([]string))
		assert.NoError(t, err)
	})

	t.Run("success execute with retry on ISE", func(t *testing.T) {
		hash := []byte{167, 123, 231, 255, 170, 172, 35, 142, 73, 31, 239, 199, 252, 120, 175, 217, 235, 220, 184, 200, 85, 203, 140, 230, 151, 221, 131, 255, 163, 151, 170, 210}
		mockSendCommandWithTxID.CommitTransaction.CommitDigest = hash

		startSession := &types.StartSessionRequest{LedgerName: &mockLedgerName}
		startSessionRequest := &qldbsession.SendCommandInput{StartSession: startSession}

		startTransaction := &types.StartTransactionRequest{}
		startTransactionRequest := &qldbsession.SendCommandInput{StartTransaction: startTransaction}
		startTransactionRequest.SessionToken = &mockDriverSessionToken

		commitTransaction := &types.CommitTransactionRequest{TransactionId: &mockTxnID, CommitDigest: hash}
		commitTransactionRequest := &qldbsession.SendCommandInput{CommitTransaction: commitTransaction}
		commitTransactionRequest.SessionToken = &mockDriverSessionToken

		testISE := &types.InvalidSessionException{Code: &ErrCodeInvalidSessionException, Message: &ErrMessageInvalidSessionException}

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, startSessionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommand", mock.Anything, startTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommand", mock.Anything, commitTransactionRequest, mock.Anything).
			Return(&mockSendCommandWithTxID, testISE).Times(4)
		mockSession.On("SendCommand", mock.Anything, commitTransactionRequest, mock.Anything).
			Return(&mockSendCommandWithTxID, nil).Once()

		testDriver.qldbSession = mockSession

		testDriver.sessionPool = make(chan *session, 10)
		testDriver.semaphore = makeSemaphore(10)

		result, err := testDriver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			tableNames := make([]string, 1)
			tableNames = append(tableNames, "table1")
			return tableNames, nil
		})

		expectedTables := make([]string, 1)
		expectedTables = append(expectedTables, "table1")

		assert.Equal(t, expectedTables, result.([]string))
		assert.NoError(t, err)
	})

	t.Run("ISE returned when exceed ISE retry limit", func(t *testing.T) {
		hash := []byte{167, 123, 231, 255, 170, 172, 35, 142, 73, 31, 239, 199, 252, 120, 175, 217, 235, 220, 184, 200, 85, 203, 140, 230, 151, 221, 131, 255, 163, 151, 170, 210}
		mockSendCommandWithTxID.CommitTransaction.CommitDigest = hash

		startSession := &types.StartSessionRequest{LedgerName: &mockLedgerName}
		startSessionRequest := &qldbsession.SendCommandInput{StartSession: startSession}

		startTransaction := &types.StartTransactionRequest{}
		startTransactionRequest := &qldbsession.SendCommandInput{StartTransaction: startTransaction}
		startTransactionRequest.SessionToken = &mockDriverSessionToken

		commitTransaction := &types.CommitTransactionRequest{TransactionId: &mockTxnID, CommitDigest: hash}
		commitTransactionRequest := &qldbsession.SendCommandInput{CommitTransaction: commitTransaction}
		commitTransactionRequest.SessionToken = &mockDriverSessionToken

		testISE := &types.InvalidSessionException{Code: &ErrCodeInvalidSessionException, Message: &ErrMessageInvalidSessionException}

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, startSessionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommand", mock.Anything, startTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommand", mock.Anything, commitTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, testISE)

		testDriver.qldbSession = mockSession

		testDriver.sessionPool = make(chan *session, 10)
		testDriver.semaphore = makeSemaphore(10)

		result, err := testDriver.Execute(context.Background(),
			func(txn Transaction) (interface{}, error) {
				tableNames := make([]string, 1)
				tableNames = append(tableNames, "table1")
				return tableNames, nil
			})
		assert.Error(t, err)
		assert.Nil(t, result)

		var ise *types.InvalidSessionException
		assert.True(t, errors.As(err, &ise))
		assert.Equal(t, testISE, err)
	})

	t.Run("CapacityExceededException returned when exceed CapacityExceededException retry limit", func(t *testing.T) {
		hash := []byte{167, 123, 231, 255, 170, 172, 35, 142, 73, 31, 239, 199, 252, 120, 175, 217, 235, 220, 184, 200, 85, 203, 140, 230, 151, 221, 131, 255, 163, 151, 170, 210}
		mockSendCommandWithTxID.CommitTransaction.CommitDigest = hash

		startSession := &types.StartSessionRequest{LedgerName: &mockLedgerName}
		startSessionRequest := &qldbsession.SendCommandInput{StartSession: startSession}

		startTransaction := &types.StartTransactionRequest{}
		startTransactionRequest := &qldbsession.SendCommandInput{StartTransaction: startTransaction}
		startTransactionRequest.SessionToken = &mockDriverSessionToken

		abortTransaction := &types.AbortTransactionRequest{}
		abortTransactionRequest := &qldbsession.SendCommandInput{AbortTransaction: abortTransaction}
		abortTransactionRequest.SessionToken = &mockDriverSessionToken

		commitTransaction := &types.CommitTransactionRequest{TransactionId: &mockTxnID, CommitDigest: hash}
		commitTransactionRequest := &qldbsession.SendCommandInput{CommitTransaction: commitTransaction}
		commitTransactionRequest.SessionToken = &mockDriverSessionToken

		testCEE := &types.CapacityExceededException{Message: &ErrMessageCapacityExceedException}

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, startSessionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommand", mock.Anything, startTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommand", mock.Anything, commitTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, testCEE)
		mockSession.On("SendCommand", mock.Anything, abortTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil).Times(5)

		testDriver.qldbSession = mockSession

		result, err := testDriver.Execute(context.Background(),
			func(txn Transaction) (interface{}, error) {
				return "tableNames", nil
			})
		assert.Error(t, err)
		assert.Nil(t, result)

		var cee *types.CapacityExceededException
		assert.True(t, errors.As(err, &cee))
		assert.Equal(t, testCEE, err)
	})

	t.Run("error on transaction expiry.", func(t *testing.T) {
		hash := []byte{167, 123, 231, 255, 170, 172, 35, 142, 73, 31, 239, 199, 252, 120, 175, 217, 235, 220, 184, 200, 85, 203, 140, 230, 151, 221, 131, 255, 163, 151, 170, 210}
		mockSendCommandWithTxID.CommitTransaction.CommitDigest = hash

		startSession := &types.StartSessionRequest{LedgerName: &mockLedgerName}
		startSessionRequest := &qldbsession.SendCommandInput{StartSession: startSession}

		startTransaction := &types.StartTransactionRequest{}
		startTransactionRequest := &qldbsession.SendCommandInput{StartTransaction: startTransaction}
		startTransactionRequest.SessionToken = &mockDriverSessionToken

		commitTransaction := &types.CommitTransactionRequest{TransactionId: &mockTxnID, CommitDigest: hash}
		commitTransactionRequest := &qldbsession.SendCommandInput{CommitTransaction: commitTransaction}
		commitTransactionRequest.SessionToken = &mockDriverSessionToken

		testTxnExpire := &types.InvalidSessionException{Code: &ErrCodeInvalidSessionException, Message: &ErrCodeInvalidSessionException2}

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, startSessionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommand", mock.Anything, startTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommand", mock.Anything, commitTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, testTxnExpire).Once()

		testDriver.qldbSession = mockSession

		testDriver.sessionPool = make(chan *session, 10)
		testDriver.semaphore = makeSemaphore(10)

		result, err := testDriver.Execute(context.Background(),
			func(txn Transaction) (interface{}, error) {
				tableNames := make([]string, 1)
				tableNames = append(tableNames, "table1")
				return tableNames, nil
			})
		assert.Error(t, err)
		assert.Nil(t, result)

		var ise *types.InvalidSessionException
		assert.True(t, errors.As(err, &ise))
		assert.Equal(t, testTxnExpire, err)
	})

	t.Run("abort transaction on customer error", func(t *testing.T) {
		hash := []byte{167, 123, 231, 255, 170, 172, 35, 142, 73, 31, 239, 199, 252, 120, 175, 217, 235, 220, 184, 200, 85, 203, 140, 230, 151, 221, 131, 255, 163, 151, 170, 210}
		mockSendCommandWithTxID.CommitTransaction.CommitDigest = hash

		startSession := &types.StartSessionRequest{LedgerName: &mockLedgerName}
		startSessionRequest := &qldbsession.SendCommandInput{StartSession: startSession}

		startTransaction := &types.StartTransactionRequest{}
		startTransactionRequest := &qldbsession.SendCommandInput{StartTransaction: startTransaction}
		startTransactionRequest.SessionToken = &mockDriverSessionToken

		commitTransaction := &types.CommitTransactionRequest{TransactionId: &mockTxnID, CommitDigest: hash}
		commitTransactionRequest := &qldbsession.SendCommandInput{CommitTransaction: commitTransaction}
		commitTransactionRequest.SessionToken = &mockDriverSessionToken

		abortTransaction := &types.AbortTransactionRequest{}
		abortTransactionRequest := &qldbsession.SendCommandInput{AbortTransaction: abortTransaction}
		abortTransactionRequest.SessionToken = &mockDriverSessionToken

		customerErr := errors.New("customer error")

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, startSessionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommand", mock.Anything, startTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommand", mock.Anything, abortTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil).Once()

		testDriver.qldbSession = mockSession

		testDriver.sessionPool = make(chan *session, 10)
		testDriver.semaphore = makeSemaphore(10)

		result, err := testDriver.Execute(context.Background(),
			func(txn Transaction) (interface{}, error) {
				return nil, customerErr
			})
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, customerErr, err)

		mockSession.AssertNumberOfCalls(t, "SendCommand", 3)
	})

	t.Run("success execute with retry on ISE and 500", func(t *testing.T) {
		hash := []byte{167, 123, 231, 255, 170, 172, 35, 142, 73, 31, 239, 199, 252, 120, 175, 217, 235, 220, 184, 200, 85, 203, 140, 230, 151, 221, 131, 255, 163, 151, 170, 210}
		mockSendCommandWithTxID.CommitTransaction.CommitDigest = hash

		startSession := &types.StartSessionRequest{LedgerName: &mockLedgerName}
		startSessionRequest := &qldbsession.SendCommandInput{StartSession: startSession}

		startTransaction := &types.StartTransactionRequest{}
		startTransactionRequest := &qldbsession.SendCommandInput{StartTransaction: startTransaction}
		startTransactionRequest.SessionToken = &mockDriverSessionToken

		commitTransaction := &types.CommitTransactionRequest{TransactionId: &mockTxnID, CommitDigest: hash}
		commitTransactionRequest := &qldbsession.SendCommandInput{CommitTransaction: commitTransaction}

		testISE := &types.InvalidSessionException{Code: &ErrCodeInvalidSessionException, Message: &ErrMessageInvalidSessionException}
		test500error := &InternalFailure{Code: &ErrCodeInternalFailure, Message: &ErrMessageInternalFailure}

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, startSessionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil).Once()
		mockSession.On("SendCommand", mock.Anything, startTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil).Once()
		mockSession.On("SendCommand", mock.Anything, commitTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, testISE).Once()

		mockSession.On("SendCommand", mock.Anything, startSessionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil).Once()
		mockSession.On("SendCommand", mock.Anything, startTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, test500error).Once()

		mockSession.On("SendCommand", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommandWithTxID, nil).Once()
		mockSession.On("SendCommand", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommandWithTxID, nil).Once()
		mockSession.On("SendCommand", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommandWithTxID, nil).Once()

		testDriver.qldbSession = mockSession

		testDriver.sessionPool = make(chan *session, 10)
		testDriver.semaphore = makeSemaphore(10)

		result, err := testDriver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			tableNames := make([]string, 1)
			tableNames = append(tableNames, "table1")
			return tableNames, nil
		})

		expectedTables := make([]string, 1)
		expectedTables = append(expectedTables, "table1")

		assert.Equal(t, expectedTables, result.([]string))
		assert.NoError(t, err)
	})
}

func TestGetTableNames(t *testing.T) {
	testDriver := QLDBDriver{
		ledgerName:                mockLedgerName,
		qldbSession:               nil,
		maxConcurrentTransactions: 10,
		logger:                    mockLogger,
		isClosed:                  false,
		semaphore:                 makeSemaphore(10),
		sessionPool:               make(chan *session, 10),
		retryPolicy: RetryPolicy{
			MaxRetryLimit: 10,
			Backoff: ExponentialBackoffStrategy{
				SleepBase: time.Duration(10) * time.Millisecond,
				SleepCap:  time.Duration(5000) * time.Millisecond}},
	}

	t.Run("GetTableNames from closed driver error", func(t *testing.T) {
		testDriver.isClosed = true

		_, err := testDriver.GetTableNames(context.Background())
		assert.Error(t, err)

		testDriver.isClosed = false
	})

	t.Run("error on Execute", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, errMock)
		testDriver.qldbSession = mockSession

		result, err := testDriver.GetTableNames(context.Background())

		assert.Nil(t, result)
		assert.Equal(t, err, errMock)
	})

	t.Run("success", func(t *testing.T) {
		type tableName struct {
			Name string `ion:"name"`
		}

		ionStruct := &tableName{"table1"}
		tableBinary, _ := ion.MarshalBinary(&ionStruct)

		mockValueHolder := types.ValueHolder{IonBinary: tableBinary}
		mockPageValues := make([]types.ValueHolder, 1)

		mockPageValues[0] = mockValueHolder
		mockExecuteForTable := types.ExecuteStatementResult{}
		mockExecuteForTable.FirstPage = &types.Page{Values: mockPageValues}

		mockSendCommandWithTxID.ExecuteStatement = &mockExecuteForTable
		mockSendCommandWithTxID.CommitTransaction.CommitDigest = []byte{46, 176, 81, 229, 236, 60, 17, 188, 81, 216, 217, 0, 89, 228, 233, 134, 252, 90, 165, 63, 143, 66, 127, 173, 131, 13, 134, 159, 14, 198, 19, 73}

		expectedTables := make([]string, 0)
		expectedTables = append(expectedTables, "table1")

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		testDriver.qldbSession = mockSession

		result, err := testDriver.GetTableNames(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, expectedTables, result)
	})
}

func TestShutdownDriver(t *testing.T) {
	testDriver := QLDBDriver{
		ledgerName:                mockLedgerName,
		qldbSession:               nil,
		maxConcurrentTransactions: 10,
		logger:                    mockLogger,
		isClosed:                  false,
		semaphore:                 nil,
		sessionPool:               make(chan *session, 10),
		retryPolicy: RetryPolicy{
			MaxRetryLimit: 10,
			Backoff: ExponentialBackoffStrategy{
				SleepBase: time.Duration(10) * time.Millisecond,
				SleepCap:  time.Duration(5000) * time.Millisecond}},
	}

	t.Run("success", func(t *testing.T) {
		testDriver.Shutdown(context.Background())
		assert.Equal(t, testDriver.isClosed, true)
		_, ok := <-testDriver.sessionPool
		assert.Equal(t, ok, false)
	})

}

func TestGetSession(t *testing.T) {
	testDriver := QLDBDriver{
		ledgerName:                mockLedgerName,
		qldbSession:               nil,
		maxConcurrentTransactions: 10,
		logger:                    mockLogger,
		isClosed:                  false,
		semaphore:                 makeSemaphore(10),
		sessionPool:               make(chan *session, 10),
		retryPolicy: RetryPolicy{
			MaxRetryLimit: 10,
			Backoff: ExponentialBackoffStrategy{
				SleepBase: time.Duration(10) * time.Millisecond,
				SleepCap:  time.Duration(5000) * time.Millisecond}},
	}
	defer testDriver.Shutdown(context.Background())

	t.Run("error", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, errMock)
		testDriver.qldbSession = mockSession

		session, err := testDriver.getSession(context.Background())

		assert.Equal(t, err, errMock)
		assert.Nil(t, session)
	})

	t.Run("success through createSession while empty pool", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, nil)
		testDriver.qldbSession = mockSession

		session, err := testDriver.getSession(context.Background())

		assert.NoError(t, err)
		assert.Equal(t, &mockSessionToken, session.communicator.(*communicator).sessionToken)
	})

	t.Run("success through existing session", func(t *testing.T) {
		mockSession := new(mockQLDBSession)

		testCommunicator := communicator{
			service:      mockSession,
			sessionToken: &mockDriverSessionToken,
			logger:       mockLogger,
		}

		session1 := &session{&testCommunicator, mockLogger}
		session2 := &session{&testCommunicator, mockLogger}

		testDriver.sessionPool <- session1
		testDriver.sessionPool <- session2

		mockSession.On("SendCommand", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, errMock)

		testDriver.qldbSession = mockSession

		session, err := testDriver.getSession(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, &mockSessionToken, session.communicator.(*communicator).sessionToken)
	})
}

func TestSessionPoolCapacity(t *testing.T) {
	t.Run("error when exceed pool limit but succeed after release one session", func(t *testing.T) {
		testDriver := QLDBDriver{
			ledgerName:                mockLedgerName,
			qldbSession:               nil,
			maxConcurrentTransactions: 2,
			logger:                    mockLogger,
			isClosed:                  false,
			semaphore:                 makeSemaphore(2),
			sessionPool:               make(chan *session, 2),
			retryPolicy: RetryPolicy{
				MaxRetryLimit: 10,
				Backoff: ExponentialBackoffStrategy{
					SleepBase: time.Duration(10) * time.Millisecond,
					SleepCap:  time.Duration(5000) * time.Millisecond}},
		}
		defer testDriver.Shutdown(context.Background())

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, nil)
		testDriver.qldbSession = mockSession

		session1, err := testDriver.getSession(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, session1)

		session2, err := testDriver.getSession(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, session2)

		session3, err := testDriver.getSession(context.Background())
		assert.Error(t, err)
		assert.Nil(t, session3)
		qldbErr := err.(*qldbDriverError)
		assert.Error(t, qldbErr)

		testDriver.releaseSession(session1)

		session4, err := testDriver.getSession(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, session4)
	})
}

func TestCreateSession(t *testing.T) {

	testDriver := QLDBDriver{
		ledgerName:                mockLedgerName,
		qldbSession:               nil,
		maxConcurrentTransactions: 10,
		logger:                    mockLogger,
		isClosed:                  false,
		semaphore:                 makeSemaphore(10),
		sessionPool:               make(chan *session, 10),
		retryPolicy: RetryPolicy{
			MaxRetryLimit: 10,
			Backoff: ExponentialBackoffStrategy{
				SleepBase: time.Duration(10) * time.Millisecond,
				SleepCap:  time.Duration(5000) * time.Millisecond}},
	}

	t.Run("error", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, errMock)
		testDriver.qldbSession = mockSession

		testDriver.semaphore.tryAcquire()
		session, err := testDriver.createSession(context.Background())

		assert.Nil(t, session)
		assert.Equal(t, errMock, err)
	})

	t.Run("success", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommand", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, nil)
		testDriver.qldbSession = mockSession

		session, err := testDriver.createSession(context.Background())

		assert.NoError(t, err)
		assert.Equal(t, &mockSessionToken, session.communicator.(*communicator).sessionToken)
	})
}

var mockLedgerName = "someLedgerName"
var defaultMaxConcurrentTransactions = 50
var defaultRetry = 4
var mockTxnID = "12341"
var mockStartTransactionWithID = types.StartTransactionResult{TransactionId: &mockTxnID}

var mockSendCommandWithTxID = qldbsession.SendCommandOutput{
	AbortTransaction:  &mockAbortTransaction,
	CommitTransaction: &mockCommitTransaction,
	EndSession:        &mockEndSession,
	ExecuteStatement:  &mockExecuteStatement,
	FetchPage:         &mockFetchPage,
	StartSession:      &mockStartSession,
	StartTransaction:  &mockStartTransactionWithID,
}

var mockDriverSessionToken = "token"
var mockDriverStartSession = types.StartSessionResult{SessionToken: &mockDriverSessionToken}
var mockDriverAbortTransaction = types.AbortTransactionResult{}
var mockDriverCommitTransaction = types.CommitTransactionResult{}
var mockDriverExecuteStatement = types.ExecuteStatementResult{}
var mockDriverEndSession = types.EndSessionResult{}
var mockDriverFetchPage = types.FetchPageResult{}
var mockDriverStartTransaction = types.StartTransactionResult{}
var mockDriverSendCommand = qldbsession.SendCommandOutput{
	AbortTransaction:  &mockDriverAbortTransaction,
	CommitTransaction: &mockDriverCommitTransaction,
	EndSession:        &mockDriverEndSession,
	ExecuteStatement:  &mockDriverExecuteStatement,
	FetchPage:         &mockDriverFetchPage,
	StartSession:      &mockDriverStartSession,
	StartTransaction:  &mockDriverStartTransaction,
}
