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
	"net/http"
	"testing"
	"time"

	"github.com/amzn/ion-go/ion"
	"github.com/aws/aws-sdk-go/aws/awserr"
	sdksession "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/youtube/vitess/go/sync2"
)

func TestNew(t *testing.T) {
	t.Run("panic for 0 max transactions", func(t *testing.T) {
		awsSession := sdksession.Must(sdksession.NewSession())
		qldbSession := qldbsession.New(awsSession)

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("New should have panicked")
			}
		}()

		New(mockLedgerName,
			qldbSession,
			func(options *DriverOptions) {
				options.LoggerVerbosity = LogOff
				options.MaxConcurrentTransactions = 0
			})
	})

	t.Run("New default success", func(t *testing.T) {
		awsSession := sdksession.Must(sdksession.NewSession())
		qldbSession := qldbsession.New(awsSession)
		initialRetries := 4
		qldbSession.Client.Config.MaxRetries = &initialRetries

		createdDriver := New(mockLedgerName,
			qldbSession,
			func(options *DriverOptions) {
				options.LoggerVerbosity = LogOff
			})

		assert.Equal(t, createdDriver.ledgerName, mockLedgerName)
		assert.Equal(t, createdDriver.maxConcurrentTransactions, defaultMaxConcurrentTransactions)
		assert.Equal(t, createdDriver.retryLimit, defaultRetry)
		assert.Equal(t, createdDriver.iseRetryLimit, uint16(defaultMaxConcurrentTransactions+3))
		assert.Equal(t, createdDriver.isClosed, false)
		assert.Equal(t, cap(createdDriver.sessionPool), int(defaultMaxConcurrentTransactions))
		assert.Equal(t, createdDriver.qldbSession, qldbSession)
		assert.Equal(t, 0, *qldbSession.Client.Config.MaxRetries)
	})

	t.Run("Retry limit overflow handled", func(t *testing.T) {
		awsSession := sdksession.Must(sdksession.NewSession())
		qldbSession := qldbsession.New(awsSession)
		initialRetries := 4
		qldbSession.Client.Config.MaxRetries = &initialRetries

		createdDriver := New(mockLedgerName,
			qldbSession,
			func(options *DriverOptions) {
				options.LoggerVerbosity = LogOff
				options.MaxConcurrentTransactions = 65534
			})

		assert.Equal(t, uint16(65534), createdDriver.maxConcurrentTransactions)
		assert.Equal(t, uint16(65535), createdDriver.iseRetryLimit)
	})
}

func TestExecute(t *testing.T) {
	testDriver := QLDBDriver{
		ledgerName:                mockLedgerName,
		qldbSession:               nil,
		retryLimit:                10,
		iseRetryLimit:             13,
		maxConcurrentTransactions: 10,
		logger:                    mockLogger,
		isClosed:                  false,
		semaphore:                 sync2.NewSemaphore(int(10), time.Duration(10)*time.Second),
		sessionPool:               make(chan *session, 10),
	}

	t.Run("panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Execute should have panicked")
			}
			testDriver.isClosed = false
		}()

		testDriver.isClosed = true
		testDriver.Execute(context.Background(), nil)

	})

	t.Run("error", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, mockError)
		testDriver.qldbSession = mockSession

		result, err := testDriver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {

			//Note : We are using a select * without specifying a where condition for the purpose of this test.
			//       However, we do not recommend using such a query in a normal/production context.
			innerResult, innerErr := txn.Execute("SELECT * FROM someTable")
			if innerErr != nil {
				return nil, innerErr
			}
			return innerResult, innerErr
		})
		assert.Equal(t, err, mockError)
		assert.Nil(t, result)

	})

	t.Run("success", func(t *testing.T) {
		mocktables := make([]string, 1)
		mocktables = append(mocktables, "table1")
		mockSession := new(mockQLDBSession)

		mockSendCommandWithTxID.CommitTransaction.CommitDigest = []byte{167, 123, 231, 255, 170, 172, 35, 142, 73, 31, 239, 199, 252, 120, 175, 217, 235, 220, 184, 200, 85, 203, 140, 230, 151, 221, 131, 255, 163, 151, 170, 210}

		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		testDriver.qldbSession = mockSession

		const tableNameQuery string = "SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'"
		type tableName struct {
			Name string `ion:"name"`
		}

		executeResult, err := testDriver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			tableNames := make([]string, 1)
			tableNames = append(tableNames, "table1")
			return tableNames, nil
		})

		assert.Equal(t, mocktables, executeResult.([]string))
		assert.Nil(t, err)

	})
}

func TestExecuteWithRetryPolicy(t *testing.T) {
	testDriver := QLDBDriver{
		ledgerName:                mockLedgerName,
		qldbSession:               nil,
		retryLimit:                10,
		iseRetryLimit:             13,
		maxConcurrentTransactions: 10,
		logger:                    mockLogger,
		isClosed:                  false,
		semaphore:                 sync2.NewSemaphore(int(10), time.Duration(10)*time.Second),
		sessionPool:               make(chan *session, 10),
	}

	t.Run("panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Execute should have panicked")
			}
			testDriver.isClosed = false
		}()

		testDriver.isClosed = true
		testDriver.ExecuteWithRetryPolicy(context.Background(), nil, RetryPolicy{MaxRetryLimit: 4, Backoff: ExponentialBackoffStrategy{SleepBaseInMillis: 10, SleepCapInMillis: 5000}})
	})

	t.Run("error get session", func(t *testing.T) {

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, mockError)
		testDriver.qldbSession = mockSession

		result, err := testDriver.ExecuteWithRetryPolicy(context.Background(), nil,
			RetryPolicy{MaxRetryLimit: 4, Backoff: ExponentialBackoffStrategy{SleepBaseInMillis: 10, SleepCapInMillis: 5000}})

		assert.Nil(t, result)
		assert.Equal(t, err, mockError)

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

		startSession := &qldbsession.StartSessionRequest{LedgerName: &mockLedgerName}
		startSessionRequest := &qldbsession.SendCommandInput{StartSession: startSession}

		startTransaction := &qldbsession.StartTransactionRequest{}
		startTransactionRequest := &qldbsession.SendCommandInput{StartTransaction: startTransaction}
		startTransactionRequest.SetSessionToken(mockDriverSessionToken)

		abortTransaction := &qldbsession.AbortTransactionRequest{}
		abortTransactionRequest := &qldbsession.SendCommandInput{AbortTransaction: abortTransaction}
		abortTransactionRequest.SetSessionToken(mockDriverSessionToken)

		var testOCCerror = awserr.New(qldbsession.ErrCodeOccConflictException, "OCC", nil)

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, startSessionRequest, mock.Anything).Return(&mockSendCommandForSession, nil)
		mockSession.On("SendCommandWithContext", mock.Anything, startTransactionRequest, mock.Anything).Return(&mockSendCommandForSession, testOCCerror)
		mockSession.On("SendCommandWithContext", mock.Anything, abortTransactionRequest, mock.Anything).Return(&mockSendCommandForSession, nil)
		testDriver.qldbSession = mockSession

		testDriver.sessionPool = make(chan *session, 10)
		testDriver.semaphore = sync2.NewSemaphore(int(10), time.Duration(10)*time.Second)

		const tableNameQuery string = "SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'"
		type tableName struct {
			Name string `ion:"name"`
		}

		result, err := testDriver.ExecuteWithRetryPolicy(context.Background(),
			func(txn Transaction) (interface{}, error) {
				tableNames := make([]string, 1)
				tableNames = append(tableNames, "table1")
				return tableNames, nil
			},
			RetryPolicy{MaxRetryLimit: 4, Backoff: ExponentialBackoffStrategy{SleepBaseInMillis: 10, SleepCapInMillis: 5000}})

		assert.Nil(t, result)
		awsErr, ok := err.(awserr.Error)
		assert.True(t, ok)
		assert.Equal(t, testOCCerror, awsErr)
		mockSession.AssertNumberOfCalls(t, "SendCommandWithContext", 14)
	})

	t.Run("success execute without retry", func(t *testing.T) {

		mockSendCommandWithTxID.CommitTransaction.CommitDigest = []byte{167, 123, 231, 255, 170, 172, 35, 142, 73, 31, 239, 199, 252, 120, 175, 217, 235, 220, 184, 200, 85, 203, 140, 230, 151, 221, 131, 255, 163, 151, 170, 210}

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		testDriver.qldbSession = mockSession

		testDriver.sessionPool = make(chan *session, 10)
		testDriver.semaphore = sync2.NewSemaphore(int(10), time.Duration(10)*time.Second)

		const tableNameQuery string = "SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'"
		type tableName struct {
			Name string `ion:"name"`
		}

		result, err := testDriver.ExecuteWithRetryPolicy(context.Background(),
			func(txn Transaction) (interface{}, error) {
				tableNames := make([]string, 1)
				tableNames = append(tableNames, "table1")
				return tableNames, nil
			},
			RetryPolicy{MaxRetryLimit: 4, Backoff: ExponentialBackoffStrategy{SleepBaseInMillis: 10, SleepCapInMillis: 5000}})

		expectedTables := make([]string, 1)
		expectedTables = append(expectedTables, "table1")

		assert.Equal(t, expectedTables, result.([]string))
		assert.Nil(t, err)
	})

	t.Run("success execute with retry on ISE", func(t *testing.T) {

		hash := []byte{167, 123, 231, 255, 170, 172, 35, 142, 73, 31, 239, 199, 252, 120, 175, 217, 235, 220, 184, 200, 85, 203, 140, 230, 151, 221, 131, 255, 163, 151, 170, 210}
		mockSendCommandWithTxID.CommitTransaction.CommitDigest = hash

		startSession := &qldbsession.StartSessionRequest{LedgerName: &mockLedgerName}
		startSessionRequest := &qldbsession.SendCommandInput{StartSession: startSession}

		startTransaction := &qldbsession.StartTransactionRequest{}
		startTransactionRequest := &qldbsession.SendCommandInput{StartTransaction: startTransaction}
		startTransactionRequest.SetSessionToken(mockDriverSessionToken)

		commitTransaction := &qldbsession.CommitTransactionRequest{TransactionId: &mocktxid, CommitDigest: hash}
		commitTransactionRequest := &qldbsession.SendCommandInput{CommitTransaction: commitTransaction}
		commitTransactionRequest.SetSessionToken(mockDriverSessionToken)

		testISE := awserr.New(qldbsession.ErrCodeInvalidSessionException, "Invalid session", nil)

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, startSessionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommandWithContext", mock.Anything, startTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommandWithContext", mock.Anything, commitTransactionRequest, mock.Anything).
			Return(&mockSendCommandWithTxID, testISE).Times(5)
		mockSession.On("SendCommandWithContext", mock.Anything, commitTransactionRequest, mock.Anything).
			Return(&mockSendCommandWithTxID, nil).Once()

		testDriver.qldbSession = mockSession

		testDriver.sessionPool = make(chan *session, 10)
		testDriver.semaphore = sync2.NewSemaphore(int(10), time.Duration(10)*time.Second)

		type tableName struct {
			Name string `ion:"name"`
		}

		result, err := testDriver.ExecuteWithRetryPolicy(context.Background(),
			func(txn Transaction) (interface{}, error) {
				tableNames := make([]string, 1)
				tableNames = append(tableNames, "table1")
				return tableNames, nil
			},
			RetryPolicy{MaxRetryLimit: 4, Backoff: ExponentialBackoffStrategy{SleepBaseInMillis: 10, SleepCapInMillis: 5000}})

		expectedTables := make([]string, 1)
		expectedTables = append(expectedTables, "table1")

		assert.Equal(t, expectedTables, result.([]string))
		assert.Nil(t, err)
	})

	t.Run("ISE returned when exceed ISE retry limit", func(t *testing.T) {

		hash := []byte{167, 123, 231, 255, 170, 172, 35, 142, 73, 31, 239, 199, 252, 120, 175, 217, 235, 220, 184, 200, 85, 203, 140, 230, 151, 221, 131, 255, 163, 151, 170, 210}
		mockSendCommandWithTxID.CommitTransaction.CommitDigest = hash

		startSession := &qldbsession.StartSessionRequest{LedgerName: &mockLedgerName}
		startSessionRequest := &qldbsession.SendCommandInput{StartSession: startSession}

		startTransaction := &qldbsession.StartTransactionRequest{}
		startTransactionRequest := &qldbsession.SendCommandInput{StartTransaction: startTransaction}
		startTransactionRequest.SetSessionToken(mockDriverSessionToken)

		commitTransaction := &qldbsession.CommitTransactionRequest{TransactionId: &mocktxid, CommitDigest: hash}
		commitTransactionRequest := &qldbsession.SendCommandInput{CommitTransaction: commitTransaction}
		commitTransactionRequest.SetSessionToken(mockDriverSessionToken)

		testISE := awserr.New(qldbsession.ErrCodeInvalidSessionException, "Invalid session", nil)

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, startSessionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommandWithContext", mock.Anything, startTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommandWithContext", mock.Anything, commitTransactionRequest, mock.Anything).
			Return(&mockSendCommandWithTxID, testISE)

		testDriver.qldbSession = mockSession

		testDriver.sessionPool = make(chan *session, 10)
		testDriver.semaphore = sync2.NewSemaphore(int(10), time.Duration(10)*time.Second)

		type tableName struct {
			Name string `ion:"name"`
		}

		result, err := testDriver.ExecuteWithRetryPolicy(context.Background(),
			func(txn Transaction) (interface{}, error) {
				tableNames := make([]string, 1)
				tableNames = append(tableNames, "table1")
				return tableNames, nil
			},
			RetryPolicy{MaxRetryLimit: 4, Backoff: ExponentialBackoffStrategy{SleepBaseInMillis: 10, SleepCapInMillis: 5000}})

		assert.Nil(t, result)
		assert.NotNil(t, err)

		awsErr, ok := err.(awserr.Error)
		assert.True(t, ok)
		assert.Equal(t, testISE, awsErr)
	})

	t.Run("error on transaction expiry.", func(t *testing.T) {

		hash := []byte{167, 123, 231, 255, 170, 172, 35, 142, 73, 31, 239, 199, 252, 120, 175, 217, 235, 220, 184, 200, 85, 203, 140, 230, 151, 221, 131, 255, 163, 151, 170, 210}
		mockSendCommandWithTxID.CommitTransaction.CommitDigest = hash

		startSession := &qldbsession.StartSessionRequest{LedgerName: &mockLedgerName}
		startSessionRequest := &qldbsession.SendCommandInput{StartSession: startSession}

		startTransaction := &qldbsession.StartTransactionRequest{}
		startTransactionRequest := &qldbsession.SendCommandInput{StartTransaction: startTransaction}
		startTransactionRequest.SetSessionToken(mockDriverSessionToken)

		commitTransaction := &qldbsession.CommitTransactionRequest{TransactionId: &mocktxid, CommitDigest: hash}
		commitTransactionRequest := &qldbsession.SendCommandInput{CommitTransaction: commitTransaction}
		commitTransactionRequest.SetSessionToken(mockDriverSessionToken)

		testTxnExpire := awserr.New(qldbsession.ErrCodeInvalidSessionException, "Transaction 23EA3C089B23423D has expired", nil)

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, startSessionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommandWithContext", mock.Anything, startTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommandWithContext", mock.Anything, commitTransactionRequest, mock.Anything).
			Return(&mockSendCommandWithTxID, testTxnExpire).Once()

		testDriver.qldbSession = mockSession

		testDriver.sessionPool = make(chan *session, 10)
		testDriver.semaphore = sync2.NewSemaphore(int(10), time.Duration(10)*time.Second)

		type tableName struct {
			Name string `ion:"name"`
		}

		result, err := testDriver.ExecuteWithRetryPolicy(context.Background(),
			func(txn Transaction) (interface{}, error) {
				tableNames := make([]string, 1)
				tableNames = append(tableNames, "table1")
				return tableNames, nil
			},
			RetryPolicy{MaxRetryLimit: 4, Backoff: ExponentialBackoffStrategy{SleepBaseInMillis: 10, SleepCapInMillis: 5000}})

		assert.Nil(t, result)
		assert.NotNil(t, err)

		awsErr, ok := err.(awserr.Error)
		assert.True(t, ok)
		assert.Equal(t, testTxnExpire, awsErr)
	})

	t.Run("abort transaction on customer error", func(t *testing.T) {

		hash := []byte{167, 123, 231, 255, 170, 172, 35, 142, 73, 31, 239, 199, 252, 120, 175, 217, 235, 220, 184, 200, 85, 203, 140, 230, 151, 221, 131, 255, 163, 151, 170, 210}
		mockSendCommandWithTxID.CommitTransaction.CommitDigest = hash

		startSession := &qldbsession.StartSessionRequest{LedgerName: &mockLedgerName}
		startSessionRequest := &qldbsession.SendCommandInput{StartSession: startSession}

		startTransaction := &qldbsession.StartTransactionRequest{}
		startTransactionRequest := &qldbsession.SendCommandInput{StartTransaction: startTransaction}
		startTransactionRequest.SetSessionToken(mockDriverSessionToken)

		commitTransaction := &qldbsession.CommitTransactionRequest{TransactionId: &mocktxid, CommitDigest: hash}
		commitTransactionRequest := &qldbsession.SendCommandInput{CommitTransaction: commitTransaction}
		commitTransactionRequest.SetSessionToken(mockDriverSessionToken)

		abortTransaction := &qldbsession.AbortTransactionRequest{}
		abortTransactionRequest := &qldbsession.SendCommandInput{AbortTransaction: abortTransaction}
		abortTransactionRequest.SetSessionToken(mockDriverSessionToken)

		customerErr := errors.New("customer error")

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, startSessionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommandWithContext", mock.Anything, startTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		mockSession.On("SendCommandWithContext", mock.Anything, abortTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil).Once()

		testDriver.qldbSession = mockSession

		testDriver.sessionPool = make(chan *session, 10)
		testDriver.semaphore = sync2.NewSemaphore(int(10), time.Duration(10)*time.Second)

		result, err := testDriver.ExecuteWithRetryPolicy(context.Background(),
			func(txn Transaction) (interface{}, error) {
				return nil, customerErr
			},
			RetryPolicy{MaxRetryLimit: 4, Backoff: ExponentialBackoffStrategy{SleepBaseInMillis: 10, SleepCapInMillis: 5000}})

		assert.Nil(t, result)
		assert.NotNil(t, err)
		assert.Equal(t, customerErr, err)

		mockSession.AssertNumberOfCalls(t, "SendCommandWithContext", 3)
	})

	t.Run("success execute with retry on ISE and 500", func(t *testing.T) {

		hash := []byte{167, 123, 231, 255, 170, 172, 35, 142, 73, 31, 239, 199, 252, 120, 175, 217, 235, 220, 184, 200, 85, 203, 140, 230, 151, 221, 131, 255, 163, 151, 170, 210}
		mockSendCommandWithTxID.CommitTransaction.CommitDigest = hash

		startSession := &qldbsession.StartSessionRequest{LedgerName: &mockLedgerName}
		startSessionRequest := &qldbsession.SendCommandInput{StartSession: startSession}

		startTransaction := &qldbsession.StartTransactionRequest{}
		startTransactionRequest := &qldbsession.SendCommandInput{StartTransaction: startTransaction}
		startTransactionRequest.SetSessionToken(mockDriverSessionToken)

		commitTransaction := &qldbsession.CommitTransactionRequest{TransactionId: &mocktxid, CommitDigest: hash}
		commitTransactionRequest := &qldbsession.SendCommandInput{CommitTransaction: commitTransaction}

		testISE := awserr.New(qldbsession.ErrCodeInvalidSessionException, "Invalid session", nil)
		test500error := awserr.New(http.StatusText(http.StatusInternalServerError), "Five Hundred", nil)

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, startSessionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, startTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, commitTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, testISE).Once()

		mockSession.On("SendCommandWithContext", mock.Anything, startSessionRequest, mock.Anything).Return(&mockSendCommandWithTxID, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, startTransactionRequest, mock.Anything).Return(&mockSendCommandWithTxID, test500error).Once()

		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommandWithTxID, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommandWithTxID, nil).Once()
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommandWithTxID, nil).Once()

		testDriver.qldbSession = mockSession

		testDriver.sessionPool = make(chan *session, 10)
		testDriver.semaphore = sync2.NewSemaphore(int(10), time.Duration(10)*time.Second)

		const tableNameQuery string = "SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'"
		type tableName struct {
			Name string `ion:"name"`
		}

		result, err := testDriver.ExecuteWithRetryPolicy(context.Background(),
			func(txn Transaction) (interface{}, error) {
				tableNames := make([]string, 1)
				tableNames = append(tableNames, "table1")
				return tableNames, nil
			},
			RetryPolicy{MaxRetryLimit: 4, Backoff: ExponentialBackoffStrategy{SleepBaseInMillis: 10, SleepCapInMillis: 5000}})

		expectedTables := make([]string, 1)
		expectedTables = append(expectedTables, "table1")

		assert.Equal(t, expectedTables, result.([]string))
		assert.Nil(t, err)
	})
}
func TestGetTableNames(t *testing.T) {
	testDriver := QLDBDriver{
		ledgerName:                mockLedgerName,
		qldbSession:               nil,
		retryLimit:                10,
		iseRetryLimit:             13,
		maxConcurrentTransactions: 10,
		logger:                    mockLogger,
		isClosed:                  false,
		semaphore:                 sync2.NewSemaphore(int(10), time.Duration(10)*time.Second),
		sessionPool:               make(chan *session, 10),
	}

	t.Run("panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Execute should have panicked")
			}
			testDriver.isClosed = false
		}()

		testDriver.isClosed = true
		testDriver.GetTableNames(context.Background())
	})

	t.Run("error on Execute", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, mockError)
		testDriver.qldbSession = mockSession

		result, err := testDriver.GetTableNames(context.Background())

		assert.Nil(t, result)
		assert.Equal(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {

		type tableName struct {
			Name string `ion:"name"`
		}

		ionstruct := &tableName{"table1"}
		tablebinary, _ := ion.MarshalBinary(&ionstruct)

		mockValueHolder := &qldbsession.ValueHolder{IonBinary: tablebinary}
		mockPageValues := make([]*qldbsession.ValueHolder, 1)

		mockPageValues[0] = mockValueHolder
		mockExecuteForTable := qldbsession.ExecuteStatementResult{}
		mockExecuteForTable.SetFirstPage(&qldbsession.Page{Values: mockPageValues})

		mockSendCommandWithTxID.ExecuteStatement = &mockExecuteForTable
		mockSendCommandWithTxID.CommitTransaction.CommitDigest = []byte{46, 176, 81, 229, 236, 60, 17, 188, 81, 216, 217, 0, 89, 228, 233, 134, 252, 90, 165, 63, 143, 66, 127, 173, 131, 13, 134, 159, 14, 198, 19, 73}

		expectedTables := make([]string, 0)
		expectedTables = append(expectedTables, "table1")

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockSendCommandWithTxID, nil)
		testDriver.qldbSession = mockSession

		result, err := testDriver.GetTableNames(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, expectedTables, result)
	})
}

func TestCloseDriver(t *testing.T) {
	testDriver := QLDBDriver{
		ledgerName:                mockLedgerName,
		qldbSession:               nil,
		retryLimit:                10,
		iseRetryLimit:             13,
		maxConcurrentTransactions: 10,
		logger:                    mockLogger,
		isClosed:                  false,
		semaphore:                 nil,
		sessionPool:               make(chan *session, 10),
	}

	t.Run("success", func(t *testing.T) {
		testDriver.Close(context.Background())
		assert.Equal(t, testDriver.isClosed, true)
		_, ok := <-testDriver.sessionPool
		assert.Equal(t, ok, false)
	})

}

func TestGetSession(t *testing.T) {
	testDriver := QLDBDriver{
		ledgerName:                mockLedgerName,
		qldbSession:               nil,
		retryLimit:                10,
		iseRetryLimit:             13,
		maxConcurrentTransactions: 10,
		logger:                    mockLogger,
		isClosed:                  false,
		semaphore:                 sync2.NewSemaphore(int(10), 0),
		sessionPool:               make(chan *session, 10),
	}

	t.Run("error", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, mockError)
		testDriver.qldbSession = mockSession

		session, err := testDriver.getSession(context.Background())

		assert.Equal(t, err, mockError)
		assert.Nil(t, session)
	})

	t.Run("success through createSession while empty pool", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, nil)
		testDriver.qldbSession = mockSession

		session, err := testDriver.getSession(context.Background())

		assert.Nil(t, err)
		assert.Equal(t, mockSession, session.communicator.service)
	})

	t.Run("success through existing session", func(t *testing.T) {
		mockSession := new(mockQLDBSession)

		testCommunicator := communicator{
			service:      mockSession,
			sessionToken: &mockDriverSessionToken,
			logger:       mockLogger,
		}

		session1Retry := uint8(4)
		session2Retry := uint8(3)
		session1 := &session{&testCommunicator, session1Retry, mockLogger}
		session2 := &session{&testCommunicator, session2Retry, mockLogger}

		testDriver.sessionPool <- session1
		testDriver.sessionPool <- session2

		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, mockError)

		testDriver.qldbSession = mockSession

		session, err := testDriver.getSession(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, mockSession, session.communicator.service)
		assert.Equal(t, session1Retry, session.retryLimit)
	})

	testDriver.Close(context.Background())
}

func TestSessionPoolCapacity(t *testing.T) {
	t.Run("error when exceed pool limit but succeed after release one session", func(t *testing.T) {
		testDriver := QLDBDriver{
			ledgerName:                mockLedgerName,
			qldbSession:               nil,
			retryLimit:                10,
			iseRetryLimit:             13,
			maxConcurrentTransactions: 2,
			logger:                    mockLogger,
			isClosed:                  false,
			semaphore:                 sync2.NewSemaphore(int(2), 0),
			sessionPool:               make(chan *session, 2),
		}

		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, nil)
		testDriver.qldbSession = mockSession

		session1, err := testDriver.getSession(context.Background())
		assert.Nil(t, err)
		assert.NotNil(t, session1)

		session2, err := testDriver.getSession(context.Background())
		assert.Nil(t, err)
		assert.NotNil(t, session2)

		session3, err := testDriver.getSession(context.Background())
		assert.NotNil(t, err)
		assert.Nil(t, session3)
		qldbErr := err.(*QLDBDriverError)
		assert.NotNil(t, qldbErr)

		testDriver.releaseSession(session1)

		session4, err := testDriver.getSession(context.Background())
		assert.Nil(t, err)
		assert.NotNil(t, session4)

		testDriver.Close(context.Background())
	})
}

func TestCreateSession(t *testing.T) {

	testDriver := QLDBDriver{
		ledgerName:                mockLedgerName,
		qldbSession:               nil,
		retryLimit:                10,
		iseRetryLimit:             13,
		maxConcurrentTransactions: 10,
		logger:                    mockLogger,
		isClosed:                  false,
		semaphore:                 sync2.NewSemaphore(int(10), time.Duration(10)*time.Second),
		sessionPool:               make(chan *session, 10),
	}

	t.Run("error", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, mockError)
		testDriver.qldbSession = mockSession

		testDriver.semaphore.Acquire()
		session, err := createSession(context.Background(), &testDriver)

		assert.Nil(t, session)
		assert.Equal(t, mockError, err)
	})

	t.Run("success", func(t *testing.T) {
		mockSession := new(mockQLDBSession)
		mockSession.On("SendCommandWithContext", mock.Anything, mock.Anything, mock.Anything).Return(&mockDriverSendCommand, nil)
		testDriver.qldbSession = mockSession

		session, err := createSession(context.Background(), &testDriver)

		assert.Nil(t, err)
		assert.Equal(t, mockSession, session.communicator.service)
	})
}

var mockLedgerName = "someLedgerName"
var defaultMaxConcurrentTransactions = uint16(50)
var defaultRetry = uint8(4)
var mocktxid = "12341"
var mockStartTransactionWithID = qldbsession.StartTransactionResult{TransactionId: &mocktxid}

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
var mockDriverStartSession = qldbsession.StartSessionResult{SessionToken: &mockDriverSessionToken}
var mockDriverAbortTransaction = qldbsession.AbortTransactionResult{}
var mockDriverCommitTransaction = qldbsession.CommitTransactionResult{}
var mockDriverExecuteStatement = qldbsession.ExecuteStatementResult{}
var mockDriverEndSession = qldbsession.EndSessionResult{}
var mockDriverFetchPage = qldbsession.FetchPageResult{}
var mockDriverStartTransaction = qldbsession.StartTransactionResult{}
var mockDriverSendCommand = qldbsession.SendCommandOutput{
	AbortTransaction:  &mockDriverAbortTransaction,
	CommitTransaction: &mockDriverCommitTransaction,
	EndSession:        &mockDriverEndSession,
	ExecuteStatement:  &mockDriverExecuteStatement,
	FetchPage:         &mockDriverFetchPage,
	StartSession:      &mockDriverStartSession,
	StartTransaction:  &mockDriverStartTransaction,
}
