/*
 Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

 Licensed under the Apache License, Version 2.0 (the "License").
 You may not use this file except in compliance with the License.
 A copy of the License is located at

 http://www.apache.org/licenses/LICENSE-2.0

 or in the "license" file accompanying this file. This file is distributed
 on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 express or implied. See the License for the specific language governing
 permissions and limitations under the License.
*/

package qldbdriver

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/qldb"
	"github.com/aws/aws-sdk-go-v2/service/qldb/types"
	"github.com/aws/aws-sdk-go-v2/service/qldbsession"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
)

type testBase struct {
	qldb       *qldb.Client
	ledgerName *string
	regionName *string
	logger     Logger
}

const (
	ledger                 = "Gotest"
	region                 = "us-east-1"
	testTableName          = "GoIntegrationTestTable"
	indexAttribute         = "Name"
	columnName             = "Name"
	singleDocumentValue    = "SingleDocumentValue"
	multipleDocumentValue1 = "MultipleDocumentValue1"
	multipleDocumentValue2 = "MultipleDocumentValue2"
)

func createTestBase() *testBase {

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}
	client := qldb.NewFromConfig(cfg, func(options *qldb.Options) {
		options.Region = region
	})
	logger := defaultLogger{}
	ledgerName := ledger
	regionName := region
	return &testBase{client, &ledgerName, &regionName, logger}
}

func (testBase *testBase) createLedger(t *testing.T) {
	testBase.logger.Log(fmt.Sprint("Creating ledger named ", *testBase.ledgerName, " ..."), LogInfo)
	deletionProtection := false
	permissions := types.PermissionsModeAllowAll
	_, err := testBase.qldb.CreateLedger(context.TODO(), &qldb.CreateLedgerInput{Name: testBase.ledgerName, DeletionProtection: &deletionProtection, PermissionsMode: permissions})
	assert.NoError(t, err)
	testBase.waitForActive()
}

func (testBase *testBase) deleteLedger(t *testing.T) {
	testBase.logger.Log(fmt.Sprint("Deleting ledger ", *testBase.ledgerName), LogInfo)
	deletionProtection := false
	_, _ = testBase.qldb.UpdateLedger(context.TODO(), &qldb.UpdateLedgerInput{DeletionProtection: &deletionProtection, Name: testBase.ledgerName})
	_, err := testBase.qldb.DeleteLedger(context.TODO(), &qldb.DeleteLedgerInput{Name: testBase.ledgerName})
	if err != nil {
		var rnf *types.ResourceNotFoundException
		if errors.As(err, &rnf) {
			testBase.logger.Log("Encountered resource not found", LogInfo)
			return
		}
		testBase.logger.Log("Encountered error during deletion", LogInfo)
		testBase.logger.Log(err.Error(), LogInfo)
		t.Errorf("Failing test due to deletion failure")
		assert.NoError(t, err)
		return
	}
	testBase.waitForDeletion()
}

func (testBase *testBase) waitForActive() {
	testBase.logger.Log("Waiting for ledger to become active...", LogInfo)
	for {
		output, _ := testBase.qldb.DescribeLedger(context.TODO(), &qldb.DescribeLedgerInput{Name: testBase.ledgerName})
		if output.State == "ACTIVE" {
			testBase.logger.Log("Success. Ledger is active and ready to use.", LogInfo)
			return
		}
		testBase.logger.Log("The ledger is still creating. Please wait...", LogInfo)
		time.Sleep(5 * time.Second)
	}
}

func (testBase *testBase) waitForDeletion() {
	testBase.logger.Log("Waiting for ledger to be deleted...", LogInfo)
	for {
		_, err := testBase.qldb.DescribeLedger(context.TODO(), &qldb.DescribeLedgerInput{Name: testBase.ledgerName})
		testBase.logger.Log("The ledger is still deleting. Please wait...", LogInfo)
		if err != nil {
			var rnf *types.ResourceNotFoundException
			if errors.As(err, &rnf) {
				testBase.logger.Log("The ledger is deleted", LogInfo)
				return
			}
		}
		time.Sleep(5 * time.Second)
	}
}

func (testBase *testBase) getDriver(ledgerName string, maxConcurrentTransactions int, retryLimit int) (*QLDBDriver, error) {

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}
	qldbSession := qldbsession.NewFromConfig(cfg, func(options *qldbsession.Options) {
		options.Region = region
	})

	return New(ledgerName, qldbSession, func(options *DriverOptions) {
		options.LoggerVerbosity = LogInfo
		options.MaxConcurrentTransactions = maxConcurrentTransactions
		options.RetryPolicy.MaxRetryLimit = retryLimit
	})
}

var ErrCodeInvalidSessionException = "InvalidSessionException"
var ErrMessageInvalidSessionException = "Invalid session"
var ErrCodeInvalidSessionException2 = "Transaction 23EA3C089B23423D has expired"
var ErrMessageOccConflictException = "OCC"
var ErrCodeBadRequestException = "BadRequestException"
var ErrMessageBadRequestException = "Bad request"
var ErrCodeInternalFailure = "InternalFailure"
var ErrMessageInternalFailure = "Five Hundred"
var ErrMessageCapacityExceedException = "Capacity Exceeded"

// InternalFailure is used to mock 500s exception in tests
type InternalFailure struct {
	Message *string
	Code    *string
}

func (e *InternalFailure) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *InternalFailure) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *InternalFailure) ErrorCode() string             { return "InternalFailure" }
func (e *InternalFailure) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }
