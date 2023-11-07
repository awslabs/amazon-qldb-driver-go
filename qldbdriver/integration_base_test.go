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
	"flag"
	"fmt"
	"strings"
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
	region                 = "us-east-2"
	testTableName          = "GoIntegrationTestTable"
	indexAttribute         = "Name"
	columnName             = "Name"
	singleDocumentValue    = "SingleDocumentValue"
	multipleDocumentValue1 = "MultipleDocumentValue1"
	multipleDocumentValue2 = "MultipleDocumentValue2"
)

var ledgerSuffix = flag.String("ledger_suffix", "", "Suffix to the ledger name")

func createTestBase(ledgerNameBase string) *testBase {

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}
	client := qldb.NewFromConfig(cfg, func(options *qldb.Options) {
		options.Region = region
	})
	logger := defaultLogger{}
	ledgerName := ledgerNameBase + *ledgerSuffix
	regionName := region
	return &testBase{client, &ledgerName, &regionName, logger}
}

func (testBase *testBase) createLedger(t *testing.T) {
	testBase.logger.Log(fmt.Sprint("Creating ledger named ", *testBase.ledgerName, " ..."), LogInfo)
	_, err := testBase.qldb.CreateLedger(context.TODO(), &qldb.CreateLedgerInput{
		Name:               testBase.ledgerName,
		DeletionProtection: newBool(false),
		PermissionsMode:    types.PermissionsModeStandard,
	})
	assert.NoError(t, err)
	testBase.waitForActive()
}

func (testBase *testBase) deleteLedger(t *testing.T) {
	testBase.logger.Log(fmt.Sprint("Deleting ledger ", *testBase.ledgerName), LogInfo)
	_, err := testBase.qldb.DeleteLedger(context.TODO(), &qldb.DeleteLedgerInput{Name: testBase.ledgerName})
	if err != nil {
		var rnf *types.ResourceNotFoundException
		if errors.As(err, &rnf) {
			testBase.logger.Log("Encountered resource not found", LogInfo)
			return
		}
		var riu *types.ResourceInUseException
		if errors.As(err, &riu) {
			if strings.Contains(riu.ErrorMessage(), "Ledger is being created") {
				testBase.logger.Log("Encountered resource still being created", LogInfo)
				testBase.waitForActive()
				_, err = testBase.qldb.DeleteLedger(context.TODO(), &qldb.DeleteLedgerInput{Name: testBase.ledgerName})
			} else if strings.Contains(riu.ErrorMessage(), "Ledger is being deleted") {
				err = nil
			}
		}
		if err != nil {
			testBase.logger.Log("Encountered error during deletion", LogInfo)
			testBase.logger.Log(err.Error(), LogInfo)
			t.Errorf("Failing test due to deletion failure")
			assert.NoError(t, err)
			return
		}
	}
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

func (testBase *testBase) getDefaultDriver() (*QLDBDriver, error) {
	return testBase.getDriver(&testDriverOptions{
		ledgerName: *testBase.ledgerName,
		maxConcTx:  10,
		retryLimit: 4,
	})
}

type testDriverOptions struct {
	ledgerName string
	maxConcTx  int
	retryLimit int
}

func (testBase *testBase) getDriver(tdo *testDriverOptions) (*QLDBDriver, error) {

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}
	qldbSession := qldbsession.NewFromConfig(cfg, func(options *qldbsession.Options) {
		options.Region = region
	})

	return New(tdo.ledgerName, qldbSession, func(options *DriverOptions) {
		options.LoggerVerbosity = LogInfo
		options.MaxConcurrentTransactions = tdo.maxConcTx
		options.RetryPolicy.MaxRetryLimit = tdo.retryLimit
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

func newBool(b bool) *bool { return &b }
