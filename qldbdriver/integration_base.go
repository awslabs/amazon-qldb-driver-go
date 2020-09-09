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
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	AWSSession "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/qldb"
	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/stretchr/testify/assert"
)

type testBase struct {
	qldb       *qldb.QLDB
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
	sess, err := AWSSession.NewSession(aws.NewConfig().WithRegion(region))
	mySession := AWSSession.Must(sess, err)
	qldb := qldb.New(mySession)
	logger := defaultLogger{}
	ledgerName := ledger
	regionName := region
	return &testBase{qldb, &ledgerName, &regionName, logger}
}

func (testBase *testBase) createLedger(t *testing.T) {
	testBase.logger.Log(fmt.Sprint("Creating ledger named ", *testBase.ledgerName, " ..."))
	deletionProtection := false
	permissions := "ALLOW_ALL"
	_, err := testBase.qldb.CreateLedger(&qldb.CreateLedgerInput{Name: testBase.ledgerName, DeletionProtection: &deletionProtection, PermissionsMode: &permissions})
	assert.Nil(t, err)
	testBase.waitForActive()
}

func (testBase *testBase) deleteLedger(t *testing.T) {
	testBase.logger.Log(fmt.Sprint("Deleting ledger ", *testBase.ledgerName))
	deletionProtection := false
	testBase.qldb.UpdateLedger(&qldb.UpdateLedgerInput{DeletionProtection: &deletionProtection, Name: testBase.ledgerName})
	_, err := testBase.qldb.DeleteLedger(&qldb.DeleteLedgerInput{Name: testBase.ledgerName})
	if err != nil {
		if _, ok := err.(*qldb.ResourceNotFoundException); ok {
			testBase.logger.Log("Encountered resource not found")
			return
		}
		testBase.logger.Log("Encountered error during deletion")
		testBase.logger.Log(err.Error())
		t.Errorf("Failing test due to deletion failure")
		assert.Nil(t, err)
		return
	}
	testBase.waitForDeletion()
}

func (testBase *testBase) waitForActive() {
	testBase.logger.Log("Waiting for ledger to become active...")
	for true {
		output, _ := testBase.qldb.DescribeLedger(&qldb.DescribeLedgerInput{Name: testBase.ledgerName})
		if *output.State == "ACTIVE" {
			testBase.logger.Log("Success. Ledger is active and ready to use.")
			return
		}
		testBase.logger.Log("The ledger is still creating. Please wait...")
		time.Sleep(5 * time.Second)
	}
}

func (testBase *testBase) waitForDeletion() {
	testBase.logger.Log("Waiting for ledger to be deleted...")
	for true {
		_, err := testBase.qldb.DescribeLedger(&qldb.DescribeLedgerInput{Name: testBase.ledgerName})
		testBase.logger.Log("The ledger is still deleting. Please wait...")
		if err != nil {
			if _, ok := err.(*qldb.ResourceNotFoundException); ok {
				testBase.logger.Log("The ledger is deleted")
				return
			}
		}
		time.Sleep(5 * time.Second)
	}
}

func (testBase *testBase) getDriver(ledgerName string, maxConcurrentTransactions uint16, retryLimit uint8) *QLDBDriver {
	driverSession := AWSSession.Must(AWSSession.NewSession(aws.NewConfig().WithRegion(*testBase.regionName)))
	qldbsession := qldbsession.New(driverSession)
	return New(ledgerName, qldbsession, func(options *DriverOptions) {
		options.LoggerVerbosity = LogInfo
		options.MaxConcurrentTransactions = maxConcurrentTransactions
		options.RetryLimit = retryLimit
	})

}
