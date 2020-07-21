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

package integration

import (
	"fmt"
	"qldbdriver/qldbdriver"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/qldb"
	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/stretchr/testify/assert"
)

type testbase struct {
	qldb       *qldb.QLDB
	ledgerName *string
	regionName *string
	logger     *testLogger
}

const ledger string = "Gotest"
const region string = "us-east-1"

func createTestBase() *testbase {
	sess, err := session.NewSession(aws.NewConfig().WithRegion(region))
	mySession := session.Must(sess, err)
	qldb := qldb.New(mySession)
	logger := &testLogger{&defaultLogger{}, LogInfo}
	ledgerName := ledger
	regionName := region
	return &testbase{qldb, &ledgerName, &regionName, logger}
}

func (testbase *testbase) createLedger(t *testing.T) {
	testbase.logger.log(fmt.Sprint("Creating ledger named ", *testbase.ledgerName, " ..."), LogInfo)
	deletionProtection := false
	permissions := "ALLOW_ALL"
	_, err := testbase.qldb.CreateLedger(&qldb.CreateLedgerInput{Name: testbase.ledgerName, DeletionProtection: &deletionProtection, PermissionsMode: &permissions})
	assert.Nil(t, err)
	testbase.waitForActive()
}

func (testbase *testbase) deleteLedger(t *testing.T) {
	testbase.logger.log(fmt.Sprint("Deleting ledger ", *testbase.ledgerName), LogInfo)
	deletionProtection := false
	testbase.qldb.UpdateLedger(&qldb.UpdateLedgerInput{DeletionProtection: &deletionProtection, Name: testbase.ledgerName})
	_, err := testbase.qldb.DeleteLedger(&qldb.DeleteLedgerInput{Name: testbase.ledgerName})
	if err != nil {
		if _, ok := err.(*qldb.ResourceNotFoundException); ok {
			testbase.logger.log("Encountered resource not found", LogInfo)
			return
		}
		testbase.logger.log("Encountered error during deletion", LogInfo)
		testbase.logger.log(err.Error(), LogInfo)
		t.Errorf("Failing test due to deletion failure")
	}
	testbase.waitForDeletion()
}

func (testbase *testbase) waitForActive() {
	testbase.logger.log("Waiting for ledger to become active...", LogInfo)
	for true {
		output, _ := testbase.qldb.DescribeLedger(&qldb.DescribeLedgerInput{Name: testbase.ledgerName})
		if *output.State == "ACTIVE" {
			testbase.logger.log("Success. Ledger is active and ready to use.", LogInfo)
			return
		}
		testbase.logger.log("The ledger is still creating. Please wait...", LogInfo)
		time.Sleep(5 * time.Second)
	}
}

func (testbase *testbase) waitForDeletion() {
	testbase.logger.log("Waiting for ledger to be deleted...", LogInfo)
	for true {
		_, err := testbase.qldb.DescribeLedger(&qldb.DescribeLedgerInput{Name: testbase.ledgerName})
		testbase.logger.log("The ledger is still deleting. Please wait...", LogInfo)
		if err != nil {
			if _, ok := err.(*qldb.ResourceNotFoundException); ok {
				testbase.logger.log("The ledger is deleted", LogInfo)
				return
			}
		}
		time.Sleep(5 * time.Second)
	}
}

func (testbase *testbase) getDriver(ledgerName string, maxConcurrentTransactions uint16, retryLimit uint8) *qldbdriver.QLDBDriver {
	driverSession := session.Must(session.NewSession(aws.NewConfig().WithRegion(*testbase.regionName)))
	qldbsession := qldbsession.New(driverSession)
	return qldbdriver.New(ledgerName, qldbsession, func(options *qldbdriver.DriverOptions) {
		options.Logger = testbase.logger.logger
		options.LoggerVerbosity = qldbdriver.LogInfo
		options.MaxConcurrentTransactions = maxConcurrentTransactions
	})

}
