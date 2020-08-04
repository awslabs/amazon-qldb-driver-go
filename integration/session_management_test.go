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
	"context"
	"qldbdriver/qldbdriver"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestSessionManagement(t *testing.T) {
	//setup
	testBase := createTestBase()
	testBase.deleteLedger(t)
	testBase.createLedger(t)

	t.Run("Fail connecting to non existent ledger", func(t *testing.T) {
		driver := testBase.getDriver("NoSuchALedger", 10, 4)

		_, err := driver.GetTableNames(context.Background())

		assert.NotNil(t, err)
		awsErr, ok := err.(awserr.Error)
		assert.True(t, ok)
		assert.Equal(t, qldbsession.ErrCodeBadRequestException, awsErr.Code())

		driver.Close(context.Background())
	})

	t.Run("Get session when pool doesnt have session and has not hit limit", func(t *testing.T) {
		driver := testBase.getDriver(ledger, 10, 4)

		result, err := driver.GetTableNames(context.Background())

		assert.Nil(t, err)
		assert.NotNil(t, result)

		driver.Close(context.Background())
	})

	t.Run("Get session when pool has session and has not hit limit", func(t *testing.T) {
		driver := testBase.getDriver(ledger, 10, 4)

		result, err := driver.GetTableNames(context.Background())

		assert.Nil(t, err)
		assert.NotNil(t, result)

		result, err = driver.GetTableNames(context.Background())

		assert.Nil(t, err)
		assert.NotNil(t, result)

		driver.Close(context.Background())
	})

	t.Run("Get session when pool doesnt have session and has hit limit", func(t *testing.T) {
		driver := testBase.getDriver(ledger, 1, 4)

		errs, ctx := errgroup.WithContext(context.Background())

		for i := 0; i < 3; i++ {
			errs.Go(func() error {
				testBase.logger.log("start "+string(i), LogInfo)
				_, err := driver.GetTableNames(ctx)
				time.Sleep(1 * time.Second)
				testBase.logger.log("end "+string(i), LogInfo)
				return err
			})
		}

		err := errs.Wait()
		assert.NotNil(t, err)
		driverErr, ok := err.(*qldbdriver.QLDBDriverError)
		assert.True(t, ok)
		assert.NotNil(t, driverErr)

		driver.Close(context.Background())
	})

	t.Run("Get session when driver is closed", func(t *testing.T) {
		driver := testBase.getDriver(ledger, 1, 4)
		driver.Close(context.Background())

		assert.Panics(t, func() { driver.GetTableNames(context.Background()) })
	})

	//cleanup
	testBase.deleteLedger(t)
}
