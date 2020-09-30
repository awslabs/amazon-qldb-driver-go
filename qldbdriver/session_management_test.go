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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestSessionManagementIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	//setup
	testBase := createTestBase()
	testBase.deleteLedger(t)
	testBase.createLedger(t)

	t.Run("Fail connecting to non existent ledger", func(t *testing.T) {
		driver, err := testBase.getDriver("NoSuchALedger", 10, 4)
		require.NoError(t, err)
		defer driver.Close(context.Background())

		_, err = driver.GetTableNames(context.Background())
		require.Error(t, err)

		awsErr, ok := err.(awserr.Error)
		assert.True(t, ok)
		assert.Equal(t, qldbsession.ErrCodeBadRequestException, awsErr.Code())
	})

	t.Run("Get session when pool doesnt have session and has not hit limit", func(t *testing.T) {
		driver, err := testBase.getDriver(ledger, 10, 4)
		require.NoError(t, err)
		defer driver.Close(context.Background())

		result, err := driver.GetTableNames(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("Get session when pool has session and has not hit limit", func(t *testing.T) {
		driver, err := testBase.getDriver(ledger, 10, 4)
		require.NoError(t, err)
		defer driver.Close(context.Background())

		result, err := driver.GetTableNames(context.Background())

		assert.NoError(t, err)
		assert.NotNil(t, result)

		result, err = driver.GetTableNames(context.Background())

		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("Get session when pool doesnt have session and has hit limit", func(t *testing.T) {
		driver, err := testBase.getDriver(ledger, 1, 4)
		require.NoError(t, err)
		driver.Close(context.Background())

		errs, ctx := errgroup.WithContext(context.Background())

		for i := 0; i < 3; i++ {
			errs.Go(func() error {
				testBase.logger.Log("start " + string(i))
				_, err := driver.GetTableNames(ctx)
				time.Sleep(1 * time.Second)
				testBase.logger.Log("end " + string(i))
				return err
			})
		}

		err = errs.Wait()
		assert.Error(t, err)
		driverErr, ok := err.(*QLDBDriverError)
		assert.True(t, ok)
		assert.Error(t, driverErr)
	})

	t.Run("Get session when driver is closed", func(t *testing.T) {
		driver, err := testBase.getDriver(ledger, 1, 4)
		require.NoError(t, err)
		driver.Close(context.Background())

		_, err = driver.GetTableNames(context.Background())
		assert.Error(t, err)
	})

	//cleanup
	testBase.deleteLedger(t)
}
