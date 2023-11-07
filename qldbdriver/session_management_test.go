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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/qldbsession/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestSessionManagementIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// setup
	testBase := createTestBase("Golang-SessionMgmt")
	testBase.deleteLedger(t)
	testBase.waitForDeletion()
	testBase.createLedger(t)
	defer testBase.deleteLedger(t)

	t.Run("Fail connecting to non existent ledger", func(t *testing.T) {
		driver, err := testBase.getDriver(&testDriverOptions{
			ledgerName: "NoSuchLedger",
			maxConcTx:  10,
			retryLimit: 4,
		})
		require.NoError(t, err)
		defer driver.Shutdown(context.Background())

		_, err = driver.GetTableNames(context.Background())
		require.Error(t, err)

		var bre *types.BadRequestException
		assert.True(t, errors.As(err, &bre))
	})

	t.Run("Get session when pool doesnt have session and has not hit limit", func(t *testing.T) {
		driver, err := testBase.getDefaultDriver()
		require.NoError(t, err)
		defer driver.Shutdown(context.Background())

		result, err := driver.GetTableNames(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("Get session when pool has session and has not hit limit", func(t *testing.T) {
		driver, err := testBase.getDefaultDriver()
		require.NoError(t, err)
		defer driver.Shutdown(context.Background())

		result, err := driver.GetTableNames(context.Background())

		assert.NoError(t, err)
		assert.NotNil(t, result)

		result, err = driver.GetTableNames(context.Background())

		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("Get session when pool doesnt have session and has hit limit", func(t *testing.T) {
		driver, err := testBase.getDriver(&testDriverOptions{
			ledgerName: *testBase.ledgerName,
			maxConcTx:  1,
			retryLimit: 4,
		})
		require.NoError(t, err)
		driver.Shutdown(context.Background())

		errs, ctx := errgroup.WithContext(context.Background())

		for i := 0; i < 3; i++ {
			i := i
			errs.Go(func() error {
				testBase.logger.Log("start "+string(rune(i)), LogInfo)
				_, err := driver.GetTableNames(ctx)
				time.Sleep(1 * time.Second)
				testBase.logger.Log("end "+string(rune(i)), LogInfo)
				return err
			})
		}

		err = errs.Wait()
		assert.Error(t, err)
		driverErr, ok := err.(*qldbDriverError)
		assert.True(t, ok)
		assert.Error(t, driverErr)
	})

	t.Run("Get session when driver is closed", func(t *testing.T) {
		driver, err := testBase.getDriver(&testDriverOptions{
			ledgerName: *testBase.ledgerName,
			maxConcTx:  1,
			retryLimit: 4,
		})
		require.NoError(t, err)
		driver.Shutdown(context.Background())

		_, err = driver.GetTableNames(context.Background())
		assert.Error(t, err)
	})
}
