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
	"testing"
)

func TestSessionManagement(t *testing.T) {
	//setup

	t.Run("Fail connecting to non existent ledger", func(t *testing.T) {
	})

	t.Run("Get session when pool doesnt have session and has not hit limit", func(t *testing.T) {
	})

	t.Run("Get session when pool has session and has not hit limit", func(t *testing.T) {
	})

	t.Run("Get session when pool doesnt have session and has hit limit", func(t *testing.T) {
	})

	t.Run("Get session when pool doesnt have session and has hit limit", func(t *testing.T) {
	})

	t.Run("Get session when pool does not have session and has hit limit and session is returned to pool", func(t *testing.T) {
	})

	t.Run("Get session when driver is closed", func(t *testing.T) {
	})

}
