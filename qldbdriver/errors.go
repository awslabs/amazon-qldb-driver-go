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
	"fmt"
)

// QLDBDriverError is returned when an error caused by QLDBDriver has occurred.
type QLDBDriverError struct {
	errorMessage string
}

// Return the message denoting the cause of the error.
func (e *QLDBDriverError) Error() string {
	return e.errorMessage
}

type txnError struct {
	transactionID string
	message       string
	err           error
}

// Return the message denoting the cause of the error.
func (e *txnError) Error() string {
	msg := e.message

	if e.transactionID != "" {
		msg = fmt.Sprintf("%s: %s", e.transactionID, msg)
	}

	if e.err != nil {
		msg = fmt.Sprintf("%s\ncaused by: %v", msg, e.err)
	}
	return msg
}

func (e *txnError) unwrap() error {
	return e.err
}
