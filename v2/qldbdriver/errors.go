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

// qldbDriverError is returned when an error caused by QLDBDriver has occurred.
type qldbDriverError struct {
	errorMessage string
}

// Return the message denoting the cause of the error.
func (e *qldbDriverError) Error() string {
	return e.errorMessage
}

type txnError struct {
	transactionID string
	message       string
	err           error
	canRetry      bool
	abortSuccess  bool
	isISE         bool
}

func (e *txnError) unwrap() error {
	return e.err
}
