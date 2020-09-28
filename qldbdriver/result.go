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
	"context"
	"errors"

	"github.com/aws/aws-sdk-go/service/qldbsession"
)

// Result is a cursor over a result set from a QLDB statement.
type Result struct {
	ctx          context.Context
	communicator qldbService
	txnId        *string
	pageValues   []*qldbsession.ValueHolder
	pageToken    *string
	index        int
	logger       *qldbLogger
	ionBinary    []byte
	err          error
}

// Return the next row of data in the current result set. Returns an error if there are no more rows.
//
// The returned data is in Ion format. Use ion.Unmarshal or other Ion library methods to handle parsing.
// See https://github.com/amzn/ion-go for more information.
func (result *Result) Next(txn Transaction) bool {
	result.err = nil

	if result.index >= len(result.pageValues) {
		if result.pageToken == nil {
			result.err = errors.New("no more values")
			return false
		}
		result.err = result.getNextPage()
		if result.err != nil {
			return false
		}
		return result.Next(txn)
	}

	result.ionBinary = result.pageValues[result.index].IonBinary
	result.index++

	return true
}

func (result *Result) getNextPage() error {
	nextPage, err := result.communicator.fetchPage(result.ctx, result.pageToken, result.txnId)
	if err != nil {
		return err
	}
	result.pageValues = nextPage.Page.Values
	result.pageToken = nextPage.Page.NextPageToken
	result.index = 0
	return nil
}

// BufferedResult is a cursor over a result set from a QLDB statement that is valid outside the context of a transaction.
type BufferedResult struct {
	values [][]byte
	index  int
}

// Return whether or not there is another row to read in the current result set.
func (result *BufferedResult) HasNext() bool {
	return result.index < len(result.values)
}

// Return the next row of data in the current result set. Returns an error if there are no more rows.
//
// The returned data is in Ion format. Use ion.Unmarshal or other Ion library methods to handle parsing.
// See https://github.com/amzn/ion-go for more information.
func (result *BufferedResult) Next() ([]byte, error) {
	if !result.HasNext() {
		return nil, errors.New("no more values")
	}
	ionBinary := result.values[result.index]
	result.index++
	return ionBinary, nil
}
