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

type Result struct {
	ctx          context.Context
	communicator *communicator
	txnId        *string
	pageValues   []*qldbsession.ValueHolder
	pageToken    *string
	index        int
	logger       *qldbLogger
}

func (result *Result) HasNext() bool {
	return result.index < len(result.pageValues) || result.pageToken != nil
}

func (result *Result) Next(txn Transaction) ([]byte, error) {
	if !result.HasNext() {
		return nil, errors.New("no more values")
	}
	if result.index == len(result.pageValues) {
		err := result.getNextPage()
		if err != nil {
			return nil, err
		}
		return result.Next(txn)
	}
	ionBinary := result.pageValues[result.index].IonBinary
	result.index++
	return ionBinary, nil
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

type BufferedResult struct {
	values [][]byte
	index  int
}

func (result *BufferedResult) HasNext() bool {
	return result.index < len(result.values)
}

func (result *BufferedResult) Next() ([]byte, error) {
	if !result.HasNext() {
		return nil, errors.New("no more values")
	}
	ionBinary := result.values[result.index]
	result.index++
	return ionBinary, nil
}
