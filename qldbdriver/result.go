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

	"github.com/aws/aws-sdk-go-v2/service/qldbsession/types"
)

// Result is a cursor over a result set from a QLDB statement.
type Result interface {
	Next(txn Transaction) bool
	GetCurrentData() []byte
	GetConsumedIOs() *IOUsage
	GetTimingInformation() *TimingInformation
	Err() error
}

type result struct {
	ctx          context.Context
	communicator qldbService
	txnID        *string
	pageValues   []types.ValueHolder
	pageToken    *string
	index        int
	logger       *qldbLogger
	ionBinary    []byte
	ioUsage      *IOUsage
	timingInfo   *TimingInformation
	err          error
}

// Next advances to the next row of data in the current result set.
// Returns true if there was another row of data to advance. Returns false if there is no more data or if an error occurred.
// After a successful call to Next, call GetCurrentData to retrieve the current row of data.
// After an unsuccessful call to Next, check Err to see if Next returned false because an error happened or because there is no more data.
func (result *result) Next(txn Transaction) bool {
	result.ionBinary = nil
	result.err = nil

	if result.index >= len(result.pageValues) {
		if result.pageToken == nil {
			// No more data left
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

func (result *result) getNextPage() error {
	nextPage, err := result.communicator.fetchPage(result.ctx, result.pageToken, result.txnID)
	if err != nil {
		return err
	}

	result.pageValues = nextPage.Page.Values
	result.pageToken = nextPage.Page.NextPageToken
	result.index = 0
	result.updateMetrics(nextPage)
	return nil
}

func (result *result) updateMetrics(fetchPageResult *types.FetchPageResult) {
	if fetchPageResult.ConsumedIOs != nil {
		*result.ioUsage.readIOs += fetchPageResult.ConsumedIOs.ReadIOs
		*result.ioUsage.writeIOs += fetchPageResult.ConsumedIOs.WriteIOs
	}

	if fetchPageResult.TimingInformation != nil {
		*result.timingInfo.processingTimeMilliseconds += fetchPageResult.TimingInformation.ProcessingTimeMilliseconds
	}
}

// GetConsumedIOs returns the statement statistics for the current number of read IO requests that were consumed. The statistics are stateful.
func (result *result) GetConsumedIOs() *IOUsage {
	if result.ioUsage == nil {
		return nil
	}
	return newIOUsage(*result.ioUsage.readIOs, *result.ioUsage.writeIOs)
}

// GetTimingInformation returns the statement statistics for the current server-side processing time. The statistics are stateful.
func (result *result) GetTimingInformation() *TimingInformation {
	if result.timingInfo == nil {
		return nil
	}
	return newTimingInformation(*result.timingInfo.processingTimeMilliseconds)
}

// GetCurrentData returns the current row of data in Ion format. Use ion.Unmarshal or other Ion library methods to handle parsing.
// See https://github.com/amzn/ion-go for more information.
func (result *result) GetCurrentData() []byte {
	return result.ionBinary
}

// Err returns an error if a previous call to Next has failed.
// The returned error will be nil if the previous call to Next succeeded.
func (result *result) Err() error {
	return result.err
}

// BufferedResult is a cursor over a result set from a QLDB statement that is valid outside the context of a transaction.
type BufferedResult interface {
	Next() bool
	GetCurrentData() []byte
	GetConsumedIOs() *IOUsage
	GetTimingInformation() *TimingInformation
}

type bufferedResult struct {
	values     [][]byte
	index      int
	ionBinary  []byte
	ioUsage    *IOUsage
	timingInfo *TimingInformation
}

// Next advances to the next row of data in the current result set.
// Returns true if there was another row of data to advance. Returns false if there is no more data.
// After a successful call to Next, call GetCurrentData to retrieve the current row of data.
func (result *bufferedResult) Next() bool {
	result.ionBinary = nil

	if result.index >= len(result.values) {
		return false
	}

	result.ionBinary = result.values[result.index]
	result.index++
	return true
}

// GetCurrentData returns the current row of data in Ion format. Use ion.Unmarshal or other Ion library methods to handle parsing.
// See https://github.com/amzn/ion-go for more information.
func (result *bufferedResult) GetCurrentData() []byte {
	return result.ionBinary
}

// GetConsumedIOs returns the statement statistics for the total number of read IO requests that were consumed.
func (result *bufferedResult) GetConsumedIOs() *IOUsage {
	if result.ioUsage == nil {
		return nil
	}
	return newIOUsage(*result.ioUsage.readIOs, *result.ioUsage.writeIOs)
}

// GetTimingInformation returns the statement statistics for the total server-side processing time.
func (result *bufferedResult) GetTimingInformation() *TimingInformation {
	if result.timingInfo == nil {
		return nil
	}
	return newTimingInformation(*result.timingInfo.processingTimeMilliseconds)
}

// IOUsage contains metrics for the amount of IO requests that were consumed.
type IOUsage struct {
	readIOs  *int64
	writeIOs *int64
}

// newIOUsage creates a new instance of IOUsage.
func newIOUsage(readIOs int64, writeIOs int64) *IOUsage {
	return &IOUsage{&readIOs, &writeIOs}
}

// GetReadIOs returns the number of read IO requests that were consumed for a statement execution.
func (ioUsage *IOUsage) GetReadIOs() *int64 {
	return ioUsage.readIOs
}

// getWriteIOs returns the number of write IO requests that were consumed for a statement execution.
func (ioUsage *IOUsage) getWriteIOs() *int64 {
	return ioUsage.writeIOs
}

// TimingInformation contains metrics for server-side processing time.
type TimingInformation struct {
	processingTimeMilliseconds *int64
}

// newTimingInformation creates a new instance of TimingInformation.
func newTimingInformation(processingTimeMilliseconds int64) *TimingInformation {
	return &TimingInformation{&processingTimeMilliseconds}
}

// GetProcessingTimeMilliseconds returns the server-side processing time in milliseconds for a statement execution.
func (timingInfo *TimingInformation) GetProcessingTimeMilliseconds() *int64 {
	return timingInfo.processingTimeMilliseconds
}
