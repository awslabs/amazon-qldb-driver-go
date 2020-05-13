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
	"github.com/aws/aws-sdk-go/service/qldbsession"
)

type communicator struct {
	service      *qldbsession.QLDBSession
	sessionToken string
}

func sendCommand(request qldbsession.SendCommandInput, service *qldbsession.QLDBSession) qldbsession.SendCommandOutput {
	panic("")
}

func startSession(service *qldbsession.QLDBSession) communicator {
	panic("")
}

func (communicator communicator) abortTransaction() qldbsession.AbortTransactionResult {
	panic("")
}

func (communicator communicator) commitTransaction() qldbsession.CommitTransactionResult {
	panic("")
}

func (communicator communicator) executeStatement() qldbsession.ExecuteStatementResult {
	panic("")
}

func (communicator communicator) endSession() qldbsession.EndSessionResult {
	panic("")
}

func (communicator communicator) fetchPage() qldbsession.FetchPageResult {
	panic("")
}

func (communicator communicator) startTransaction() qldbsession.StartTransactionResult {
	panic("")
}
