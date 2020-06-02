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
	"github.com/aws/aws-sdk-go/service/qldbsession"
)

type communicator struct {
	service      *qldbsession.QLDBSession
	sessionToken *string
}

func startSession(ctx context.Context, ledgerName string, service *qldbsession.QLDBSession) (*communicator, error) {
	startSession := &qldbsession.StartSessionRequest{LedgerName: &ledgerName}
	request := &qldbsession.SendCommandInput{StartSession: startSession}
	result, err := service.SendCommandWithContext(ctx, request)
	if err != nil {
		return nil, err
	}
	return &communicator{service, result.StartSession.SessionToken}, nil
}

func (communicator *communicator) abortTransaction(ctx context.Context) (*qldbsession.AbortTransactionResult, error) {
	panic("not yet implemented")
}

func (communicator *communicator) commitTransaction(ctx context.Context) (*qldbsession.CommitTransactionResult, error) {
	panic("not yet implemented")
}

func (communicator *communicator) executeStatement(ctx context.Context, statement *string, txnId *string) (*qldbsession.ExecuteStatementResult, error) {
	executeStatement := &qldbsession.ExecuteStatementRequest{Statement: statement, TransactionId: txnId}
	request := &qldbsession.SendCommandInput{ExecuteStatement: executeStatement}
	result, err := communicator.sendCommand(ctx, request)
	if err != nil {
		return nil, err
	}
	return result.ExecuteStatement, nil
}

func (communicator *communicator) endSession(ctx context.Context) (*qldbsession.EndSessionResult, error) {
	endSession := &qldbsession.EndSessionRequest{}
	request := &qldbsession.SendCommandInput{EndSession: endSession}
	result, err := communicator.sendCommand(ctx, request)
	if err != nil {
		return nil, err
	}
	return result.EndSession, nil
}

func (communicator *communicator) fetchPage(ctx context.Context, pageToken *string, txnId *string) (*qldbsession.FetchPageResult, error) {
	fetchPage := &qldbsession.FetchPageRequest{NextPageToken: pageToken, TransactionId: txnId}
	request := &qldbsession.SendCommandInput{FetchPage: fetchPage}
	result, err := communicator.sendCommand(ctx, request)
	if err != nil {
		return nil, err
	}
	return result.FetchPage, nil
}

func (communicator *communicator) startTransaction(ctx context.Context) (*qldbsession.StartTransactionResult, error) {
	startTransaction := &qldbsession.StartTransactionRequest{}
	request := &qldbsession.SendCommandInput{StartTransaction: startTransaction}
	result, err := communicator.sendCommand(ctx, request)
	if err != nil {
		return nil, err
	}
	return result.StartTransaction, nil
}

func (communicator *communicator) sendCommand(ctx context.Context, request *qldbsession.SendCommandInput) (*qldbsession.SendCommandOutput, error) {
	request.SetSessionToken(*communicator.sessionToken)
	return communicator.service.SendCommandWithContext(ctx, request)
}
