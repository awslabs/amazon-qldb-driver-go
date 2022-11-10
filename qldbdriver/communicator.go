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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/qldbsession"
	"github.com/aws/aws-sdk-go-v2/service/qldbsession/types"
	"github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver/qldbsessioniface"
)

const version string = "3.0.1"
const userAgentString string = "QLDB Driver for Golang v" + version

type qldbService interface {
	abortTransaction(ctx context.Context) (*types.AbortTransactionResult, error)
	commitTransaction(ctx context.Context, txnID *string, commitDigest []byte) (*types.CommitTransactionResult, error)
	executeStatement(ctx context.Context, statement *string, parameters []types.ValueHolder, txnID *string) (*types.ExecuteStatementResult, error)
	endSession(context.Context) (*types.EndSessionResult, error)
	fetchPage(ctx context.Context, pageToken *string, txnID *string) (*types.FetchPageResult, error)
	startTransaction(ctx context.Context) (*types.StartTransactionResult, error)
}

type communicator struct {
	service      qldbsessioniface.ClientAPI
	sessionToken *string
	logger       *qldbLogger
}

func startSession(ctx context.Context, ledgerName string, service qldbsessioniface.ClientAPI, logger *qldbLogger) (*communicator, error) {
	startSession := &types.StartSessionRequest{LedgerName: &ledgerName}
	sendInput := &qldbsession.SendCommandInput{StartSession: startSession}
	result, err := service.SendCommand(ctx, sendInput, func(options *qldbsession.Options) {
		options.Retryer = aws.NopRetryer{}
		options.APIOptions = append(options.APIOptions, middleware.AddUserAgentKey(userAgentString))
	})
	if err != nil {
		return nil, err
	}
	return &communicator{service, result.StartSession.SessionToken, logger}, nil
}

func (communicator *communicator) abortTransaction(ctx context.Context) (*types.AbortTransactionResult, error) {
	abortTransaction := &types.AbortTransactionRequest{}
	sendInput := &qldbsession.SendCommandInput{AbortTransaction: abortTransaction}
	result, err := communicator.sendCommand(ctx, sendInput)
	if err != nil {
		return nil, err
	}
	return result.AbortTransaction, nil
}

func (communicator *communicator) commitTransaction(ctx context.Context, txnID *string, commitDigest []byte) (*types.CommitTransactionResult, error) {
	commitTransaction := &types.CommitTransactionRequest{TransactionId: txnID, CommitDigest: commitDigest}
	sendInput := &qldbsession.SendCommandInput{CommitTransaction: commitTransaction}
	result, err := communicator.sendCommand(ctx, sendInput)
	if err != nil {
		return nil, err
	}
	return result.CommitTransaction, nil
}

func (communicator *communicator) executeStatement(ctx context.Context, statement *string, parameters []types.ValueHolder, txnID *string) (*types.ExecuteStatementResult, error) {
	executeStatement := &types.ExecuteStatementRequest{
		Parameters:    parameters,
		Statement:     statement,
		TransactionId: txnID,
	}
	sendInput := &qldbsession.SendCommandInput{ExecuteStatement: executeStatement}
	result, err := communicator.sendCommand(ctx, sendInput)
	if err != nil {
		return nil, err
	}
	return result.ExecuteStatement, nil
}

func (communicator *communicator) endSession(ctx context.Context) (*types.EndSessionResult, error) {
	endSession := &types.EndSessionRequest{}
	sendInput := &qldbsession.SendCommandInput{EndSession: endSession}
	result, err := communicator.sendCommand(ctx, sendInput)
	if err != nil {
		return nil, err
	}
	return result.EndSession, nil
}

func (communicator *communicator) fetchPage(ctx context.Context, pageToken *string, txnID *string) (*types.FetchPageResult, error) {
	fetchPage := &types.FetchPageRequest{NextPageToken: pageToken, TransactionId: txnID}
	sendInput := &qldbsession.SendCommandInput{FetchPage: fetchPage}
	result, err := communicator.sendCommand(ctx, sendInput)
	if err != nil {
		return nil, err
	}
	return result.FetchPage, nil
}

func (communicator *communicator) startTransaction(ctx context.Context) (*types.StartTransactionResult, error) {
	startTransaction := &types.StartTransactionRequest{}
	sendInput := &qldbsession.SendCommandInput{StartTransaction: startTransaction}
	result, err := communicator.sendCommand(ctx, sendInput)
	if err != nil {
		return nil, err
	}
	return result.StartTransaction, nil
}

func (communicator *communicator) sendCommand(ctx context.Context, command *qldbsession.SendCommandInput) (*qldbsession.SendCommandOutput, error) {
	command.SessionToken = communicator.sessionToken
	communicator.logger.logf(LogDebug, "%v", command)
	return communicator.service.SendCommand(ctx, command, func(options *qldbsession.Options) {
		options.Retryer = aws.NopRetryer{}
		options.APIOptions = append(options.APIOptions, middleware.AddUserAgentKey(userAgentString))
	})
}
