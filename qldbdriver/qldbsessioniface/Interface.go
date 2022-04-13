/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
the License. A copy of the License is located at

http://www.apache.org/licenses/LICENSE-2.0

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
and limitations under the License.
*/

// Package qldbsessioniface provides an interface to enable mocking the Amazon QLDB Session service client
// for testing your code.
//
// It is important to note that this interface will have breaking changes
// when the service model is updated and adds new API operations, paginators,
// and waiters.
package qldbsessioniface

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/qldbsession"
)

// ClientAPI provides an interface to enable mocking the
// qldbsession.Client methods. This make unit testing your code that
// calls out to the SDK's service client's calls easier.
//
// The best way to use this interface is so the SDK's service client's calls
// can be stubbed out for unit testing your code with the SDK without needing
// to inject custom request handlers into the SDK's request pipeline.
//
//    // myFunc uses an SDK service client to make a request to
//    // QLDB Session.
//    func myFunc(svc qldbsessioniface.ClientAPI) bool {
//        // Make svc.SendCommand request
//    }
//
//    func main() {
//        cfg, err := external.LoadDefaultAWSConfig()
//        if err != nil {
//            panic("failed to load config, " + err.Error())
//        }
//
//        svc := qldbsession.New(cfg)
//
//        myFunc(svc)
//    }
//
// In your _test.go file:
//
//    // Define a mock struct to be used in your unit tests of myFunc.
//    type mockClientClient struct {
//        qldbsessioniface.ClientAPI
//    }
//    func (m *mockClientClient) SendCommand(ctx context.Context, params *qldbsession.SendCommandInput, optFns ...func(*qldbsession.Options)) (*qldbsession.SendCommandOutput, error) {
//        // mock response/functionality
//    }
//
//    func TestMyFunc(t *testing.T) {
//        // Setup Test
//        mockSvc := &mockClientClient{}
//
//        myfunc(mockSvc)
//
//        // Verify myFunc's functionality
//    }
//
// It is important to note that this interface will have breaking changes
// when the service model is updated and adds new API operations, paginators,
// and waiters. It's suggested to use the pattern above for testing, or using
// tooling to generate mocks to satisfy the interfaces.
type ClientAPI interface {
	SendCommand(ctx context.Context, params *qldbsession.SendCommandInput, optFns ...func(*qldbsession.Options)) (*qldbsession.SendCommandOutput, error)
}

var _ ClientAPI = (*qldbsession.Client)(nil)
