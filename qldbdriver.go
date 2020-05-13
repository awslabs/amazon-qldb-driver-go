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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/qldbsession"
)

type QLDBDriver interface {
	Execute(fn func(txn Transaction) interface{}) interface{}
	GetTableNames() []string
	Close()
}

type driver struct {
	ledgerName string
	service    *qldbsession.QLDBSession
}

func (driver *driver) Execute(fn func(txn Transaction) interface{}) interface{} {
	panic("")
}

func (driver *driver) GetTableNames() []string {
	panic("")
}

func (driver *driver) Close() {
	panic("")
}

type DriverBuilder struct {
	ledgerName     string
	configProvider client.ConfigProvider
	configs        []*aws.Config
}

func (builder *DriverBuilder) Build() QLDBDriver {
	return &driver{}
}

func (builder *DriverBuilder) WithLedgerName(ledgerName string) *DriverBuilder {
	builder.ledgerName = ledgerName
	return builder
}

func (builder *DriverBuilder) WithConfigProvider(configProvider client.ConfigProvider) *DriverBuilder {
	builder.configProvider = configProvider
	return builder
}

func (builder *DriverBuilder) WithConfigs(configs ...*aws.Config) *DriverBuilder {
	builder.configs = configs
	return builder
}
