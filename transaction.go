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

type Transaction interface {
	Execute(statement string, parameters ...IonValue) Result
	Abort()
}

type transaction struct {
	communicator communicator
}

func (txn *transaction) execute(statement string, parameters ...IonValue) Result {
	panic("")
}

func (txn *transaction) commit() {

}

func (txn *transaction) abort() {

}

type transactionExecutor struct {
	txn      *transaction
	isClosed bool
}

func (executor *transactionExecutor) Execute(statement string, parameters ...IonValue) {
	executor.txn.execute(statement, parameters...)
}

func (executor *transactionExecutor) Abort() {
	panic("")
}
