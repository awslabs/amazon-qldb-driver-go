// Package qldbdriveriface provides an interface to enable mocking the QLDBDriver
// for testing your code.
package qldbdriveriface

import (
	"context"
	"github.com/awslabs/amazon-qldb-driver-go/qldbdriver"
)

type QLDBDriverAPI interface {
	SetRetryPolicy(rp qldbdriver.RetryPolicy)
	Execute(ctx context.Context, fn func(txn qldbdriver.Transaction) (interface{}, error)) (interface{}, error)
	GetTableNames(ctx context.Context) ([]string, error)
	Shutdown(ctx context.Context)
}

var _ QLDBDriverAPI = (*qldbdriver.QLDBDriver)(nil)
