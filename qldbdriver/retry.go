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
	"math"
	"math/rand"
	"time"
)

// RetryPolicyContext contains retry context passing to the retry strategy
type RetryPolicyContext struct {
	RetryAttempted int
	RetriedError   error
}

// BackoffStrategy defines customized back off strategy
type BackoffStrategy interface {
	Delay(ctx RetryPolicyContext) time.Duration
}

// RetryPolicy defines the retry policy
type RetryPolicy struct {
	MaxRetryLimit int
	Backoff       BackoffStrategy
}

// ExponentialBackoffStrategy is the default back off strategy implementation
type ExponentialBackoffStrategy struct {
	SleepBaseInMillis float64
	SleepCapInMillis  float64
}

// Delay implements BackoffStratgy
func (s ExponentialBackoffStrategy) Delay(ctx RetryPolicyContext) time.Duration {
	jitter := rand.Float64()*0.5 + 0.5

	return time.Duration(jitter*math.Min(s.SleepCapInMillis, math.Pow(s.SleepBaseInMillis, float64(ctx.RetryAttempted)))) * time.Millisecond
}
