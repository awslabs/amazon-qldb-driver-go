/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
the License. A copy of the License is located at

http://www.apache.org/licenses/LICENSE-2.0

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
and limitations under the License.
*/

package integration

import "log"

type Logger interface {
	Log(message string)
}

type LogLevel uint8

const (
	LogOff LogLevel = iota
	LogInfo
	LogDebug
)

type testLogger struct {
	logger    Logger
	verbosity LogLevel
}

func (testLogger *testLogger) log(message string, verbosityLevel LogLevel) {
	if verbosityLevel <= testLogger.verbosity {
		switch verbosityLevel {
		case LogInfo:
			testLogger.logger.Log("[INFO]" + message)
		case LogDebug:
			testLogger.logger.Log("[DEBUG]" + message)
		default:
			testLogger.logger.Log(message)
		}
	}
}

type defaultLogger struct{}

func (logger defaultLogger) Log(message string) {
	log.Println(message)
}
