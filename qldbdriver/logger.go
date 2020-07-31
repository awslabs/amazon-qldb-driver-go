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

import "log"

// Interface for a logger that can be used with QLDBDriver.
type Logger interface {
	Log(message string)
}

// LogLevel represents the valid logging verbosity levels.
type LogLevel uint8

const (
	// Log nothing.
	LogOff LogLevel = iota
	// Log informative events. This is the default logging level.
	LogInfo
	// Log information useful for closely tracing the operation of the QLDBDriver.
	LogDebug
)

type qldbLogger struct {
	logger    Logger
	verbosity LogLevel
}

func (qldbLogger *qldbLogger) log(message string, verbosityLevel LogLevel) {
	if verbosityLevel <= qldbLogger.verbosity {
		switch verbosityLevel {
		case LogInfo:
			qldbLogger.logger.Log("[INFO]" + message)
		case LogDebug:
			qldbLogger.logger.Log("[DEBUG]" + message)
		default:
			qldbLogger.logger.Log(message)
		}
	}
}

type defaultLogger struct{}

// Log the message using the built-in Golang logging package.
func (logger defaultLogger) Log(message string) {
	log.Println(message)
}
