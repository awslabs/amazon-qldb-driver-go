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
	"fmt"
	"log"
)

// Logger is an interface for a QLDBDriver logger.
type Logger interface {
	// Log the message using the built-in Golang logging package.
	Log(message string, verbosity LogLevel)
}

// LogLevel represents the valid logging verbosity levels.
type LogLevel uint8

const (
	// LogOff is for logging nothing.
	LogOff LogLevel = iota
	// LogInfo is for logging informative events. This is the default logging level.
	LogInfo
	// LogDebug is for logging information useful for closely tracing the operation of the QLDBDriver.
	LogDebug
)

type qldbLogger struct {
	logger    Logger
	verbosity LogLevel
}

func (qldbLogger *qldbLogger) log(verbosityLevel LogLevel, message string) {
	if verbosityLevel <= qldbLogger.verbosity {
		switch verbosityLevel {
		case LogInfo:
			qldbLogger.logger.Log("[INFO] "+message, verbosityLevel)
		case LogDebug:
			qldbLogger.logger.Log("[DEBUG] "+message, verbosityLevel)
		default:
			qldbLogger.logger.Log(message, verbosityLevel)
		}
	}
}

func (qldbLogger *qldbLogger) logf(verbosityLevel LogLevel, message string, args ...interface{}) {
	if verbosityLevel <= qldbLogger.verbosity {
		switch verbosityLevel {
		case LogInfo:
			qldbLogger.logger.Log(fmt.Sprintf("[INFO] "+message, args...), verbosityLevel)
		case LogDebug:
			qldbLogger.logger.Log(fmt.Sprintf("[DEBUG] "+message, args...), verbosityLevel)
		default:
			qldbLogger.logger.Log(fmt.Sprintf(message, args...), verbosityLevel)
		}
	}
}

type defaultLogger struct{}

// Log the message using the built-in Golang logging package.
func (logger defaultLogger) Log(message string, verbosity LogLevel) {
	log.Println(message)
}
