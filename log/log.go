// Copyright (c) 2013 Kelsey Hightower. All rights reserved.
// Use of this source code is governed by the Apache License, Version 2.0
// that can be found in the LICENSE file.

/*
Package log provides support for logging to stdout and stderr.

Log entries will be logged in the following format:

    timestamp hostname tag[pid]: SEVERITY Message
*/
package log

import (
	"fmt"
	"os"
	"time"
)

var globalLogger *Log

func SetLogger(l *Log) {
	globalLogger = l
}

type Log struct {
	tag     string
	pid     int

	quiet   bool
	verbose bool
	debug   bool
}

func NewLogger(quiet bool, verbose bool, debug bool) *Log {
	return &Log {
		tag:     os.Args[0],
		pid:     os.Getpid(),
		quiet:   quiet,
		debug:   verbose,
		verbose: debug,
	}
}

// Debug logs a message with severity DEBUG.
func (l *Log) Debug(v ...interface{}) {
	if l.debug {
		l.write("DEBUG", v...)
	}
}

func Debug(v ...interface{}) {
	globalLogger.Debug(v...)
}

// Error logs a message with severity ERROR.
func (l *Log) Error(v ...interface{}) {
	l.write("ERROR", v...)
}

func Error(v ...interface{}) {
	globalLogger.Error(v...)
}

// Fatal logs a message with severity ERROR followed by a call to os.Exit().
func (l *Log) Fatal(v ...interface{}) {
	l.write("ERROR", v...)
	os.Exit(1)
}

func Fatal(v ...interface{}) {
	globalLogger.Fatal(v...)
}

// Info logs a message with severity INFO.
func (l *Log) Info(v ...interface{}) {
	l.write("INFO", v...)
}

func Info(v ...interface{}) {
	globalLogger.Info(v...)
}

// Notice logs a message with severity NOTICE.
func (l *Log) Notice(v ...interface{}) {
	if l.verbose || l.debug {
		l.write("NOTICE", v...)
	}
}

func Notice(v ...interface{}) {
	globalLogger.Notice(v...)
}

// Warning logs a message with severity WARNING.
func (l *Log) Warning(v ...interface{}) {
	l.write("WARNING", v...)
}

func Warning(v ...interface{}) {
	globalLogger.Warning(v...)
}

// write writes error messages to stderr and all others to stdout.
// Messages are written in the following format:
//     timestamp hostname name: SEVERITY Message
func (l *Log) write(level string, v ...interface{}) {
	var w *os.File
	timestamp := time.Now().Format(time.RFC3339)
	hostname, _ := os.Hostname()
	switch level {
	case "DEBUG", "INFO", "NOTICE", "WARNING":
		if l.quiet {
			return
		}
		w = os.Stdout
	case "ERROR":
		w = os.Stderr
	}
	fmt.Fprintf(
		w,
		"%s %s %s[%d]: %s %s\n",
		timestamp, hostname, l.tag, l.pid, level, fmt.Sprint(v...),
	)
}
