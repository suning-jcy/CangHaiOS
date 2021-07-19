// Go support for leveled logs, analogous to https://code.google.com/p/google-glog/
//
// Copyright 2013 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package glog implements logging analogous to the Google-internal C++ INFO/ERROR/V setup.
// It provides functions Info, Warning, Error, Fatal, plus formatting variants such as
// Infof. It also provides V-style logging controlled by the -v and -vmodule=file=2 flags.
//
// Basic examples:
//
//	glog.Info("Prepare to repel boarders")
//
//	glog.Fatalf("Initialization failed: %s", err)
//
// See the documentation for the V function for an explanation of these examples:
//
//	if glog.V(2) {
//		glog.Info("Starting transaction...")
//	}
//
//	glog.V(2).Infoln("Processed", nItems, "elements")
//
// Log output is buffered and written periodically using Flush. Programs
// should call Flush before exiting to guarantee all log output is written.
//
// By default, all log statements write to files in a temporary directory.
// This package provides several flags that modify this behavior.
//
//	-logtostderr=false
//		Logs are written to standard error instead of to files.
//	-alsologtostderr=false
//		Logs are written to standard error as well as to files.
//	-stderrthreshold=ERROR
//		Log events at or above this severity are logged to standard
//		error as well as to files.
//
//	Other flags provide aids to debugging.
//
//	-log_backtrace_at=""
//		When set to a file and line number holding a logging statement,
//		such as
//			-log_backtrace_at=gopherflakes.go:234
//		a stack trace will be written to the Info log whenever execution
//		hits that statement. (Unlike with -vmodule, the ".go" must be
//		present.)
//	-v=0
//		Enable V-leveled logging at the specified level.
//	-vmodule=""
//		The syntax of the argument is a comma-separated list of pattern=N,
//		where pattern is a literal file name (minus the ".go" suffix) or
//		"glob" pattern and N is a V level. For instance,
//			-vmodule=gopher*=3
//		sets the V level to 3 in all Go files whose names begin "gopher".
//
package glog

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.google.com/p/weed-fs/go/public"
)

// severity identifies the sort of log: info, warning etc. It also implements
// the flag.Value interface. The -stderrthreshold flag is of type severity and
// should be modified only through the flag.Value interface. The values match
// the corresponding constants in C++.
type severity int32 // sync/atomic int32

const (
	infoLog severity = iota
	warningLog
	errorLog
	fatalLog
	masterHttpLog
	filerHttpLog
	volumeHttpLog
	hissyncHttpLog
	folderHttpLog
	accountHttpLog
	bucketHttpLog
	syncinterfaceHttpLog
	syncdatahandlingHttpLog
	regionHttpLog
	partitionHttpLog
	failRecordLog
	statusRecordLog
	ftpProtLog
	duohuoLog
	backupHttpLog
	sepRecLog
	numSeverity
)

var HttpLogEnable = true

const severityChar = "IWEF"

var severityName = []string{
	infoLog:    "INFO",
	warningLog: "WARNING",
	errorLog:   "ERROR",
	fatalLog:   "FATAL",
	//hujf
	masterHttpLog:    "MASTERHTTP",
	filerHttpLog:     "FILERHTTP",
	volumeHttpLog:    "VOLUMEHTTP",
	hissyncHttpLog:   "HISSYNCHTTP",
	folderHttpLog:    "FOLDERHTTP",
	accountHttpLog:   "ACCOUNTHTTP",
	bucketHttpLog:    "BUCKETHTTP",
	partitionHttpLog: "PARTITIONHTTP",
	regionHttpLog:    "REGIONHTTP",
	syncinterfaceHttpLog: "SYNCINTERFACEHTTP",
	syncdatahandlingHttpLog: "SYNCDATAHANDLINGHTTP",
	failRecordLog:    "FAILRECORD",
	statusRecordLog:  "STATUSRECORD",
	ftpProtLog:       "FTPPROT",
	duohuoLog:        "RECOVERYLOG",
	backupHttpLog:    "BACKUPHTTP",
	sepRecLog:        "SEPARATIONHTTP",
}

// get returns the value of the severity.
func (s *severity) get() severity {
	return severity(atomic.LoadInt32((*int32)(s)))
}

// set sets the value of the severity.
func (s *severity) set(val severity) {
	atomic.StoreInt32((*int32)(s), int32(val))
}

// String is part of the flag.Value interface.
func (s *severity) String() string {
	return strconv.FormatInt(int64(*s), 10)
}

// Get is part of the flag.Value interface.
func (s *severity) Get() interface{} {
	return *s
}

// Set is part of the flag.Value interface.
func (s *severity) Set(value string) error {
	var threshold severity
	// Is it a known name?
	if v, ok := severityByName(value); ok {
		threshold = v
	} else {
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		threshold = severity(v)
	}
	logging.stderrThreshold.set(threshold)
	return nil
}

func severityByName(s string) (severity, bool) {
	s = strings.ToUpper(s)
	for i, name := range severityName {
		if name == s {
			return severity(i), true
		}
	}
	return 0, false
}

// OutputStats tracks the number of output lines and bytes written.
type OutputStats struct {
	lines int64
	bytes int64
}

// Lines returns the number of lines written.
func (s *OutputStats) Lines() int64 {
	return atomic.LoadInt64(&s.lines)
}

// Bytes returns the number of bytes written.
func (s *OutputStats) Bytes() int64 {
	return atomic.LoadInt64(&s.bytes)
}

// Stats tracks the number of lines of output and number of bytes
// per severity level. Values must be read with atomic.LoadInt64.
var Stats struct {
	Info, Warning, Error OutputStats
}

var severityStats = [numSeverity]*OutputStats{
	infoLog:    &Stats.Info,
	warningLog: &Stats.Warning,
	errorLog:   &Stats.Error,
}

// Level is exported because it appears in the arguments to V and is
// the type of the v flag, which can be set programmatically.
// It's a distinct type because we want to discriminate it from logType.
// Variables of type level are only changed under logging.mu.
// The -v flag is read only with atomic ops, so the state of the logging
// module is consistent.

// Level is treated as a sync/atomic int32.

// Level specifies a level of verbosity for V logs. *Level implements
// flag.Value; the -v flag is of type Level and should be modified
// only through the flag.Value interface.
type Level int32

// get returns the value of the Level.
func (l *Level) get() Level {
	return Level(atomic.LoadInt32((*int32)(l)))
}

// set sets the value of the Level.
func (l *Level) set(val Level) {
	atomic.StoreInt32((*int32)(l), int32(val))
}

// String is part of the flag.Value interface.
func (l *Level) String() string {
	return strconv.FormatInt(int64(*l), 10)
}

// Get is part of the flag.Value interface.
func (l *Level) Get() interface{} {
	return *l
}

// Set is part of the flag.Value interface.
func (l *Level) Set(value string) error {
	v, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	logging.mu.Lock()
	defer logging.mu.Unlock()
	logging.setVState(Level(v), logging.vmodule.filter, false)
	return nil
}

// moduleSpec represents the setting of the -vmodule flag.
type moduleSpec struct {
	filter []modulePat
}

// modulePat contains a filter for the -vmodule flag.
// It holds a verbosity level and a file pattern to match.
type modulePat struct {
	pattern string
	literal bool // The pattern is a literal string
	level   Level
}

// match reports whether the file matches the pattern. It uses a string
// comparison if the pattern contains no metacharacters.
func (m *modulePat) match(file string) bool {
	if m.literal {
		return file == m.pattern
	}
	match, _ := filepath.Match(m.pattern, file)
	return match
}

func (m *moduleSpec) String() string {
	// Lock because the type is not atomic. TODO: clean this up.
	logging.mu.Lock()
	defer logging.mu.Unlock()
	var b bytes.Buffer
	for i, f := range m.filter {
		if i > 0 {
			b.WriteRune(',')
		}
		fmt.Fprintf(&b, "%s=%d", f.pattern, f.level)
	}
	return b.String()
}

// Get is part of the (Go 1.2)  flag.Getter interface. It always returns nil for this flag type since the
// struct is not exported.
func (m *moduleSpec) Get() interface{} {
	return nil
}

var errVmoduleSyntax = errors.New("syntax error: expect comma-separated list of filename=N")

// Syntax: -vmodule=recordio=2,file=1,gfs*=3
func (m *moduleSpec) Set(value string) error {
	var filter []modulePat
	for _, pat := range strings.Split(value, ",") {
		if len(pat) == 0 {
			// Empty strings such as from a trailing comma can be ignored.
			continue
		}
		patLev := strings.Split(pat, "=")
		if len(patLev) != 2 || len(patLev[0]) == 0 || len(patLev[1]) == 0 {
			return errVmoduleSyntax
		}
		pattern := patLev[0]
		v, err := strconv.Atoi(patLev[1])
		if err != nil {
			return errors.New("syntax error: expect comma-separated list of filename=N")
		}
		if v < 0 {
			return errors.New("negative value for vmodule level")
		}
		if v == 0 {
			continue // Ignore. It's harmless but no point in paying the overhead.
		}
		// TODO: check syntax of filter?
		filter = append(filter, modulePat{pattern, isLiteral(pattern), Level(v)})
	}
	logging.mu.Lock()
	defer logging.mu.Unlock()
	logging.setVState(logging.verbosity, filter, true)
	return nil
}

// isLiteral reports whether the pattern is a literal string, that is, has no metacharacters
// that require filepath.Match to be called to match the pattern.
func isLiteral(pattern string) bool {
	return !strings.ContainsAny(pattern, `*?[]\`)
}

// traceLocation represents the setting of the -log_backtrace_at flag.
type traceLocation struct {
	file string
	line int
}

// isSet reports whether the trace location has been specified.
// logging.mu is held.
func (t *traceLocation) isSet() bool {
	return t.line > 0
}

// match reports whether the specified file and line matches the trace location.
// The argument file name is the full path, not the basename specified in the flag.
// logging.mu is held.
func (t *traceLocation) match(file string, line int) bool {
	if t.line != line {
		return false
	}
	if i := strings.LastIndex(file, "/"); i >= 0 {
		file = file[i+1:]
	}
	return t.file == file
}

func (t *traceLocation) String() string {
	// Lock because the type is not atomic. TODO: clean this up.
	logging.mu.Lock()
	defer logging.mu.Unlock()
	return fmt.Sprintf("%s:%d", t.file, t.line)
}

// Get is part of the (Go 1.2) flag.Getter interface. It always returns nil for this flag type since the
// struct is not exported
func (t *traceLocation) Get() interface{} {
	return nil
}

var errTraceSyntax = errors.New("syntax error: expect file.go:234")

// Syntax: -log_backtrace_at=gopherflakes.go:234
// Note that unlike vmodule the file extension is included here.
func (t *traceLocation) Set(value string) error {
	if value == "" {
		// Unset.
		t.line = 0
		t.file = ""
	}
	fields := strings.Split(value, ":")
	if len(fields) != 2 {
		return errTraceSyntax
	}
	file, line := fields[0], fields[1]
	if !strings.Contains(file, ".") {
		return errTraceSyntax
	}
	v, err := strconv.Atoi(line)
	if err != nil {
		return errTraceSyntax
	}
	if v <= 0 {
		return errors.New("negative or zero value for level")
	}
	logging.mu.Lock()
	defer logging.mu.Unlock()
	t.line = v
	t.file = file
	return nil
}

// flushSyncWriter is the interface satisfied by logging destinations.
type flushSyncWriter interface {
	Flush() error
	Sync() error
	rotateFile(now time.Time) error
	rotateFileByIdx(force bool) error
	io.Writer
}

func init() {
	flag.BoolVar(&logging.toStderr, "logtostderr", false, "log to standard error instead of files")
	flag.BoolVar(&logging.alsoToStderr, "alsologtostderr", false, "log to standard error as well as files")
	flag.Var(&logging.verbosity, "v", "log level for V logs")
	flag.Var(&logging.stderrThreshold, "stderrthreshold", "logs at or above this threshold go to stderr")
	flag.Var(&logging.vmodule, "vmodule", "comma-separated list of pattern=N settings for file-filtered logging")
	flag.Var(&logging.traceLocation, "log_backtrace_at", "when logging hits line file:N, emit a stack trace")
	//hujf http log switch
	flag.BoolVar(&logging.http, "http", false, "if true, log the http request")
	// Default stderrThreshold is ERROR.
	logging.stderrThreshold = errorLog

	logging.setVState(0, nil, false)
	go logging.flushDaemon()
}

//added by hujianfei in 20141013
var logParamConf = flag.String("logParamConf", "/etc/weedfs/sdfs.conf", "weedfs param conf file")

func initLogDir() {
	conf := SetConf(*logParamConf)
	*logDir = conf.GetStringValue("log", "log_dir", "", *logDir)
	tmpVerb := conf.GetIntValue("log", "v", 0, int(logging.verbosity.get()))
	logging.verbosity.set(Level(tmpVerb))
}

// Flush flushes all pending log I/O.
func Flush() {
	logging.lockAndFlushAll()
}

// loggingT collects all the global state of the logging setup.
type loggingT struct {
	// Boolean flags. Not handled atomically because the flag.Value interface
	// does not let us avoid the =true, and that shorthand is necessary for
	// compatibility. TODO: does this matter enough to fix? Seems unlikely.
	toStderr     bool // The -logtostderr flag.
	alsoToStderr bool // The -alsologtostderr flag.

	// Level flag. Handled atomically.
	stderrThreshold severity // The -stderrthreshold flag.

	// freeList is a list of byte buffers, maintained under freeListMu.
	freeList *buffer
	// freeListMu maintains the free list. It is separate from the main mutex
	// so buffers can be grabbed and printed to without holding the main lock,
	// for better parallelization.
	freeListMu sync.Mutex

	// mu protects the remaining elements of this structure and is
	// used to synchronize logging.
	mu sync.Mutex
	// file holds writer for each of the log types.
	file [numSeverity]flushSyncWriter
	// pcs is used in V to avoid an allocation when computing the caller's PC.
	pcs [1]uintptr
	// vmap is a cache of the V Level for each V() call site, identified by PC.
	// It is wiped whenever the vmodule flag changes state.
	vmap map[uintptr]Level
	// filterLength stores the length of the vmodule filter chain. If greater
	// than zero, it means vmodule is enabled. It may be read safely
	// using sync.LoadInt32, but is only modified under mu.
	filterLength int32
	// traceLocation is the state of the -log_backtrace_at flag.
	traceLocation traceLocation
	// These flags are modified only under lock, although verbosity may be fetched
	// safely using atomic.LoadInt32.
	vmodule   moduleSpec // The state of the -vmodule flag.
	verbosity Level      // V logging level, the value of the -v flag/
	//hujf
	http bool // if log the http request
}

// buffer holds a byte Buffer for reuse. The zero value is ready for use.
type buffer struct {
	bytes.Buffer
	tmp  [64]byte // temporary byte array for creating headers.
	next *buffer
}

var logging loggingT

// setVState sets a consistent state for V logging.
// l.mu is held.
func (l *loggingT) setVState(verbosity Level, filter []modulePat, setFilter bool) {
	// Turn verbosity off so V will not fire while we are in transition.
	logging.verbosity.set(0)
	// Ditto for filter length.
	logging.filterLength = 0

	// Set the new filters and wipe the pc->Level map if the filter has changed.
	if setFilter {
		logging.vmodule.filter = filter
		logging.vmap = make(map[uintptr]Level)
	}

	// Things are consistent now, so enable filtering and verbosity.
	// They are enabled in order opposite to that in V.
	atomic.StoreInt32(&logging.filterLength, int32(len(filter)))
	logging.verbosity.set(verbosity)
}

// getBuffer returns a new, ready-to-use buffer.
func (l *loggingT) getBuffer() *buffer {
	l.freeListMu.Lock()
	b := l.freeList
	if b != nil {
		l.freeList = b.next
	}
	l.freeListMu.Unlock()
	if b == nil {
		b = new(buffer)
	} else {
		b.next = nil
		b.Reset()
	}
	return b
}

// putBuffer returns a buffer to the free list.
func (l *loggingT) putBuffer(b *buffer) {
	if b.Len() >= 256 {
		// Let big buffers die a natural death.
		return
	}
	l.freeListMu.Lock()
	b.next = l.freeList
	l.freeList = b
	l.freeListMu.Unlock()
}

var timeNow = time.Now // Stubbed out for testing.

/*
header formats a log header as defined by the C++ implementation.
It returns a buffer containing the formatted header.

Log lines have this form:
	Lmmdd hh:mm:ss.uuuuuu threadid file:line] msg...
where the fields are defined as follows:
	L                A single character, representing the log level (eg 'I' for INFO)
	mm               The month (zero padded; ie May is '05')
	dd               The day (zero padded)
	hh:mm:ss.uuuuuu  Time in hours, minutes and fractional seconds
	threadid         The space-padded thread ID as returned by GetTID()
	file             The file name
	line             The line number
	msg              The user-supplied message
*/

func (l *loggingT) header(s severity) *buffer {
	// Lmmdd hh:mm:ss.uuuuuu threadid file:line]
	// [loglevel YYYYMMDD HHMMSS.US  threadid file:line] OP result
	now := timeNow()
	_, file, line, ok := runtime.Caller(3) // It's always the same number of frames to the user's call.
	if !ok {
		file = "???"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		if slash >= 0 {
			file = file[slash+1:]
		}
	}
	if line < 0 {
		line = 0 // not a real line number, but acceptable to someDigits
	}
	if s > fatalLog {
		s = infoLog // for safety.
	}
	buf := l.getBuffer()

	// Avoid Fprintf, for speed. The format is so simple that we can do it quickly by hand.
	// It's worth about 3X. Fprintf is hard.
	buf.WriteString(now.Local().Format("2006-01-02 15:04:05.000")[:23])
	buf.WriteString(space)
	buf.tmp[0] = '['
	buf.tmp[1] = severityChar[s]
	buf.tmp[2] = ' '
	buf.nDigits(5, 3, pid) // TODO: should be TID
	buf.tmp[8] = ' '
	buf.Write(buf.tmp[:9])
	buf.WriteString(file)
	buf.tmp[0] = ':'
	n := buf.someDigits(1, line)
	buf.tmp[n+1] = ']'
	buf.tmp[n+2] = ' '
	buf.Write(buf.tmp[:n+3])
	return buf
}

func (l *loggingT) header1(s severity) *buffer {
	// Lmmdd hh:mm:ss.uuuuuu threadid file:line]
	now := timeNow()
	_, file, line, ok := runtime.Caller(3) // It's always the same number of frames to the user's call.
	if !ok {
		file = "???"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		if slash >= 0 {
			file = file[slash+1:]
		}
	}
	if line < 0 {
		line = 0 // not a real line number, but acceptable to someDigits
	}
	if s > fatalLog {
		s = infoLog // for safety.
	}
	buf := l.getBuffer()

	// Avoid Fprintf, for speed. The format is so simple that we can do it quickly by hand.
	// It's worth about 3X. Fprintf is hard.
	_, month, day := now.Date()
	hour, minute, second := now.Clock()
	buf.tmp[0] = severityChar[s]
	buf.twoDigits(1, int(month))
	buf.twoDigits(3, day)
	buf.tmp[5] = ' '
	buf.twoDigits(6, hour)
	buf.tmp[8] = ':'
	buf.twoDigits(9, minute)
	buf.tmp[11] = ':'
	buf.twoDigits(12, second)
	buf.tmp[14] = ' '
	buf.nDigits(5, 15, pid) // TODO: should be TID
	buf.tmp[20] = ' '
	buf.Write(buf.tmp[:21])
	buf.WriteString(file)
	buf.tmp[0] = ':'
	n := buf.someDigits(1, line)
	buf.tmp[n+1] = ']'
	buf.tmp[n+2] = ' '
	buf.Write(buf.tmp[:n+3])
	return buf
}

// Some custom tiny helper functions to print the log header efficiently.

const digits = "0123456789"

// twoDigits formats a zero-prefixed two-digit integer at buf.tmp[i].
func (buf *buffer) twoDigits(i, d int) {
	buf.tmp[i+1] = digits[d%10]
	d /= 10
	buf.tmp[i] = digits[d%10]
}

// nDigits formats a zero-prefixed n-digit integer at buf.tmp[i].
func (buf *buffer) nDigits(n, i, d int) {
	for j := n - 1; j >= 0; j-- {
		buf.tmp[i+j] = digits[d%10]
		d /= 10
	}
}

// someDigits formats a zero-prefixed variable-width integer at buf.tmp[i].
func (buf *buffer) someDigits(i, d int) int {
	// Print into the top, then copy down. We know there's space for at least
	// a 10-digit number.
	j := len(buf.tmp)
	for {
		j--
		buf.tmp[j] = digits[d%10]
		d /= 10
		if d == 0 {
			break
		}
	}
	return copy(buf.tmp[i:], buf.tmp[j:])
}

func (l *loggingT) println(s severity, args ...interface{}) {
	buf := l.header(s)
	fmt.Fprintln(buf, args...)
	l.output(s, buf)
}

func (l *loggingT) print(s severity, args ...interface{}) {
	buf := l.header(s)
	fmt.Fprint(buf, args...)
	if buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}
	l.output(s, buf)
}

func (l *loggingT) printf(s severity, format string, args ...interface{}) {
	buf := l.header(s)
	fmt.Fprintf(buf, format, args...)
	if buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}
	l.output(s, buf)
}

// output writes the data to the log files and releases the buffer.
func (l *loggingT) output(s severity, buf *buffer) {
	l.mu.Lock()
	if l.traceLocation.isSet() {
		_, file, line, ok := runtime.Caller(3) // It's always the same number of frames to the user's call (same as header).
		if ok && l.traceLocation.match(file, line) {
			buf.Write(stacks(false))
		}
	}
	data := buf.Bytes()
	if l.toStderr {
		os.Stderr.Write(data)
	} else {
		//if l.alsoToStderr || s >= l.stderrThreshold.get() {
		if (l.alsoToStderr || s >= l.stderrThreshold.get()) && s <= fatalLog {
			os.Stderr.Write(data)
		}
		if l.file[s] == nil {
			if err := l.createFiles(s); err != nil {
				os.Stderr.Write(data) // Make sure the message appears somewhere.
				l.exit(err)
				l.mu.Unlock()
				return
			}
		}
		//	if s <= fatalLog || l.http || s == ftpProtLog || s == failRecordLog || s == statusRecordLog || s == duohuoLog{

		switch s {
		case duohuoLog:
			l.file[duohuoLog].Write(data)
			//zxw
		case ftpProtLog:
			l.file[ftpProtLog].Write(data)
		case failRecordLog:
			l.file[failRecordLog].Write(data)
		case statusRecordLog:
			l.file[statusRecordLog].Write(data)
			//hujf
		case masterHttpLog:
			l.file[masterHttpLog].Write(data)
			//fallthrough
		case volumeHttpLog:
			l.file[volumeHttpLog].Write(data)
		case hissyncHttpLog:
			l.file[hissyncHttpLog].Write(data)
		case folderHttpLog:
			l.file[folderHttpLog].Write(data)
			//fallthrough
		case syncinterfaceHttpLog:
			l.file[syncinterfaceHttpLog].Write(data)
		case syncdatahandlingHttpLog:
			l.file[syncdatahandlingHttpLog].Write(data)
		case regionHttpLog:
			l.file[regionHttpLog].Write(data)
		case filerHttpLog:
			l.file[filerHttpLog].Write(data)
		case accountHttpLog:
			l.file[accountHttpLog].Write(data)
		case bucketHttpLog:
			l.file[bucketHttpLog].Write(data)
		case partitionHttpLog:
			l.file[partitionHttpLog].Write(data)
		case backupHttpLog:
			l.file[backupHttpLog].Write(data)
		case sepRecLog:
			l.file[sepRecLog].Write(data)
			//fallthrough
		case fatalLog:
			l.file[fatalLog].Write(data)
			fallthrough
		case errorLog:
			l.file[errorLog].Write(data)
			fallthrough
		case warningLog:
			l.file[warningLog].Write(data)
			fallthrough
		case infoLog:
			l.file[infoLog].Write(data)
		}
		//}
	}
	if s == fatalLog {
		// Make sure we see the trace for the current goroutine on standard error.
		if !l.toStderr {
			os.Stderr.Write(stacks(false))
		}
		// Write the stack trace for all goroutines to the files.
		trace := stacks(true)
		logExitFunc = func(error) {} // If we get a write error, we'll still exit below.
		for log := fatalLog; log >= infoLog; log-- {
			if f := l.file[log]; f != nil { // Can be nil if -logtostderr is set.
				f.Write(trace)
			}
		}
		l.mu.Unlock()
		timeoutFlush(10 * time.Second)
		os.Exit(255) // C++ uses -1, which is silly because it's anded with 255 anyway.
	}
	l.putBuffer(buf)
	l.mu.Unlock()
	if stats := severityStats[s]; stats != nil {
		atomic.AddInt64(&stats.lines, 1)
		atomic.AddInt64(&stats.bytes, int64(len(data)))
	}
}

// timeoutFlush calls Flush and returns when it completes or after timeout
// elapses, whichever happens first.  This is needed because the hooks invoked
// by Flush may deadlock when glog.Fatal is called from a hook that holds
// a lock.
func timeoutFlush(timeout time.Duration) {
	done := make(chan bool, 1)
	go func() {
		Flush() // calls logging.lockAndFlushAll()
		done <- true
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		fmt.Fprintln(os.Stderr, "glog: Flush took longer than", timeout)
	}
}

// stacks is a wrapper for runtime.Stack that attempts to recover the data for all goroutines.
func stacks(all bool) []byte {
	// We don't know how big the traces are, so grow a few times if they don't fit. Start large, though.
	n := 10000
	if all {
		n = 100000
	}
	var trace []byte
	for i := 0; i < 5; i++ {
		trace = make([]byte, n)
		nbytes := runtime.Stack(trace, all)
		if nbytes < len(trace) {
			return trace[:nbytes]
		}
		n *= 2
	}
	return trace
}

// logExitFunc provides a simple mechanism to override the default behavior
// of exiting on error. Used in testing and to guarantee we reach a required exit
// for fatal logs. Instead, exit could be a function rather than a method but that
// would make its use clumsier.
var logExitFunc func(error)

// exit is called if there is trouble creating or writing log files.
// It flushes the logs and exits the program; there's no point in hanging around.
// l.mu is held.
/*
func (l *loggingT) exit(err error) {
	fmt.Fprintf(os.Stderr, "log: exiting because of error: %s\n", err)
	// If logExitFunc is set, we do that instead of exiting.
	if logExitFunc != nil {
		logExitFunc(err)
		return
	}
	l.flushAll()
	os.Exit(2)
}
*/
func (l *loggingT) exit(err error) {
	//fmt.Fprintf(os.Stderr, "log: exiting because of error: %s\n", err)
	fmt.Fprintf(os.Stderr, "Warning: log can't be done  because of error: %s\n", err)
	// If logExitFunc is set, we do that instead of exiting.
	if logExitFunc != nil {
		logExitFunc(err)
		return
	}
	l.flushAll()
	//hujf
	//os.Exit(2)
}

// syncBuffer joins a bufio.Writer to its underlying file, providing access to the
// file's Sync method and providing a wrapper for the Write method that provides log
// file rotation. There are conflicting methods, so the file cannot be embedded.
// l.mu is held for all its methods.
type syncBuffer struct {
	logger *loggingT
	*bufio.Writer
	file   *os.File
	sev    severity
	nbytes uint64 // The number of bytes written to this file
}

func (sb *syncBuffer) Sync() error {
	return sb.file.Sync()
}

func (sb *syncBuffer) Write(p []byte) (n int, err error) {
	//if sb.sev <= fatalLog && sb.nbytes+uint64(len(p)) >= MaxSize {
	//	if err := sb.rotateFile(time.Now()); err != nil {
	//		sb.logger.exit(err)
	//	}
	//}
	n, err = sb.Writer.Write(p)
	sb.nbytes += uint64(n)
	if err != nil {
		sb.logger.exit(err)
		sb.Reset(sb.file)
	}
	return
}

// 2016-8-23:
// Instead of rotating the file, we now open an existed log file
func (sb *syncBuffer) openFile(now time.Time) (err error) {
	sb.file, _, err = open(severityName[sb.sev])
	sb.nbytes = 0
	if err != nil {
		return err
	}

	sb.Writer = bufio.NewWriterSize(sb.file, bufferSize)

	// Write header.
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Log file opened at: %s\n", now.Format("2006/01/02 15:04:05"))
	fmt.Fprintf(&buf, "Running on machine: %s\n", host)
	fmt.Fprintf(&buf, "Binary: Built with %s %s for %s/%s\n", runtime.Compiler, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	//fmt.Fprintf(&buf, "Log line format: [IWEF]mmdd hh:mm:ss threadid file:line] msg\n")
	fmt.Fprintf(&buf, "Log line format: [loglevel YYMMDD HHMMSS.US  threadid file:line] OP result msg\n")
	fmt.Fprintf(&buf, "Http line format: remote_addr host remote_user  [$time_local] request status body_bytes_sent http_referer http_user_agent http_x_forwarded_for\n")
	n, err := sb.file.Write(buf.Bytes())
	sb.nbytes += uint64(n)
	return err
}

func rotateLogByIdx(tag string){
	for i := MaxNum;i>=1;i--{
		newName := filepath.Join(logDirs[0], program + "." + tag+"."+fmt.Sprintf("%d",i))
		if i == MaxNum {
			os.Remove(newName)
		}
		oldname := filepath.Join(logDirs[0], program + "." + tag+"."+fmt.Sprintf("%d",i-1))
		fmt.Println("1old:",oldname,newName)
		os.Rename(oldname,newName)
	}
	newName := filepath.Join(logDirs[0], program + "." + tag+".0")
	oldname := filepath.Join(logDirs[0], program + "." + tag)
	fmt.Println("2old:",oldname,newName)
	os.Rename(oldname,newName)
}

// rotateFile closes the syncBuffer's file and starts a new one.
func (sb *syncBuffer) rotateFileByIdx(force bool) error {
	needRotate:=false

	if sb.file != nil {
		info,err := sb.file.Stat()
		if err == nil{
			size:=info.Size()
			if size >= MaxSize {
				needRotate = true
			}else if force == false{
				return nil
			}
		}
		sb.file.Close()
	}
	fmt.Println(" size:",MaxSize," MaxNum:",MaxNum)
	if force || needRotate{
		rotateLogByIdx(severityName[sb.sev])
	}
	var err error
	sb.file, _, err = create1(severityName[sb.sev])
	sb.nbytes = 0
	if err != nil {
		return err
	}

	sb.Writer = bufio.NewWriterSize(sb.file, bufferSize)

	// Write header.
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Log file created at: %s\n",time.Now().Format("2006/01/02 15:04:05"))
	fmt.Fprintf(&buf, "Running on machine: %s\n", host)
	fmt.Fprintf(&buf, "Binary: Built with %s %s for %s/%s\n", runtime.Compiler, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	//fmt.Fprintf(&buf, "Log line format: [IWEF]mmdd hh:mm:ss threadid file:line] msg\n")
	fmt.Fprintf(&buf, "Log line format: [loglevel YYMMDD HHMMSS.US  threadid file:line] OP result msg\n")
	fmt.Fprintf(&buf, "Http line format: remote_addr host remote_user  [$time_local] request status body_bytes_sent http_referer http_user_agent http_x_forwarded_for\n")
	n, err := sb.file.Write(buf.Bytes())
	sb.nbytes += uint64(n)
	return err
}
// rotateFile closes the syncBuffer's file and starts a new one.
func (sb *syncBuffer) rotateFile(now time.Time) error {
	if sb.file != nil {
		sb.Flush()
		sb.file.Close()
	}
	var err error
	//sb.file, _, err = create(severityName[sb.sev], now)
	//hujf
	//if sb.sev <= fatalLog {
	//	sb.file, _, err = create(severityName[sb.sev], now)
	//} else {
	sb.file, _, err = create1(severityName[sb.sev])
	//}
	sb.nbytes = 0
	if err != nil {
		return err
	}

	sb.Writer = bufio.NewWriterSize(sb.file, bufferSize)

	// Write header.
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Log file created at: %s\n", now.Format("2006/01/02 15:04:05"))
	fmt.Fprintf(&buf, "Running on machine: %s\n", host)
	fmt.Fprintf(&buf, "Binary: Built with %s %s for %s/%s\n", runtime.Compiler, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	//fmt.Fprintf(&buf, "Log line format: [IWEF]mmdd hh:mm:ss threadid file:line] msg\n")
	fmt.Fprintf(&buf, "Log line format: [loglevel YYMMDD HHMMSS.US  threadid file:line] OP result msg\n")
	fmt.Fprintf(&buf, "Http line format: remote_addr host remote_user  [$time_local] request status body_bytes_sent http_referer http_user_agent http_x_forwarded_for\n")
	n, err := sb.file.Write(buf.Bytes())
	sb.nbytes += uint64(n)
	return err
}

// bufferSize sizes the buffer associated with each log file. It's large
// so that log records can accumulate without the logging thread blocking
// on disk I/O. The flushDaemon will block instead.
const bufferSize = 256 * 1024

// createFiles creates all the log files for severity from sev down to infoLog.
// l.mu is held.
func (l *loggingT) createFiles(sev severity) error {
	now := time.Now()
	// Files are created in decreasing severity order, so as soon as we find one
	// has already been created, we can stop.
	for s := sev; s >= infoLog && l.file[s] == nil; s-- {
		sb := &syncBuffer{
			logger: l,
			sev:    s,
		}
		if err := sb.openFile(now); err != nil {
			return err
		}
		l.file[s] = sb
		if s > fatalLog {
			return nil
		}
	}
	return nil
}

//const flushInterval = 30 * time.Second
const flushInterval = 5 * time.Second

// flushDaemon periodically flushes the log file buffers.
func (l *loggingT) flushDaemon() {
	for _ = range time.NewTicker(flushInterval).C {
		l.lockAndFlushAll()
	}
}

// lockAndFlushAll is like flushAll but locks l.mu first.
func (l *loggingT) lockAndFlushAll() {
	l.mu.Lock()
	l.flushAll()
	l.mu.Unlock()
}

// flushAll flushes all the logs and attempts to "sync" their data to disk.
// l.mu is held.
func (l *loggingT) flushAll() {
	// Flush from fatal down, in case there's trouble flushing.
	// for s := fatalLog; s >= infoLog; s-- {
	for s := severity(numSeverity - 1); s >= infoLog; s-- {
		file := l.file[s]
		if file != nil {
			file.Flush() // ignore error
			file.Sync()  // ignore error
		}
	}
}

// setV computes and remembers the V level for a given PC
// when vmodule is enabled.
// File pattern matching takes the basename of the file, stripped
// of its .go suffix, and uses filepath.Match, which is a little more
// general than the *? matching used in C++.
// l.mu is held.
func (l *loggingT) setV(pc uintptr) Level {
	fn := runtime.FuncForPC(pc)
	file, _ := fn.FileLine(pc)
	// The file is something like /a/b/c/d.go. We want just the d.
	if strings.HasSuffix(file, ".go") {
		file = file[:len(file)-3]
	}
	if slash := strings.LastIndex(file, "/"); slash >= 0 {
		file = file[slash+1:]
	}
	for _, filter := range l.vmodule.filter {
		if filter.match(file) {
			l.vmap[pc] = filter.level
			return filter.level
		}
	}
	l.vmap[pc] = 0
	return 0
}

// Verbose is a boolean type that implements Infof (like Printf) etc.
// See the documentation of V for more information.
type Verbose bool

// V reports whether verbosity at the call site is at least the requested level.
// The returned value is a boolean of type Verbose, which implements Info, Infoln
// and Infof. These methods will write to the Info log if called.
// Thus, one may write either
//	if glog.V(2) { glog.Info("log this") }
// or
//	glog.V(2).Info("log this")
// The second form is shorter but the first is cheaper if logging is off because it does
// not evaluate its arguments.
//
// Whether an individual call to V generates a log record depends on the setting of
// the -v and --vmodule flags; both are off by default. If the level in the call to
// V is at least the value of -v, or of -vmodule for the source file containing the
// call, the V call will log.
func V(level Level) Verbose {
	// This function tries hard to be cheap unless there's work to do.
	// The fast path is two atomic loads and compares.

	// Here is a cheap but safe test to see if V logging is enabled globally.
	if logging.verbosity.get() >= level {
		return Verbose(true)
	}

	// It's off globally but it vmodule may still be set.
	// Here is another cheap but safe test to see if vmodule is enabled.
	if atomic.LoadInt32(&logging.filterLength) > 0 {
		// Now we need a proper lock to use the logging structure. The pcs field
		// is shared so we must lock before accessing it. This is fairly expensive,
		// but if V logging is enabled we're slow anyway.
		logging.mu.Lock()
		defer logging.mu.Unlock()
		if runtime.Callers(2, logging.pcs[:]) == 0 {
			return Verbose(false)
		}
		v, ok := logging.vmap[logging.pcs[0]]
		if !ok {
			v = logging.setV(logging.pcs[0])
		}
		return Verbose(v >= level)
	}
	return Verbose(false)
}

// Info is equivalent to the global Info function, guarded by the value of v.
// See the documentation of V for usage.
func (v Verbose) Info(args ...interface{}) {
	if v {
		logging.print(infoLog, args...)
	}
}

// Infoln is equivalent to the global Infoln function, guarded by the value of v.
// See the documentation of V for usage.
func (v Verbose) Infoln(args ...interface{}) {
	if v {
		logging.println(infoLog, args...)
	}
}

// Infof is equivalent to the global Infof function, guarded by the value of v.
// See the documentation of V for usage.
func (v Verbose) Infof(format string, args ...interface{}) {
	if v {
		logging.printf(infoLog, format, args...)
	}
}

// Info logs to the INFO log.
// Arguments are handled in the manner of fmt.Print; a newline is appended if missing.
func Info(args ...interface{}) {
	logging.print(infoLog, args...)
}

// Infoln logs to the INFO log.
// Arguments are handled in the manner of fmt.Println; a newline is appended if missing.
func Infoln(args ...interface{}) {
	logging.println(infoLog, args...)
}

// Infof logs to the INFO log.
// Arguments are handled in the manner of fmt.Printf; a newline is appended if missing.
func Infof(format string, args ...interface{}) {
	logging.printf(infoLog, format, args...)
}

// Warning logs to the WARNING and INFO logs.
// Arguments are handled in the manner of fmt.Print; a newline is appended if missing.
func Warning(args ...interface{}) {
	logging.print(warningLog, args...)
}

// Warningln logs to the WARNING and INFO logs.
// Arguments are handled in the manner of fmt.Println; a newline is appended if missing.
func Warningln(args ...interface{}) {
	logging.println(warningLog, args...)
}

// Warningf logs to the WARNING and INFO logs.
// Arguments are handled in the manner of fmt.Printf; a newline is appended if missing.
func Warningf(format string, args ...interface{}) {
	logging.printf(warningLog, format, args...)
}

// Error logs to the ERROR, WARNING, and INFO logs.
// Arguments are handled in the manner of fmt.Print; a newline is appended if missing.
func Error(args ...interface{}) {
	logging.print(errorLog, args...)
}

// Errorln logs to the ERROR, WARNING, and INFO logs.
// Arguments are handled in the manner of fmt.Println; a newline is appended if missing.
func Errorln(args ...interface{}) {
	logging.println(errorLog, args...)
}

// Errorf logs to the ERROR, WARNING, and INFO logs.
// Arguments are handled in the manner of fmt.Printf; a newline is appended if missing.
func Errorf(format string, args ...interface{}) {
	logging.printf(errorLog, format, args...)
}

// Fatal logs to the FATAL, ERROR, WARNING, and INFO logs,
// including a stack trace of all running goroutines, then calls os.Exit(255).
// Arguments are handled in the manner of fmt.Print; a newline is appended if missing.
func Fatal(args ...interface{}) {
	logging.print(fatalLog, args...)
}

// Fatalln logs to the FATAL, ERROR, WARNING, and INFO logs,
// including a stack trace of all running goroutines, then calls os.Exit(255).
// Arguments are handled in the manner of fmt.Println; a newline is appended if missing.
func Fatalln(args ...interface{}) {
	logging.println(fatalLog, args...)
}

// Fatalf logs to the FATAL, ERROR, WARNING, and INFO logs,
// including a stack trace of all running goroutines, then calls os.Exit(255).
// Arguments are handled in the manner of fmt.Printf; a newline is appended if missing.
func Fatalf(format string, args ...interface{}) {
	logging.printf(fatalLog, format, args...)
}

func Httpln(r *http.Request, status int, module string) {
	switch module {
	case "filer":
		logging.httpprintln(filerHttpLog, r, status, r.ContentLength, "")
	case "volume":
		logging.httpprintln(volumeHttpLog, r, status, r.ContentLength, "")
	case "master":
		logging.httpprintln(masterHttpLog, r, status, r.ContentLength, "")
	case "account":
		logging.httpprintln(accountHttpLog, r, status, r.ContentLength, "")
	case "SyncPlatformInterface":
		logging.httpprintln(syncinterfaceHttpLog, r, status, r.ContentLength, "")
	case "SyncDatahandling":
		logging.httpprintln(syncdatahandlingHttpLog, r, status, r.ContentLength, "")
	case "region":
		logging.httpprintln(regionHttpLog, r, status, r.ContentLength, "")
	case "bucket":
		logging.httpprintln(bucketHttpLog, r, status, r.ContentLength, "")
	case "folder":
		logging.httpprintln(folderHttpLog, r, status, r.ContentLength, "")
	}
}

func HttplnD(r *http.Request, status int, module string, startTime int64, endTime int64) {
	switch module {
	case "filer":
		logging.httpprintlnD(filerHttpLog, r, status, r.ContentLength, startTime, endTime, "")
	case "volume":
		logging.httpprintlnD(volumeHttpLog, r, status, r.ContentLength, startTime, endTime, "")
	case "master":
		logging.httpprintlnD(masterHttpLog, r, status, r.ContentLength, startTime, endTime, "")
	case "folder":
		logging.httpprintlnD(folderHttpLog, r, status, r.ContentLength, startTime, endTime, "")
	default:
		logging.httpprintlnD(infoLog, r, status, r.ContentLength, startTime, endTime, "")
	}

}
func HttplnWithLengthOrName(r *http.Request, status int, module string, bodyLength int64, fileName string, startTime int64, endTime int64) {
	switch module {
	case "filer":
		logging.httpprintlnD(filerHttpLog, r, status, bodyLength, startTime, endTime, fileName)
	case "volume":
		logging.httpprintlnD(volumeHttpLog, r, status, bodyLength, startTime, endTime, fileName)
	case "hissync":
		logging.httpprintlnD(hissyncHttpLog, r, status, bodyLength, startTime, endTime, fileName)
	case "region":
		logging.httpprintlnD(regionHttpLog, r, status, bodyLength, startTime, endTime, fileName)
	case "folder":
		logging.httpprintlnD(folderHttpLog, r, status, bodyLength, startTime, endTime, fileName)
	case "master":
		logging.httpprintlnD(masterHttpLog, r, status, bodyLength, startTime, endTime, fileName)
	default:
		logging.httpprintlnD(infoLog, r, status, bodyLength, startTime, endTime, "")
	}
}
func FailRecordLog(args ...interface{}) {
	logging.recordlog(failRecordLog, args...)
}
func StatusRecordLog(args ...interface{}) {
	logging.recordlog(statusRecordLog, args...)
}
func (l *loggingT) recordlog(s severity, args ...interface{}) {
	buf := l.getBuffer()
	if s != failRecordLog {
		buf.WriteString(time.Now().Local().Format("2006-01-02 15:04:05.456")[:23])
	}
	buf.WriteString(space)
	fmt.Fprintln(buf, args...)
	if buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}
	l.output(s, buf)

}

const (
	split        = "-"
	space        = " "
	leftbracket  = "["
	rightbracket = "]"
	comma        = ","
	quotation    = "\""
)

/*
func (l *loggingT) httpprintln(s severity, r *http.Request,status int) {
     buf := l.getBuffer()
     if parts := strings.Split(r.RemoteAddr, ":") ; len(parts) == 2 {
         buf.WriteString(parts[0])
     } else {
         buf.WriteString(split)
     }
     buf.WriteString(space)

     if parts := strings.Split(r.Host, ":");len(parts) == 2 {
        buf.WriteString(parts[0])
     }else {
        buf.WriteString(split)
     }
     buf.WriteString(space)

     if r.URL.User != nil && r.URL.User.Username() != "" {
        buf.WriteString(r.URL.User.Username())
     } else {
        buf.WriteString(split)
     }
     buf.WriteString(space)

     buf.WriteString(leftbracket + time.Now().Local().Format("02/Jan/2006:15:04:05 -0700") + rightbracket)

     buf.WriteString(space)

     buf.WriteString(r.Method + space + r.RequestURI + space + r.Proto)

     buf.WriteString(space)

     buf.WriteString(strconv.Itoa(status))

     buf.WriteString(space)

     buf.WriteString(strconv.FormatInt(r.ContentLength, 10))

     buf.WriteString(space)

     if r.Referer() != "" {
        buf.WriteString(r.Referer())
     } else {
        buf.WriteString(split)
     }

     buf.WriteString(space)

     buf.WriteString(r.UserAgent())

     buf.WriteString(space)

     n := len(r.Header["X-Forwarded-For"])
     if n == 0 {
        buf.WriteString(split)
     } else {
	     for i := 0; i < n; i ++ {
	         buf.WriteString(r.Header["X-Forwarded-For"][i])
	         if i < n {
	            buf.WriteString(comma)
	         }
	     }
     }
     if buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
     }
     l.output(s, buf)

}
*/

func (l *loggingT) httpprintln(s severity, r *http.Request, status int, bodyLength int64, fileName string) {
	buf := l.getBuffer()
	if parts := strings.Split(r.RemoteAddr, ":"); len(parts) == 2 {
		buf.WriteString(parts[0])
	} else {
		buf.WriteString(split)
	}
	buf.WriteString(space)

	if parts := strings.Split(r.Host, ":"); len(parts) == 2 {
		buf.WriteString(parts[0])
	} else {
		buf.WriteString(split)
	}
	buf.WriteString(space)

	if r.URL.User != nil && r.URL.User.Username() != "" {
		buf.WriteString(r.URL.User.Username())
	} else {
		buf.WriteString(split)
	}
	buf.WriteString(space)

	buf.WriteString(leftbracket + time.Now().Local().Format("02/Jan/2006:15:04:05 -0700") + rightbracket)

	buf.WriteString(space)

	buf.WriteString(quotation + r.Method + space + filepath.Join(r.RequestURI, fileName) + space + r.Proto + quotation)

	buf.WriteString(space)

	buf.WriteString(strconv.Itoa(status))

	buf.WriteString(space)

	buf.WriteString(strconv.FormatInt(bodyLength, 10))

	buf.WriteString(space)

	if r.Referer() != "" {
		buf.WriteString(quotation + r.Referer() + quotation)
	} else {
		buf.WriteString(split)
	}

	buf.WriteString(space)

	buf.WriteString(quotation + r.UserAgent() + quotation)

	buf.WriteString(space)
	/*
		     n := len(r.Header.Get("X-Forwarded-For"))
		     if n == 0 {
		        buf.WriteString(split)
		     } else {
		             buf.WriteString(quotation)
			     for i := 0; i < n; i ++ {
			         buf.WriteString(r.Header["X-Forwarded-For"][i])
			         if i < n {
			            buf.WriteString(comma)
			         }
			     }
		             buf.WriteString(quotation)
		     }
	*/
	if r.Header.Get("X-Forwarded-For") == "" {
		buf.WriteString(split)
	} else {
		buf.WriteString(r.Header.Get("X-Forwarded-For"))
	}
	if buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}
	l.output(s, buf)

}
func (l *loggingT) httpprintlnD(s severity, r *http.Request, status int, bodyLength int64, startTime int64, endTime int64, fileName string) {
	buf := l.getBuffer()

	buf.WriteString(time.Now().Local().Format("2006-01-02 15:04:05.000")[:23])
	buf.WriteString(space)

	if parts := strings.Split(r.RemoteAddr, ":"); len(parts) == 2 {
		buf.WriteString(parts[0])
	} else {
		buf.WriteString(split)
	}
	buf.WriteString(space)

	if parts := strings.Split(r.Host, ":"); len(parts) == 2 {
		buf.WriteString(parts[0])
	} else {
		buf.WriteString(split)
	}
	buf.WriteString(space)

	if r.URL.User != nil && r.URL.User.Username() != "" {
		buf.WriteString(r.URL.User.Username())
	} else {
		buf.WriteString(split)
	}
	buf.WriteString(space)

	if fileName != "" && s == filerHttpLog || fileName != "" && s == hissyncHttpLog {
		buf.WriteString(quotation + r.Method + space + fileName + space + r.Proto + quotation)
	} else {
		buf.WriteString(quotation + r.Method + space + filepath.Join(r.RequestURI, fileName) + space + r.Proto + quotation)
	}
	buf.WriteString(space)

	buf.WriteString(strconv.Itoa(status))

	buf.WriteString(space)

	buf.WriteString(strconv.FormatInt(bodyLength, 10))

	buf.WriteString(space)
	buf.WriteString(fmt.Sprint((float64(endTime) - float64(startTime)) / 1000000))
	buf.WriteString("ms")
	buf.WriteString(space)
	if r.Referer() != "" {
		buf.WriteString(quotation + r.Referer() + quotation)
	} else {
		buf.WriteString(split)
	}

	buf.WriteString(space)

	buf.WriteString(quotation + r.UserAgent() + quotation)
	buf.WriteString(space)
	buf.WriteString("From:" + r.RemoteAddr)
	buf.WriteString(space)
	buf.WriteString("To:" + r.Host)
	buf.WriteString(space)
	/*
		     n := len(r.Header.Get("X-Forwarded-For"))
		     if n == 0 {
		        buf.WriteString(split)
		     } else {
		             buf.WriteString(quotation)
			     for i := 0; i < n; i ++ {
			         buf.WriteString(r.Header["X-Forwarded-For"][i])
			         if i < n {
			            buf.WriteString(comma)
			         }
			     }
		             buf.WriteString(quotation)
		     }
	*/
	if r.Header.Get("X-Forwarded-For") == "" {
		buf.WriteString(split)
	} else {
		buf.WriteString(r.Header.Get("X-Forwarded-For"))
	}
	if buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}
	l.output(s, buf)

}
func (v Verbose) InfoNewf(op string, ret string, msg ...interface{}) {
	if v {
		args := []interface{}{op, ret}
		args = append(args, msg...)
		logging.println(infoLog, args...)
	}
}

func SetVerbosity(v int) {
	logging.verbosity.set(Level(v))
}

func RotateHttpFile() {
	logging.mu.Lock()
	defer logging.mu.Unlock()
	defer func(previous func(error)) { logExitFunc = previous }(logExitFunc)
	logExitFunc = func(e error) {}
	//for s := filerHttpLog; s <= masterHttpLog; s++ {
	for s := infoLog; s < numSeverity; s++ {
		if logging.file[s] != nil {
			sb := logging.file[s]
			sb.rotateFile(time.Now())
		}
	}
}

func RotateLogFile(force bool){
	logging.mu.Lock()
	defer logging.mu.Unlock()
	//for s := filerHttpLog; s <= masterHttpLog; s++ {
	for s := infoLog; s < numSeverity; s++ {
		if logging.file[s] != nil {
			sb := logging.file[s]
			sb.rotateFileByIdx(force)
		}
	}
}
/*
uuid,               连接的uuid
host,                filerserver的ip
remoteaddr,          远端地址，控制链的地址
RequestURI,          请求的参数（所有）
status,              命名的执行状态
Method,              命令
request_length_str,  从客户端获取到的长度
response_length_str, 发送给客户端的长度
spendtime,           总的操作时间
splittimes,          需要分割，获取到操作filer的时间
dataconn,              替代为数据链的ip+port
Username,            登录的用户名
Fileip               使用的filer地址
*/
func FtpProtln(uuid, host, remoteaddr, RequestURI, status, Method, request_length_str, response_length_str, spendtime, splittimes, dataconn, Username, filerip, errmess string) {
	logging.LogProtln(ftpProtLog,
		time.Now().Local().Format("2006-01-02T15:04:05.999+08:00"),
		"FTP",
		uuid,
		host,
		remoteaddr,
		RequestURI,
		status,
		Method,
		request_length_str,
		response_length_str,
		spendtime,
		splittimes,
		dataconn,
		Username,
		filerip,
		"",
		"",
		"",
		"",      //realpath
		"",      //lifecycle
		errmess, //errmess
	)
}

func SFtpProtln(uuid, host, remoteaddr, RequestURI, status, Method, request_length_str, response_length_str, spendtime, splittimes, dataconn, Username, filerip, errmess string) {
	logging.LogProtln(ftpProtLog,
		time.Now().Local().Format("2006-01-02T15:04:05.999+08:00"),
		"SFTP",
		uuid,
		host,
		remoteaddr,
		RequestURI,
		status,
		Method,
		request_length_str,
		response_length_str,
		spendtime,
		splittimes,
		dataconn,
		Username,
		filerip,
		"",
		"",
		"",
		"",      //realpath
		"",      //lifecycle
		errmess, //errmess
	)
}

/*
fromIP     主端的filer地址
toIP       备端的backup地址
method      binlog类型
filepath    文件路径
status      处理状态
bodyLength  文件大小
binlogtime  binlog文件记录时间
startTime   开始处理时间（再次读取出来准备分配的时间）
endTime     处理结束时间（收到ack）
*/
func DuohuoProtln(fromIP, toIP, binlogtype, filepath string, status int, bodyLength, binlogtime, startTime, endTime int64) {
	logging.DuohuoProtln(fromIP, toIP, binlogtype, filepath, status, bodyLength, binlogtime, startTime, endTime)
}

func (l *loggingT) DuohuoProtln(fromIP, toIP, binlogtype, filepath string, status int, bodyLength, binlogtime, startTime, endTime int64) {

	buf := l.getBuffer()

	buf.WriteString(time.Now().Local().Format("2006-01-02 15:04:05.000")[:23])
	buf.WriteString(space)

	buf.WriteString(binlogtype)

	buf.WriteString(space)

	buf.WriteString(filepath)

	buf.WriteString(space)

	buf.WriteString(strconv.Itoa(status))

	buf.WriteString(space)

	buf.WriteString(strconv.FormatInt(bodyLength, 10))

	buf.WriteString(space)

	buf.WriteString(fmt.Sprint((endTime - startTime) / 1000000))
	buf.WriteString("ms")
	buf.WriteString(space)

	buf.WriteString(fmt.Sprint((endTime - binlogtime) / 1000000))
	buf.WriteString("ms")

	buf.WriteString(space)
	buf.WriteString("From:" + fromIP)
	buf.WriteString(space)
	buf.WriteString("To:" + toIP)
	buf.WriteString(space)

	if buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}
	l.output(duohuoLog, buf)

}

//func FtpProtln(fromIP, toIP, sessionID, method, fileName string, status int, bodyLength, startTime, endTime int64) {
func SftpProtln(fromIP, toIP, user, method, fileName string, status uint32, startTime, endTime int64) {

	logging.sftpProtln(fromIP, toIP, user, method, fileName, status, startTime, endTime)
}

func (l *loggingT) sftpProtln(fromIP, toIP, user, method string, fileName string, status uint32, startTime, endTime int64) {

	buf := l.getBuffer()

	buf.WriteString(time.Now().Local().Format("2006-01-02 15:04:05.000")[:23])
	buf.WriteString(space)

	buf.WriteString(user)

	buf.WriteString(space)

	buf.WriteString(method)

	buf.WriteString(space)
	buf.WriteString(fileName)

	buf.WriteString(space)
	buf.WriteString(strconv.Itoa(int(status)))

	buf.WriteString(space)
	buf.WriteString(fmt.Sprint((float64(endTime) - float64(startTime)) / 1000000))
	buf.WriteString("ms")
	buf.WriteString(space)

	buf.WriteString("From:" + fromIP)
	buf.WriteString(space)
	buf.WriteString("To:" + toIP)
	buf.WriteString(space)

	if buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}
	l.output(ftpProtLog, buf)

}

//按顺序，按模块打印http日志。
//参数以空格分割
func (l *loggingT) LogProtln(logMode severity, members ...string) {

	buf := l.getBuffer()

	for i := 0; i < len(members); i++ {
		//空的 "-"  代替
		if members[i] == "" {
			members[i] = split
		}
		//原有的双引号变为单引号
		strings.Replace(members[i], quotation, "'", -1)
		//带有空格的，用双引号包含
		if strings.Contains(members[i], " ") {
			members[i] = quotation + members[i] + quotation
		}

		buf.WriteString(members[i])
		buf.WriteString(space)
	}

	if buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}

	l.output(logMode, buf)
}

/*
r Request
status   请求处理状态
bodyLength   上传/下载的文件大小
fileName     文件的最终名字
ctx
*/
func FilerHttpLog(r *http.Request, respstatus int, contentLength int64, fileName string, startTime, endTime int64, hostip string, ctx *context.Context) {
	if !HttpLogEnable {
		return
	}
	var logtime, reqid, host, remoteaddr, RequestURI, status, Method, request_length_str, response_length_str, spendtime, splittimes, sessid, Username, Proto, Referer, UserAgent, Forwarded, realpath, lifecycle, errmess,origin string


	//logtime
	logtime = time.Now().Local().Format("2006-01-02T15:04:05.999+08:00")

	//reqid
	if ctx != nil {
		reqid, _ = ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
	}

	//host
	host = hostip

	//remoteaddr
	if parts := strings.Split(r.RemoteAddr, ":"); len(parts) == 2 {
		remoteaddr = parts[0]
	} else {
		remoteaddr = r.RemoteAddr
	}

	//RequestURI
	if fileName != "" { //系统定义的bucket上传才会filename不为空
		RequestURI = fileName
		realpath = r.RequestURI
	} else {
		RequestURI = r.RequestURI
		var err error
		RequestURI, err = url.QueryUnescape(RequestURI)
		if err != nil {
			RequestURI = r.URL.Path
		}
	}

	//status
	status = fmt.Sprint(respstatus)

	//Method
	Method = r.Method
	origin = r.Header.Get("Origin")
	//request_length_str response_length_str
	if r.Method == "POST" || r.Method == "PUT" {
		request_length_str = fmt.Sprint(contentLength)
	} else {
		response_length_str = fmt.Sprint(contentLength)
	}
	//spendtime
	spendtime = fmt.Sprint((float64(endTime)-float64(startTime))/1000000, "ms")
	//splittimes
	if ctx != nil {
		splittimes, _ = ((*ctx).Value(public.SDOSS_SPLIT_TIME)).(string)
	}

	//errmess
	if ctx != nil {
		errmess, _ = ((*ctx).Value(public.SDOSS_ERROR_MESSAGE)).(string)
	}
	//sessid
	if ctx != nil {
		sessid, _ = ((*ctx).Value(public.SDOSS_SESSION_ID)).(string)
	}

	//Username
	if r.URL.User != nil && r.URL.User.Username() != "" {
		Username = r.URL.User.Username()
	}

	//Proto
	Proto = r.Proto

	//Referer
	Referer = r.Referer()

	//UserAgent
	UserAgent = r.UserAgent()

	//Forwarded
	Forwarded = r.Header.Get("X-Forwarded-For")

	if r.Method == "POST" || r.Method == "PUT" {
		//lifecycle = r.Header.Get("life-cycle")
		if ctx != nil {
			lifecycle, _ = ((*ctx).Value("life-cycle")).(string)
		}
		if lifecycle == "" {
			lifecycle = r.Header.Get("life-cycle")
		}
	} else {
		//实际是range
		if ctx != nil {
			lifecycle, _ = ((*ctx).Value("Range")).(string)
		}
	}

	logging.LogProtln(filerHttpLog,
		logtime,
		"FILER",
		reqid,
		host,
		remoteaddr,
		RequestURI,
		status,
		Method,
		request_length_str,
		response_length_str,
		spendtime,
		splittimes,
		sessid,
		Username, Proto,
		Referer,
		UserAgent,
		Forwarded,
		realpath,
		lifecycle,
		origin,
		errmess,
	)

}

//volume 的日志，暂时是跟filer格式基本类似
func VolumeHttpLog(r *http.Request, respstatus int, contentLength int64, startTime, endTime int64, hostip,collection string, ctx *context.Context) {
	if !HttpLogEnable {
		return
	}

	//
	var logtime, reqid, host, remoteaddr, RequestURI, status, Method, request_length_str, response_length_str, spendtime, splittimes, sessid, Username, Proto, Referer, UserAgent, Forwarded string
	var moduleName = "VOLUME"
	if collection == public.EC{
		moduleName = "ECVOLUME"
	}
	//logtime
	logtime = time.Now().Local().Format("2006-01-02T15:04:05.999+08:00")

	//reqid
	if ctx != nil {
		reqid, _ = ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
	}
	//host
	host = hostip

	//remoteaddr
	if parts := strings.Split(r.RemoteAddr, ":"); len(parts) == 2 {
		remoteaddr = parts[0]
	} else {
		remoteaddr = r.RemoteAddr
	}
	//RequestURI
	RequestURI = r.RequestURI

	//status
	status = fmt.Sprint(respstatus)

	//Method
	Method = r.Method

	//request_length_str, response_length_str
	if r.Method == "POST" || r.Method == "PUT" {
		request_length_str = fmt.Sprint(contentLength)
	} else {
		response_length_str = fmt.Sprint(contentLength)
	}

	//spendtime
	spendtime = fmt.Sprint((float64(endTime)-float64(startTime))/1000000, "ms")

	//splittimes
	if ctx != nil {
		splittimes, _ = ((*ctx).Value(public.SDOSS_SPLIT_TIME)).(string)
	}
	//sessid

	//Username
	if r.URL.User != nil && r.URL.User.Username() != "" {
		Username = r.URL.User.Username()
	}

	//Proto
	Proto = r.Proto

	//Referer
	Referer = r.Referer()

	//UserAgent
	UserAgent = r.UserAgent()

	//Forwarded
	Forwarded = r.Header.Get("X-Forwarded-For")

	//logging.LogProtln(volumeHttpLog, logtime, reqid, remoteaddr, host, Username, Method, RequestURI, Proto, status, request_length_str, response_length_str, spendtime, splittimes, Referer, UserAgent, Forwarded)
	logging.LogProtln(volumeHttpLog,
		logtime,
		moduleName,
		reqid,
		host,
		remoteaddr,
		RequestURI,
		status,
		Method,
		request_length_str,
		response_length_str,
		spendtime,
		splittimes,
		sessid,
		Username, Proto,
		Referer,
		UserAgent,
		Forwarded,
		"", //realpath
		r.Header.Get("Range"), //Range
		"", //errmess
	)
}

//folder 的日志，样式参考volume
func FolderHttpLog(r *http.Request, respstatus int, contentLength int64, startTime, endTime int64, hostip string, ctx *context.Context) {
	if !HttpLogEnable {
		return
	}

	//
	var logtime, reqid, host, remoteaddr, RequestURI, status, Method, request_length_str, response_length_str, spendtime, splittimes, sessid, Username, Proto, Referer, UserAgent, Forwarded string
	//logtime
	logtime = time.Now().Local().Format("2006-01-02T15:04:05.999+08:00")

	//reqid
	if ctx != nil {
		reqid, _ = ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
	}
	//host
	host = hostip

	//remoteaddr
	if parts := strings.Split(r.RemoteAddr, ":"); len(parts) == 2 {
		remoteaddr = parts[0]
	} else {
		remoteaddr = r.RemoteAddr
	}
	//RequestURI
	RequestURI = r.RequestURI

	//status
	status = fmt.Sprint(respstatus)

	//Method
	Method = r.Method

	//request_length_str, response_length_str
	if r.Method == "POST" || r.Method == "PUT" {
		request_length_str = fmt.Sprint(contentLength)
	} else {
		response_length_str = fmt.Sprint(contentLength)
	}

	//spendtime
	spendtime = fmt.Sprint((float64(endTime)-float64(startTime))/1000000, "ms")

	//splittimes
	if ctx != nil {
		splittimes, _ = ((*ctx).Value(public.SDOSS_SPLIT_TIME)).(string)
	}
	//sessid

	//Username
	if r.URL.User != nil && r.URL.User.Username() != "" {
		Username = r.URL.User.Username()
	}

	//Proto
	Proto = r.Proto

	//Referer
	Referer = r.Referer()

	//UserAgent
	UserAgent = r.UserAgent()

	//Forwarded
	Forwarded = r.Header.Get("X-Forwarded-For")

	//logging.LogProtln(volumeHttpLog, logtime, reqid, remoteaddr, host, Username, Method, RequestURI, Proto, status, request_length_str, response_length_str, spendtime, splittimes, Referer, UserAgent, Forwarded)
	logging.LogProtln(folderHttpLog,
		logtime,
		"FOLDER",
		reqid,
		host,
		remoteaddr,
		RequestURI,
		status,
		Method,
		request_length_str,
		response_length_str,
		spendtime,
		splittimes,
		sessid,
		Username, Proto,
		Referer,
		UserAgent,
		Forwarded,
		"", //realpath
		r.Header.Get("Range"), //Range
		"", //errmess
	)
}

func PartiTionHttpLog(r *http.Request, respstatus int, startTime, endTime int64, hostip string, ctx *context.Context) {
	if !HttpLogEnable {
		return
	}
	//
	var logtime, reqid, host, remoteaddr, RequestURI, status, Method, request_length_str, response_length_str, spendtime, splittimes, sessid, Username, Proto, Referer, UserAgent, Forwarded string
	//logtime
	logtime = time.Now().Local().Format("2006-01-02T15:04:05.999+08:00")

	//reqid
	if ctx != nil {
		reqid, _ = ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
	}

	//host
	host = hostip

	//remoteaddr
	if parts := strings.Split(r.RemoteAddr, ":"); len(parts) == 2 {
		remoteaddr = parts[0]
	} else {
		remoteaddr = r.RemoteAddr
	}

	//RequestURI
	RequestURI = r.RequestURI

	//status
	status = fmt.Sprint(respstatus)
	//Method
	Method = r.Method
	if strings.HasPrefix(RequestURI, "/handfind") || strings.HasPrefix(RequestURI, "/find") || strings.HasPrefix(RequestURI, "/listfiles") || strings.HasPrefix(RequestURI, "/list") || strings.HasPrefix(RequestURI, "/recycle") {
		Method = "GET"
	}
	if strings.HasPrefix(RequestURI, "/delete") {
		Method = "DELETE"
	}
	//request_length_str, response_length_str

	//spendtime
	spendtime = fmt.Sprint((float64(endTime)-float64(startTime))/1000000, "ms")

	//splittimes

	//sessid

	//Username
	if r.URL.User != nil && r.URL.User.Username() != "" {
		Username = r.URL.User.Username()
	}
	//Proto
	Proto = r.Proto
	//Referer
	Referer = r.Referer()
	//UserAgent
	UserAgent = r.UserAgent()
	//Forwarded
	Forwarded = r.Header.Get("X-Forwarded-For")

	//logging.LogProtln(partitionHttpLog, logtime, reqid, remoteaddr, host, Username, Method, RequestURI, Proto, status, filepath, spendtime, Referer, UserAgent, Forwarded)
	logging.LogProtln(partitionHttpLog,
		logtime,
		"PARTITION",
		reqid,
		host,
		remoteaddr,
		RequestURI,
		status,
		Method,
		request_length_str,
		response_length_str,
		spendtime,
		splittimes,
		sessid,
		Username, Proto,
		Referer,
		UserAgent,
		Forwarded,
		"", //realpath
		"", //lifecycle
		"", //errmess
	)
}

func MasterHttpLog(r *http.Request, respstatus int, startTime, endTime int64, hostip string, ctx *context.Context) {
	if !HttpLogEnable {
		return
	}
	//
	var logtime, reqid, host, remoteaddr, RequestURI, status, Method, request_length_str, response_length_str, spendtime, splittimes, sessid, Username, Proto, Referer, UserAgent, Forwarded string
	//logtime
	logtime = time.Now().Local().Format("2006-01-02T15:04:05.999+08:00")

	//reqid
	if ctx != nil {
		reqid, _ = ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
	}
	//host
	host = hostip

	//remoteaddr
	if parts := strings.Split(r.RemoteAddr, ":"); len(parts) == 2 {
		remoteaddr = parts[0]
	} else {
		remoteaddr = r.RemoteAddr
	}

	//RequestURI
	RequestURI = r.RequestURI

	//status
	status = fmt.Sprint(respstatus)

	//Method
	Method = r.Method

	// request_length_str, response_length_str

	//spendtime
	spendtime = fmt.Sprint((float64(endTime)-float64(startTime))/1000000, "ms")

	//splittimes

	//	sessid

	// Username
	if r.URL.User != nil && r.URL.User.Username() != "" {
		Username = r.URL.User.Username()
	}
	// Proto,
	Proto = r.Proto
	// Referer,
	Referer = r.Referer()
	// UserAgent,
	UserAgent = r.UserAgent()

	//Forwarded
	Forwarded = r.Header.Get("X-Forwarded-For")

	//	logging.LogProtln(masterHttpLog, logtime, reqid, remoteaddr, host, Username, Method, RequestURI, Proto, status, spendtime, Referer, UserAgent, Forwarded)
	logging.LogProtln(masterHttpLog,
		logtime,
		"MASTER",
		reqid,
		host,
		remoteaddr,
		RequestURI,
		status,
		Method,
		request_length_str,
		response_length_str,
		spendtime,
		splittimes,
		sessid,
		Username, Proto,
		Referer,
		UserAgent,
		Forwarded,
		"", //realpath
		"", //lifecycle
		"", //errmess
	)
}

func AccountHttpLog(r *http.Request, respstatus int, startTime, endTime int64, hostip string, ctx *context.Context) {
	if !HttpLogEnable {
		return
	}
	//
	var logtime, reqid, host, remoteaddr, RequestURI, status, Method, request_length_str, response_length_str, spendtime, splittimes, sessid, Username, Proto, Referer, UserAgent, Forwarded string

	//logtime,
	logtime = time.Now().Local().Format("2006-01-02T15:04:05.999+08:00")
	// reqid,
	if ctx != nil {
		reqid, _ = ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
	}
	// host,
	host = hostip
	// remoteaddr,
	if parts := strings.Split(r.RemoteAddr, ":"); len(parts) == 2 {
		remoteaddr = parts[0]
	} else {
		remoteaddr = r.RemoteAddr
	}
	// RequestURI,
	RequestURI = r.RequestURI

	// status,
	status = fmt.Sprint(respstatus)

	// Method,
	Method = r.Method

	// request_length_str, response_length_str,

	// spendtime,
	spendtime = fmt.Sprint((float64(endTime)-float64(startTime))/1000000, "ms")

	// splittimes,

	// sessid,

	// Username,
	if r.URL.User != nil && r.URL.User.Username() != "" {
		Username = r.URL.User.Username()
	}

	// Proto,
	Proto = r.Proto

	// Referer,
	Referer = r.Referer()

	// UserAgent,
	UserAgent = r.UserAgent()

	// Forwarded
	Forwarded = r.Header.Get("X-Forwarded-For")

	logging.LogProtln(accountHttpLog,
		logtime,
		"ACCOUNT",
		reqid,
		host,
		remoteaddr,
		RequestURI,
		status,
		Method,
		request_length_str,
		response_length_str,
		spendtime,
		splittimes,
		sessid,
		Username, Proto,
		Referer,
		UserAgent,
		Forwarded,
		"", //realpath
		"", //lifecycle
		"", //errmess
	)
}

func BucketHttpLog(r *http.Request, respstatus int, path string, startTime, endTime int64, hostip string, ctx *context.Context) {
	if !HttpLogEnable {
		return
	}
	var errmsg string
	var logtime, reqid, host, remoteaddr, RequestURI, status, Method, request_length_str, response_length_str, spendtime, splittimes, sessid, Username, Proto, Referer, UserAgent, Forwarded string

	//logtime,
	logtime = time.Now().Local().Format("2006-01-02T15:04:05.999+08:00")

	// reqid,
	if ctx != nil {
		reqid, _ = ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
		errmsg, _ = ((*ctx).Value(public.SDOSS_ERROR_MESSAGE)).(string)
	}
	// host,
	host = hostip
	// remoteaddr,
	if parts := strings.Split(r.RemoteAddr, ":"); len(parts) == 2 {
		remoteaddr = parts[0]
	} else {
		remoteaddr = r.RemoteAddr
	}
	// RequestURI,
	RequestURI = r.RequestURI
	if path != "" {
		RequestURI += split + path
	}
	// status,
	status = fmt.Sprint(respstatus)

	// Method,
	Method = r.Method

	// request_length_str, response_length_str,

	// spendtime,
	spendtime = fmt.Sprint((float64(endTime)-float64(startTime))/1000000, "ms")

	// splittimes,
	// sessid,
	// Username,
	if r.URL.User != nil && r.URL.User.Username() != "" {
		Username = r.URL.User.Username()
	}
	// Proto,
	Proto = r.Proto

	// Referer,
	Referer = r.Referer()
	// UserAgent,
	UserAgent = r.UserAgent()
	// Forwarded string
	Forwarded = r.Header.Get("X-Forwarded-For")

	//logging.LogProtln(bucketHttpLog, logtime, reqid, remoteaddr, host, Username, Method, RequestURI, accountbucket, Proto, status, spendtime, Referer, UserAgent, Forwarded)
	logging.LogProtln(bucketHttpLog,
		logtime,
		"BUCKET",
		reqid,
		host,
		remoteaddr,
		RequestURI,
		status,
		Method,
		request_length_str,
		response_length_str,
		spendtime,
		splittimes,
		sessid,
		Username, Proto,
		Referer,
		UserAgent,
		Forwarded,
		"", //realpath
		"", //lifecycle
		errmsg, //errmess
	)
}

func SetHttpLogEnable(v bool) {
	HttpLogEnable = v
}

func RegionHttpLog(r *http.Request, respstatus int, contentLength int64, fileName string, startTime, endTime int64, hostip string, reqid, splittimes, errmess string) {

	if !logging.http {
		return
	}
	var logtime, host, remoteaddr, RequestURI, status, Method, request_length_str, response_length_str, spendtime, Username, Proto, Referer, UserAgent, Forwarded, realpath, lifecycle string

	//logtime
	logtime = time.Now().Local().Format("2006-01-02T15:04:05.999+08:00")

	//reqid

	//host
	host = hostip

	//remoteaddr
	if parts := strings.Split(r.RemoteAddr, ":"); len(parts) == 2 {
		remoteaddr = parts[0]
	} else {
		remoteaddr = r.RemoteAddr
	}
	//RequestURI
	if fileName != "" { //
		RequestURI = fileName
		realpath = r.URL.Path
	} else {
		RequestURI = r.RequestURI
		var err error
		RequestURI, err = url.QueryUnescape(RequestURI)
		if err != nil {
			RequestURI = r.URL.Path
		}
	}

	//status
	status = fmt.Sprint(respstatus)

	//Method
	Method = r.Method

	//request_length_str response_length_str
	if r.Method == "POST" || r.Method == "PUT" {
		request_length_str = fmt.Sprint(contentLength)
	} else {
		response_length_str = fmt.Sprint(contentLength)
	}
	//spendtime
	spendtime = fmt.Sprint((float64(endTime)-float64(startTime))/1000000, "ms")
	//splittimes

	//errmess

	//sessid

	//Username
	if r.URL.User != nil && r.URL.User.Username() != "" {
		Username = r.URL.User.Username()
	}
	//Proto
	Proto = r.Proto

	//Referer
	Referer = r.Referer()

	//UserAgent
	UserAgent = r.UserAgent()

	//Forwarded
	Forwarded = r.Header.Get("X-Forwarded-For")

	lifecycle = r.Header.Get("life-cycle")
	logging.LogProtln(regionHttpLog, logtime,
		"REGION",
		reqid,
		host,
		remoteaddr,
		RequestURI,
		status,
		Method,
		request_length_str,
		response_length_str,
		spendtime,
		splittimes,
		"",
		Username,
		Proto,
		Referer,
		UserAgent,
		Forwarded,
		realpath,
		lifecycle,
		errmess,
	)
}


func SyncInterfaceHttpLog(r *http.Request, respstatus int, contentLength int64, fileName string, startTime, endTime int64, hostip string, reqid, splittimes, errmess string) {

	if !logging.http {
		return
	}
	var logtime, host, remoteaddr, RequestURI, status, Method, request_length_str, response_length_str, spendtime, Username, Proto, Referer, UserAgent, Forwarded, realpath, lifecycle string

	//logtime
	logtime = time.Now().Local().Format("2006-01-02T15:04:05.999+08:00")

	//reqid

	//host
	host = hostip

	//remoteaddr
	if parts := strings.Split(r.RemoteAddr, ":"); len(parts) == 2 {
		remoteaddr = parts[0]
	} else {
		remoteaddr = r.RemoteAddr
	}
	//RequestURI
	if fileName != "" { //
		RequestURI = fileName
		realpath = r.URL.Path
	} else {
		RequestURI = r.RequestURI
		var err error
		RequestURI, err = url.QueryUnescape(RequestURI)
		if err != nil {
			RequestURI = r.URL.Path
		}
	}

	//status
	status = fmt.Sprint(respstatus)

	//Method
	Method = r.Method

	//request_length_str response_length_str
	if r.Method == "POST" || r.Method == "PUT" {
		request_length_str = fmt.Sprint(contentLength)
	} else {
		response_length_str = fmt.Sprint(contentLength)
	}
	//spendtime
	spendtime = fmt.Sprint((float64(endTime)-float64(startTime))/1000000, "ms")
	//splittimes

	//errmess

	//sessid

	//Username
	if r.URL.User != nil && r.URL.User.Username() != "" {
		Username = r.URL.User.Username()
	}
	//Proto
	Proto = r.Proto

	//Referer
	Referer = r.Referer()

	//UserAgent
	UserAgent = r.UserAgent()

	//Forwarded
	Forwarded = r.Header.Get("X-Forwarded-For")

	lifecycle = r.Header.Get("life-cycle")
	logging.LogProtln(syncinterfaceHttpLog, logtime,
		"SYNCINTERFACE",
		reqid,
		host,
		remoteaddr,
		RequestURI,
		status,
		Method,
		request_length_str,
		response_length_str,
		spendtime,
		splittimes,
		"",
		Username,
		Proto,
		Referer,
		UserAgent,
		Forwarded,
		realpath,
		lifecycle,
		errmess,
	)

}


func SyncDatahandlingHttpLog(r *http.Request, respstatus int, contentLength int64, fileName string, startTime, endTime int64, hostip string, reqid, splittimes, errmess string) {

	if !logging.http {
		return
	}
	var logtime, host, remoteaddr, RequestURI, status, Method, request_length_str, response_length_str, spendtime, Username, Proto, Referer, UserAgent, Forwarded, realpath, lifecycle string

	//logtime
	logtime = time.Now().Local().Format("2006-01-02T15:04:05.999+08:00")

	//reqid

	//host
	host = hostip

	//remoteaddr
	if parts := strings.Split(r.RemoteAddr, ":"); len(parts) == 2 {
		remoteaddr = parts[0]
	} else {
		remoteaddr = r.RemoteAddr
	}
	//RequestURI
	if fileName != "" { //
		RequestURI = fileName
		realpath = r.URL.Path
	} else {
		RequestURI = r.RequestURI
		var err error
		RequestURI, err = url.QueryUnescape(RequestURI)
		if err != nil {
			RequestURI = r.URL.Path
		}
	}

	//status
	status = fmt.Sprint(respstatus)

	//Method
	Method = r.Method

	//request_length_str response_length_str
	if r.Method == "POST" || r.Method == "PUT" {
		request_length_str = fmt.Sprint(contentLength)
	} else {
		response_length_str = fmt.Sprint(contentLength)
	}
	//spendtime
	spendtime = fmt.Sprint((float64(endTime)-float64(startTime))/1000000, "ms")
	//splittimes

	//errmess

	//sessid

	//Username
	if r.URL.User != nil && r.URL.User.Username() != "" {
		Username = r.URL.User.Username()
	}
	//Proto
	Proto = r.Proto

	//Referer
	Referer = r.Referer()

	//UserAgent
	UserAgent = r.UserAgent()

	//Forwarded
	Forwarded = r.Header.Get("X-Forwarded-For")

	lifecycle = r.Header.Get("life-cycle")
	logging.LogProtln(syncdatahandlingHttpLog, logtime,
		"SYNCDATAHANDLING",
		reqid,
		host,
		remoteaddr,
		RequestURI,
		status,
		Method,
		request_length_str,
		response_length_str,
		spendtime,
		splittimes,
		"",
		Username,
		Proto,
		Referer,
		UserAgent,
		Forwarded,
		realpath,
		lifecycle,
		errmess,
	)
}


func BackupHttpLog(remoteaddr,Method,Opcode,fileName,contentLength,Id,lifecycle string,respstatus int,startTime, endTime int64, host string, ctx *context.Context) {
	if !HttpLogEnable {
		return
	}
	var reqid,  RequestURI, status, request_length_str, response_length_str, spendtime, splittimes, sessid,  Proto, Referer, UserAgent, Forwarded, errmess string

	//logtime
	logtime := time.Now().Local().Format("2006-01-02T15:04:05.999+08:00")

	//reqid
	if ctx != nil {
		reqid, _ = ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
	}

	//RequestURI
	if fileName != "" { //系统定义的bucket上传才会filename不为空
		RequestURI = fileName
	}

	//status
	status = fmt.Sprint(respstatus)


	//request_length_str response_length_str
	if Method == "POST" || Method == "PUT" {
		request_length_str = contentLength
	} else {
		response_length_str = contentLength
	}
	//spendtime
	spendtime = fmt.Sprint((float64(endTime)-float64(startTime))/1000000, "ms")
	//splittimes
	if ctx != nil {
		splittimes, _ = ((*ctx).Value(public.SDOSS_SPLIT_TIME)).(string)
	}

	//errmess
	if ctx != nil {
		errmess, _ = ((*ctx).Value(public.SDOSS_ERROR_MESSAGE)).(string)
	}
	//sessid
	if ctx != nil {
		sessid, _ = ((*ctx).Value(public.SDOSS_SESSION_ID)).(string)
	}

	//Proto
	Proto = "-"

	//Referer
	Referer = "-"

	//UserAgent
	UserAgent = "BackupServer"

	//Forwarded
	Forwarded = "-"

	if Method == "POST" || Method == "PUT" {
		//lifecycle = r.Header.Get("life-cycle")
		if ctx != nil {
			lifecycle, _ = ((*ctx).Value("life-cycle")).(string)
		}
		if lifecycle == "" {
			lifecycle = "-"
		}
	} else {
		//实际是range
		lifecycle, _ = ((*ctx).Value("Range")).(string)
	}

	logging.LogProtln(backupHttpLog,
		logtime,
		"BACKUP",
		reqid,
		host,
		remoteaddr,
		RequestURI,
		status,
		Method,
		request_length_str,
		response_length_str,
		spendtime,
		splittimes,
		sessid,
		Opcode, Proto,
		Referer,
		UserAgent,
		Forwarded,
		Id,
		lifecycle,
		errmess,
	)
}

//func SepRecLog(fileName ,optype string, filesize int64,retcode int,timecost int64, ctx *context.Context){
func SeparationHttpLog(r *http.Request, respstatus int, contentLength int64, startTime, endTime int64, hostip string, ctx *context.Context) {
	if !HttpLogEnable {
		return
	}
	//
	var logtime, reqid, host, remoteaddr, RequestURI, status, Method, request_length_str, response_length_str, spendtime, splittimes, sessid, Username, Proto, Referer, UserAgent, Forwarded,errmess string
		//logtime
	logtime = time.Now().Local().Format("2006-01-02T15:04:05.999+08:00")

	//reqid
	if ctx != nil {
		errmess, _ = ((*ctx).Value(public.SDOSS_ERROR_MESSAGE)).(string)
		reqid, _ = ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
	}
	//host
	host = hostip

	//remoteaddr
	if parts := strings.Split(r.RemoteAddr, ":"); len(parts) == 2 {
		remoteaddr = parts[0]
	} else {
		remoteaddr = r.RemoteAddr
	}
	//RequestURI
	RequestURI = r.RequestURI

	//status
	status = fmt.Sprint(respstatus)

	//Method
	Method = r.Method

	//request_length_str, response_length_str
	if r.Method == "POST" || r.Method == "PUT" {
		request_length_str = fmt.Sprint(contentLength)
	} else {
		response_length_str = fmt.Sprint(contentLength)
	}

	//spendtime
	spendtime = fmt.Sprint((float64(endTime)-float64(startTime))/1000000, "ms")

	//splittimes
	if ctx != nil {
		splittimes, _ = ((*ctx).Value(public.SDOSS_SPLIT_TIME)).(string)
	}
	//sessid

	//Username
	if r.URL.User != nil && r.URL.User.Username() != "" {
		Username = r.URL.User.Username()
	}

	//Proto
	Proto = r.Proto

	//Referer
	Referer = r.Referer()

	//UserAgent
	UserAgent = r.UserAgent()

	//Forwarded
	Forwarded = r.Header.Get("X-Forwarded-For")

	logging.LogProtln(sepRecLog,
		logtime,
		"Separation",
		reqid,
		host,
		remoteaddr,
		RequestURI,
		status,
		Method,
		request_length_str,
		response_length_str,
		spendtime,
		splittimes,
		sessid,
		Username, Proto,
		Referer,
		UserAgent,
		Forwarded,
		r.URL.Path, //realpath
		r.Header.Get("Range"), //Range
		errmess, //errmess
	)
}
func SeparationHttpLog2(requestUri,method,filename string, respstatus int, contentLength int64, startTime, endTime int64, hostip string, ctx *context.Context) {
	if !HttpLogEnable {
		return
	}
	//
	var logtime, reqid, host, remoteaddr, RequestURI, status, Method, request_length_str, response_length_str, spendtime, splittimes, sessid, Username, Proto, Referer, UserAgent, Forwarded,errmess string
	//logtime
	logtime = time.Now().Local().Format("2006-01-02T15:04:05.999+08:00")

	//reqid
	if ctx != nil {
		errmess, _ = ((*ctx).Value(public.SDOSS_ERROR_MESSAGE)).(string)
		reqid, _ = ((*ctx).Value(public.SDOSS_REQUEST_ID)).(string)
	}
	//host
	host = hostip

	//RequestURI
	RequestURI = requestUri

	//status
	status = fmt.Sprint(respstatus)

	//Method
	Method = method

	//request_length_str, response_length_str
	response_length_str = fmt.Sprint(contentLength)

	//spendtime
	spendtime = fmt.Sprint((float64(endTime)-float64(startTime))/1000000, "ms")

	//splittimes
	if ctx != nil {
		splittimes, _ = ((*ctx).Value(public.SDOSS_SPLIT_TIME)).(string)
	}
	//sessid

	//Username

	//Proto

	//Referer

	//UserAgent

	//Forwarded

	//logging.LogProtln(volumeHttpLog, logtime, reqid, remoteaddr, host, Username, Method, RequestURI, Proto, status, request_length_str, response_length_str, spendtime, splittimes, Referer, UserAgent, Forwarded)
	logging.LogProtln(sepRecLog,
		logtime,
		"Separation",
		reqid,
		host,
		remoteaddr,
		filename,
		status,
		Method,
		request_length_str,
		response_length_str,
		spendtime,
		splittimes,
		sessid,
		Username, Proto,
		Referer,
		UserAgent,
		Forwarded,
		RequestURI,//realpath
		"", //Range
		errmess, //errmess
	)
}
