// Package rhizodat is the Go binding of the rhizodat core, through its C ABI
// (crates/rhizodat-ffi). It links librhizodat_ffi statically: build the
// library first with
//
//	cargo build --release -p rhizodat-ffi
//
// from the repository root, or point CGO_LDFLAGS at another build of it.
package rhizodat

/*
#cgo CFLAGS: -I${SRCDIR}/../crates/rhizodat-ffi/include
#cgo LDFLAGS: ${SRCDIR}/../target/release/librhizodat_ffi.a
#cgo linux LDFLAGS: -lm -ldl -lpthread
#cgo darwin LDFLAGS: -framework CoreFoundation -framework Security
#include <stdlib.h>
#include "rhizodat.h"
*/
import "C"

import (
	"errors"
	"fmt"
)

// Status is what a call into the core did.
type Status int

// The statuses of the C ABI (RhizodatStatus).
const (
	StatusOK              Status = C.RHIZODAT_STATUS_OK
	StatusInvalidArgument Status = C.RHIZODAT_STATUS_INVALID_ARGUMENT
	StatusFailed          Status = C.RHIZODAT_STATUS_FAILED
	StatusPanic           Status = C.RHIZODAT_STATUS_PANIC
)

// Unexported aliases, so tests (which cannot use cgo) build errors from statuses.
const (
	statusOK              = StatusOK
	statusInvalidArgument = StatusInvalidArgument
)

func (s Status) String() string {
	switch s {
	case StatusOK:
		return "ok"
	case StatusInvalidArgument:
		return "invalid argument"
	case StatusFailed:
		return "failed"
	case StatusPanic:
		return "panic"
	}
	return fmt.Sprintf("status %d", int(s))
}

// Sentinels for errors.Is: an *Error matches the one of its status.
var (
	ErrInvalidArgument = errors.New("rhizodat: invalid argument")
	ErrFailed          = errors.New("rhizodat: failed")
	ErrPanic           = errors.New("rhizodat: panic in the core")
)

// Error is a failed call: its status and the core's message.
type Error struct {
	Status  Status
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("rhizodat: %s: %s", e.Status, e.Message) }

// Is matches the sentinel of the error's status.
func (e *Error) Is(target error) bool {
	switch target {
	case ErrInvalidArgument:
		return e.Status == StatusInvalidArgument
	case ErrFailed:
		return e.Status == StatusFailed
	case ErrPanic:
		return e.Status == StatusPanic
	}
	return false
}

func statusError(status Status, message string) error {
	if status == StatusOK {
		return nil
	}
	return &Error{Status: status, Message: message}
}

// takeString copies a string the library returned and frees it.
func takeString(raw *C.char) string {
	if raw == nil {
		return ""
	}
	defer C.rhizodat_string_free(raw)
	return C.GoString(raw)
}

// call runs one C ABI function that reports an error through its last argument.
func call(fn func(errOut **C.char) C.enum_RhizodatStatus) error {
	var message *C.char
	status := Status(fn(&message))
	return statusError(status, takeString(message))
}

// Version is the version of the linked core library.
func Version() string { return C.GoString(C.rhizodat_version()) }

// NewID is a new UUIDv7, as its text. Ids from all goroutines are unique; those
// from one goroutine are in order.
func NewID() (string, error) {
	var out *C.char
	if err := call(func(e **C.char) C.enum_RhizodatStatus { return C.rhizodat_new_id(&out, e) }); err != nil {
		return "", err
	}
	return takeString(out), nil
}
