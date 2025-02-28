// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import (
	"runtime/internal/atomic"
	"runtime/internal/sys"
	"unsafe"
)

// Should be a built-in for unsafe.Pointer?
//go:nosplit
func add(p unsafe.Pointer, x uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(p) + x)
}

// getg returns the pointer to the current g.
// The compiler rewrites calls to this function into instructions
// that fetch the g directly (from TLS or from the dedicated register).
func getg() *g

// mcall switches from the g to the g0 stack and invokes fn(g),
// where g is the goroutine that made the call.
// mcall saves g's current PC/SP in g->sched so that it can be restored later.
// It is up to fn to arrange for that later execution, typically by recording
// g in a data structure, causing something to call ready(g) later.
// mcall returns to the original goroutine g later, when g has been rescheduled.
// fn must not return at all; typically it ends by calling schedule, to let the m
// run other goroutines.
//
// mcall can only be called from g stacks (not g0, not gsignal).
//
// This must NOT be go:noescape: if fn is a stack-allocated closure,
// fn puts g on a run queue, and g executes before fn returns, the
// closure will be invalidated while it is still executing.
func mcall(fn func(*g))

// systemstack runs fn on a system stack.
//
// It is common to use a func literal as the argument, in order
// to share inputs and outputs with the code around the call
// to system stack:
//
//	... set up y ...
//	systemstack(func() {
//		x = bigcall(y)
//	})
//	... use x ...
//
// For the gc toolchain this permits running a function that requires
// additional stack space in a context where the stack can not be
// split.  For gccgo, however, stack splitting is not managed by the
// Go runtime. In effect, all stacks are system stacks. So this gccgo
// version just runs the function.
func systemstack(fn func()) {
	fn()
}

func badsystemstack() {
	throw("systemstack called from unexpected goroutine")
}

// memclr clears n bytes starting at ptr.
// in memclr_*.s
//go:noescape
func memclr(ptr unsafe.Pointer, n uintptr)

//go:linkname reflect_memclr reflect.memclr
func reflect_memclr(ptr unsafe.Pointer, n uintptr) {
	memclr(ptr, n)
}

// memmove copies n bytes from "from" to "to".
// in memmove_*.s
//go:noescape
func memmove(to, from unsafe.Pointer, n uintptr)

//go:linkname reflect_memmove reflect.memmove
func reflect_memmove(to, from unsafe.Pointer, n uintptr) {
	memmove(to, from, n)
}

// exported value for testing
var hashLoad = loadFactor

// in asm_*.s
func fastrand1() uint32

// in asm_*.s
//go:noescape
func memequal(a, b unsafe.Pointer, size uintptr) bool

// noescape hides a pointer from escape analysis.  noescape is
// the identity function but escape analysis doesn't think the
// output depends on the input.  noescape is inlined and currently
// compiles down to a single xor instruction.
// USE CAREFULLY!
//go:nosplit
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}

func mincore(addr unsafe.Pointer, n uintptr, dst *byte) int32

//go:noescape
func jmpdefer(fv *funcval, argp uintptr)
func exit1(code int32)
func asminit()
func setg(gg *g)
func breakpoint()

// reflectcall calls fn with a copy of the n argument bytes pointed at by arg.
// After fn returns, reflectcall copies n-retoffset result bytes
// back into arg+retoffset before returning. If copying result bytes back,
// the caller should pass the argument frame type as argtype, so that
// call can execute appropriate write barriers during the copy.
// Package reflect passes a frame type. In package runtime, there is only
// one call that copies results back, in cgocallbackg1, and it does NOT pass a
// frame type, meaning there are no write barriers invoked. See that call
// site for justification.
func reflectcall(argtype *_type, fn, arg unsafe.Pointer, argsize uint32, retoffset uint32)

func procyield(cycles uint32)

type neverCallThisFunction struct{}

// goexit is the return stub at the top of every goroutine call stack.
// Each goroutine stack is constructed as if goexit called the
// goroutine's entry point function, so that when the entry point
// function returns, it will return to goexit, which will call goexit1
// to perform the actual exit.
//
// This function must never be called directly. Call goexit1 instead.
// gentraceback assumes that goexit terminates the stack. A direct
// call on the stack will cause gentraceback to stop walking the stack
// prematurely and if there are leftover stack barriers it may panic.
func goexit(neverCallThisFunction)

// publicationBarrier performs a store/store barrier (a "publication"
// or "export" barrier). Some form of synchronization is required
// between initializing an object and making that object accessible to
// another processor. Without synchronization, the initialization
// writes and the "publication" write may be reordered, allowing the
// other processor to follow the pointer and observe an uninitialized
// object. In general, higher-level synchronization should be used,
// such as locking or an atomic pointer write. publicationBarrier is
// for when those aren't an option, such as in the implementation of
// the memory manager.
//
// There's no corresponding barrier for the read side because the read
// side naturally has a data dependency order. All architectures that
// Go supports or seems likely to ever support automatically enforce
// data dependency ordering.
func publicationBarrier()

//go:noescape
func setcallerpc(argp unsafe.Pointer, pc uintptr)

// getcallerpc returns the program counter (PC) of its caller's caller.
// getcallersp returns the stack pointer (SP) of its caller's caller.
// For both, the argp must be a pointer to the caller's first function argument.
// The implementation may or may not use argp, depending on
// the architecture.
//
// For example:
//
//	func f(arg1, arg2, arg3 int) {
//		pc := getcallerpc(unsafe.Pointer(&arg1))
//		sp := getcallersp(unsafe.Pointer(&arg1))
//	}
//
// These two lines find the PC and SP immediately following
// the call to f (where f will return).
//
// The call to getcallerpc and getcallersp must be done in the
// frame being asked about. It would not be correct for f to pass &arg1
// to another function g and let g call getcallerpc/getcallersp.
// The call inside g might return information about g's caller or
// information about f's caller or complete garbage.
//
// The result of getcallersp is correct at the time of the return,
// but it may be invalidated by any subsequent call to a function
// that might relocate the stack in order to grow or shrink it.
// A general rule is that the result of getcallersp should be used
// immediately and can only be passed to nosplit functions.

//go:noescape
func getcallerpc(argp unsafe.Pointer) uintptr

//go:noescape
func getcallersp(argp unsafe.Pointer) uintptr

// argp used in Defer structs when there is no argp.
const _NoArgs = ^uintptr(0)

// //go:linkname time_now time.now
// func time_now() (sec int64, nsec int32)

/*
func unixnanotime() int64 {
	sec, nsec := time_now()
	return sec*1e9 + int64(nsec)
}
*/

// round n up to a multiple of a.  a must be a power of 2.
func round(n, a uintptr) uintptr {
	return (n + a - 1) &^ (a - 1)
}

// checkASM returns whether assembly runtime checks have passed.
func checkASM() bool {
	return true
}

// For gccgo this is in the C code.
func osyield()

// For gccgo this can be called directly.
//extern syscall
func syscall(trap uintptr, a1, a2, a3, a4, a5, a6 uintptr) uintptr

// throw crashes the program.
// For gccgo unless and until we port panic.go.
func throw(string)

// newobject allocates a new object.
// For gccgo unless and until we port malloc.go.
func newobject(*_type) unsafe.Pointer

// newarray allocates a new array of objects.
// For gccgo unless and until we port malloc.go.
func newarray(*_type, int) unsafe.Pointer

// funcPC returns the entry PC of the function f.
// It assumes that f is a func value. Otherwise the behavior is undefined.
// For gccgo here unless and until we port proc.go.
//go:nosplit
func funcPC(f interface{}) uintptr {
	return **(**uintptr)(add(unsafe.Pointer(&f), sys.PtrSize))
}

// typedmemmove copies a typed value.
// For gccgo for now.
//go:nosplit
func typedmemmove(typ *_type, dst, src unsafe.Pointer) {
	memmove(dst, src, typ.size)
}

// Here for gccgo unless and until we port string.go.
type stringStruct struct {
	str unsafe.Pointer
	len int
}

// Here for gccgo unless and until we port string.go.
func stringStructOf(sp *string) *stringStruct {
	return (*stringStruct)(unsafe.Pointer(sp))
}

// Here for gccgo unless and until we port slice.go.
type slice struct {
	array unsafe.Pointer
	len   int
	cap   int
}

// Here for gccgo until we port malloc.go.
const (
	_64bit              = 1 << (^uintptr(0) >> 63) / 2
	_MHeapMap_TotalBits = (_64bit*sys.GoosWindows)*35 + (_64bit*(1-sys.GoosWindows)*(1-sys.GoosDarwin*sys.GoarchArm64))*39 + sys.GoosDarwin*sys.GoarchArm64*31 + (1-_64bit)*32
	_MaxMem             = uintptr(1<<_MHeapMap_TotalBits - 1)
)

// Here for gccgo until we port malloc.go.
//extern runtime_mallocgc
func c_mallocgc(size uintptr, typ uintptr, flag uint32) unsafe.Pointer
func mallocgc(size uintptr, typ *_type, needzero bool) unsafe.Pointer {
	flag := uint32(0)
	if !needzero {
		flag = 1 << 3
	}
	return c_mallocgc(size, uintptr(unsafe.Pointer(typ)), flag)
}

// Here for gccgo unless and until we port string.go.
func rawstring(size int) (p unsafe.Pointer, s string) {
	p = mallocgc(uintptr(size), nil, false)

	(*(*stringStruct)(unsafe.Pointer(&s))).str = p
	(*(*stringStruct)(unsafe.Pointer(&s))).len = size

	return
}

// Here for gccgo unless and until we port string.go.
func gostring(p *byte) string {
	l := findnull(p)
	if l == 0 {
		return ""
	}
	m, s := rawstring(l)
	memmove(m, unsafe.Pointer(p), uintptr(l))
	return s
}

// Here for gccgo unless and until we port string.go.
func index(s, t string) int {
	if len(t) == 0 {
		return 0
	}
	for i := 0; i < len(s); i++ {
		if s[i] == t[0] && hasprefix(s[i:], t) {
			return i
		}
	}
	return -1
}

// Here for gccgo unless and until we port string.go.
func hasprefix(s, t string) bool {
	return len(s) >= len(t) && s[:len(t)] == t
}

// Here for gccgo unless and until we port string.go.
//go:nosplit
func findnull(s *byte) int {
	if s == nil {
		return 0
	}
	p := (*[_MaxMem/2 - 1]byte)(unsafe.Pointer(s))
	l := 0
	for p[l] != 0 {
		l++
	}
	return l
}

// Here for gccgo unless and until we port string.go.
//go:nosplit
func gostringnocopy(str *byte) string {
	ss := stringStruct{str: unsafe.Pointer(str), len: findnull(str)}
	return *(*string)(unsafe.Pointer(&ss))
}

// Here for gccgo unless and until we port string.go.
func atoi(s string) int {
	n := 0
	for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
		n = n*10 + int(s[0]) - '0'
		s = s[1:]
	}
	return n
}

// Here for gccgo until we port mgc.go.
var writeBarrier struct {
	enabled bool   // compiler emits a check of this before calling write barrier
	needed  bool   // whether we need a write barrier for current GC phase
	cgo     bool   // whether we need a write barrier for a cgo check
	alignme uint64 // guarantee alignment so that compiler can use a 32 or 64-bit load
}

// Here for gccgo until we port atomic_pointer.go and mgc.go.
//go:nosplit
func casp(ptr *unsafe.Pointer, old, new unsafe.Pointer) bool {
	if !atomic.Casp1((*unsafe.Pointer)(noescape(unsafe.Pointer(ptr))), noescape(old), new) {
		return false
	}
	return true
}

// Here for gccgo until we port lock_*.go.
func lock(l *mutex)
func unlock(l *mutex)

// Here for gccgo for Solaris.
func errno() int

// Temporary for gccgo until we port proc.go.
func entersyscall(int32)
func entersyscallblock(int32)
func exitsyscall(int32)
