package args

import (
	"flag"
	"reflect"
	"time"
)

// A represents an argument
type A func(i interface{}) V

// Get returns the value of the argument from the values.
// nil is returned if no value and no default is set for this argument.
func (a A) Get(vals ...interface{}) (val interface{}) {
	val = get(a(nil).arg(), vals)
	return
}

// IsSet returns whether the argument is set in the values.
func (a A) IsSet(vals ...interface{}) bool {
	return isSet(a(nil).arg(), vals)
}

// New creates a new argument.
func New(vals ...V) A {
	a := &arg{}

	d := Default.Get(vals)
	a.defv = d

	f := A(flagA).Get(vals)
	if f != nil {
		panic("cannot set flag for interface{}")
	}

	return a.getA()
}

// BoolA is a boolean argument.
type BoolA func(b bool) V

// Get returns the value of the int argument among vals.
func (a BoolA) Get(vals ...interface{}) (val bool) {
	v := get(a(false).arg(), vals)
	switch v := v.(type) {
	case *bool:
		return *v
	case bool:
		return v
	}
	return
}

// IsSet returns whether the argument is set in the values.
func (a BoolA) IsSet(vals ...interface{}) bool {
	return isSet(a(false).arg(), vals)
}

// NewBool creates a new integer argument.
func NewBool(vals ...V) BoolA {
	a := &arg{}

	d := Default.Get(vals)
	a.defv = d

	f := A(flagA).Get(vals)
	if f != nil {
		f := f.(flagT)
		a.defv = flag.Bool(f.name, f.defv.(bool), f.usage)
	}

	ab := a.getA()
	return func(b bool) V {
		return ab(b)
	}
}

// IntA is an integer argument.
type IntA func(i int) V

// Get returns the value of the int argument among vals.
func (a IntA) Get(vals ...interface{}) (val int) {
	v := get(a(0).arg(), vals)
	switch v := v.(type) {
	case *int:
		return *v
	case int:
		return v
	}
	return
}

// IsSet returns whether the argument is set in the values.
func (a IntA) IsSet(vals ...interface{}) bool {
	return isSet(a(0).arg(), vals)
}

// NewInt creates a new integer argument.
func NewInt(vals ...V) IntA {
	a := &arg{}

	d := Default.Get(vals)
	a.defv = d

	f := A(flagA).Get(vals)
	if f != nil {
		f := f.(flagT)
		a.defv = flag.Int(f.name, f.defv.(int), f.usage)
	}

	ai := a.getA()
	return func(i int) V {
		return ai(i)
	}
}

// UintA is an unsigned integer argument.
type UintA func(i uint) V

// Get returns the value of the int argument among vals.
func (a UintA) Get(vals ...interface{}) (val uint) {
	v := get(a(0).arg(), vals)
	switch v := v.(type) {
	case *uint:
		return *v
	case uint:
		return v
	}
	return
}

// IsSet returns whether the argument is set in the values.
func (a UintA) IsSet(vals ...interface{}) bool {
	return isSet(a(0).arg(), vals)
}

// NewUint creates a new integer argument.
func NewUint(vals ...V) UintA {
	a := &arg{}

	d := Default.Get(vals)
	a.defv = d

	f := A(flagA).Get(vals)
	if f != nil {
		f := f.(flagT)
		a.defv = flag.Uint(f.name, f.defv.(uint), f.usage)
	}

	ai := a.getA()
	return func(i uint) V {
		return ai(i)
	}
}

// Int64A is a 64-bit integer argument.
type Int64A func(i int64) V

// Get returns the value of the int64 argument among vals.
func (a Int64A) Get(vals ...interface{}) (val int64) {
	v := get(a(0).arg(), vals)
	switch v := v.(type) {
	case *int64:
		return *v
	case int64:
		return v
	}
	return
}

// IsSet returns whether the argument is set in the values.
func (a Int64A) IsSet(vals ...interface{}) bool {
	return isSet(a(0).arg(), vals)
}

// NewInt64 creates a new integer argument.
func NewInt64(vals ...V) Int64A {
	a := &arg{}

	d := Default.Get(vals)
	a.defv = d

	f := A(flagA).Get(vals)
	if f != nil {
		f := f.(flagT)
		a.defv = flag.Int64(f.name, f.defv.(int64), f.usage)
	}

	ai := a.getA()
	return func(i int64) V {
		return ai(i)
	}
}

// Uint64A is an unsigned 64-bit integer argument.
type Uint64A func(i uint64) V

// Get returns the value of the uint64 argument among vals.
func (a Uint64A) Get(vals ...interface{}) (val uint64) {
	v := get(a(0).arg(), vals)
	switch v := v.(type) {
	case *uint64:
		return *v
	case uint64:
		return v
	}
	return
}

// IsSet returns whether the argument is set in the values.
func (a Uint64A) IsSet(vals ...interface{}) bool {
	return isSet(a(0).arg(), vals)
}

// NewUint64 creates a new integer argument.
func NewUint64(vals ...V) Uint64A {
	a := &arg{}

	d := Default.Get(vals)
	a.defv = d

	f := A(flagA).Get(vals)
	if f != nil {
		f := f.(flagT)
		a.defv = flag.Uint64(f.name, f.defv.(uint64), f.usage)
	}

	ai := a.getA()
	return func(i uint64) V {
		return ai(i)
	}
}

// Float64A is a 64-bit float argument.
type Float64A func(i float64) V

// Get returns the value of the float64 argument among vals.
func (a Float64A) Get(vals ...interface{}) (val float64) {
	v := get(a(0).arg(), vals)
	switch v := v.(type) {
	case *float64:
		return *v
	case float64:
		return v
	}
	return
}

// IsSet returns whether the argument is set in the values.
func (a Float64A) IsSet(vals ...interface{}) bool {
	return isSet(a(0).arg(), vals)
}

// NewFloat64 creates a new integer argument.
func NewFloat64(vals ...V) Float64A {
	a := &arg{}

	d := Default.Get(vals)
	a.defv = d

	f := A(flagA).Get(vals)
	if f != nil {
		f := f.(flagT)
		a.defv = flag.Float64(f.name, f.defv.(float64), f.usage)
	}

	af := a.getA()
	return func(f float64) V {
		return af(f)
	}
}

// StringA is a string argument.
type StringA func(s string) V

// Get returns the value of the string argument among vals.
func (a StringA) Get(vals ...interface{}) (val string) {
	v := get(a("").arg(), vals)
	switch v := v.(type) {
	case *string:
		return *v
	case string:
		return v
	}
	return
}

// IsSet returns whether the argument is set in the values.
func (a StringA) IsSet(vals ...interface{}) bool {
	return isSet(a("").arg(), vals)
}

// NewString creates a new integer argument.
func NewString(vals ...V) StringA {
	a := &arg{}

	d := Default.Get(vals)
	a.defv = d

	f := A(flagA).Get(vals)
	if f != nil {
		f := f.(flagT)
		a.defv = flag.String(f.name, f.defv.(string), f.usage)
	}

	as := a.getA()
	return func(s string) V {
		return as(s)
	}
}

// DurationA is a duration argument.
type DurationA func(d time.Duration) V

// Get returns the value of the time.Duration argument among vals.
func (a DurationA) Get(vals ...interface{}) (val time.Duration) {
	v := get(a(0).arg(), vals)
	switch v := v.(type) {
	case *time.Duration:
		return *v
	case time.Duration:
		return v
	}
	return
}

// IsSet returns whether the argument is set in the values.
func (a DurationA) IsSet(vals ...interface{}) bool {
	return isSet(a(0).arg(), vals)
}

// NewDuration creates a new integer argument.
func NewDuration(vals ...V) DurationA {
	a := &arg{}

	d := Default.Get(vals)
	a.defv = d

	f := A(flagA).Get(vals)
	if f != nil {
		f := f.(flagT)
		a.defv = flag.Duration(f.name, f.defv.(time.Duration), f.usage)
	}

	ad := a.getA()
	return func(d time.Duration) V {
		return ad(d)
	}
}

// V represents the value of an argument.
type V interface {
	val() interface{}
	arg() *arg
}

type argVal struct {
	value    interface{}
	argument *arg
}

func (v argVal) val() interface{} {
	return v.value
}

func (v argVal) arg() *arg {
	return v.argument
}

func get(arg *arg, vals []interface{}) interface{} {
	for i := len(vals) - 1; 0 <= i; i-- {
		val := vals[i]
		switch reflect.TypeOf(val).Kind() {
		case reflect.Slice:
			s := reflect.ValueOf(val)
			for j := s.Len() - 1; 0 <= j; j-- {
				if v, ok := s.Index(j).Interface().(V); ok && arg == v.arg() {
					return v.val()
				}
			}

		default:
			if v, ok := arg.valueOf(val); ok {
				return v
			}
		}
	}
	return arg.defv
}

func isSet(arg *arg, vals []interface{}) bool {
	for i := len(vals) - 1; 0 <= i; i-- {
		val := vals[i]
		switch reflect.TypeOf(val).Kind() {
		case reflect.Slice:
			s := reflect.ValueOf(val)
			for j := s.Len() - 1; 0 <= j; j-- {
				if v, ok := s.Index(j).Interface().(V); ok && arg == v.arg() {
					return true
				}
			}

		default:
			_, ok := arg.valueOf(val)
			return ok
		}
	}
	return false
}

type arg struct {
	defv interface{}
}

func (a *arg) valueOf(i interface{}) (val interface{}, ok bool) {
	if v, ok := i.(V); ok && a == v.arg() {
		return v.val(), true
	}
	return
}

func (a *arg) getA() A {
	return func(i interface{}) V {
		return argVal{
			value:    i,
			argument: a,
		}
	}
}

// Default sets the default value of an argument.
var Default = defArg.getA()
var defArg = &arg{}

// Flag sets the default value of an argument based on a flag.
//
// Note that Flag can only be used in typed New functions (e.g., NewInt(), ...).
// New() will panic if it receives a Flag.
//
// Flag overwrites Default if both passed to an argument.
func Flag(name string, defv interface{}, usage string) V {
	return flagA(flagT{
		name:  name,
		defv:  defv,
		usage: usage,
	})
}

var flagA = flagArg.getA()
var flagArg = &arg{}

type flagT struct {
	name  string      // name is the name of the flag.
	defv  interface{} // defv is the default value of this flag.
	usage string      // usage is the usage documentation of this flag.
}
