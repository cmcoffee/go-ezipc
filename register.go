package blab

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"unicode"
	"unicode/utf8"
	"encoding/base64"
	"net"
)

// Wraps function to handle incoming and outgoing IPC msgs.
func wrapFunc(fptr interface{}) (newFunc func(*msg) *msg, err error) {
	fn := reflect.TypeOf(fptr)

	// Sanity checks for registering function.
	if fn.Kind() != reflect.Func {
		return nil, fmt.Errorf("Only functions may be registered, got %s.", fn.Kind().String())
	}

	if fn.NumIn() != 2 {
		return nil,
			errors.New("Method must contain two exported (or builtin) arguments.")
	}

	varCheck := func(input reflect.Type) bool {
		// Not an pointer, but built-in type.
		if input.Kind() != reflect.Ptr {
			return true
		}
		if input.Elem().Kind() != reflect.Struct {
			return true
		}

		var name []rune
		name = []rune(input.Elem().Name())
		if len(name) < 1 {
			return false
		}
		if unicode.IsUpper(name[0]) {
			return true
		}
		return false
	}

	if !varCheck(fn.In(0)) {
		return nil, errors.New("Method must use exported (or builtin) argument.")
	}
	if fn.In(1).Kind() != reflect.Ptr || !varCheck(fn.In(1)) {
		return nil, errors.New("Second argument or Reply must be ptr to exported (or builtin) value.")
	}
	if fn.NumOut() != 1 || fn.Out(0).Name() != "error" {
		return nil, errors.New("Method must return only an error.")
	}

	in := reflect.New(fn.In(0))
	out := reflect.New(fn.In(1).Elem())

	funcPtr := reflect.ValueOf(fptr)

	// Create new function that recieves *MSG and outputs *MSG.
	newFunc = func(req *msg) *msg {
		// Flip destination and source for return message.
		Va1, err := base64.StdEncoding.DecodeString(req.Va1)
		if err != nil {
			req.Err = err.Error()
			return req
		}

		Va2, err := base64.StdEncoding.DecodeString(req.Va2)
		if err != nil {
			req.Err = err.Error()
			return req
		}

		err = json.Unmarshal(Va1, in.Interface())
		req.Va1 = ""
		if err != nil {
			req.Err = err.Error()
			return req
		}

		err = json.Unmarshal(Va2, out.Interface())
		req.Va2 = ""
		if err != nil {
			req.Err = err.Error()
			return req
		}

		errResp := funcPtr.Call([]reflect.Value{in.Elem(), out})[0].Interface()
		if errResp != nil {
			req.Err = errResp.(error).Error()
			return req
		}

		json_out, err := json.Marshal(out.Interface())
		if err != nil {
			req.Err = err.Error()
			return req
		}

		req.Va2 = base64.StdEncoding.EncodeToString(json_out)

		return req
	}
	return newFunc, err
}

// Registers local methods or function, informs Broker of registration.
// Function/method template should follow:
// func name(argType T1, replyType *T2) error
// func (*T) Name(argType T1, replyType *T2) error
func (r *router) Register(fptr interface{}) error { return r.RegisterName("", fptr) }

// RegisterName operates exactly as Register but allows changing the name of the object or function.
func (r *router) RegisterName(name string, fptr interface{}) (err error) {
	// Allows registration of both functions and methods.
	// Register function if provided function, register all methods if provided an object.

	switch reflect.TypeOf(fptr).Kind() {
	case reflect.Func:
		wFunc, err := wrapFunc(fptr)
		if err != nil {
			return err
		}

		if name == "" {
			name = strings.TrimPrefix(runtime.FuncForPC(reflect.ValueOf(fptr).Pointer()).Name(), "main.")
		}

		// Add wrapped method to local method map.
		r.route(&msg{
			Dst: name,
			Tag: 0,
			conn: &connection{
				routes: []string{name},
				router: r,
				exec: wFunc, 
			},
		})

	case reflect.Ptr:
		ft := reflect.TypeOf(fptr)
		fv := reflect.ValueOf(fptr)
		for i := 0; i < ft.NumMethod(); i++ {
			method := fv.Method(i)
			if name == "" {
				name = ft.Elem().Name()
			}
			method_name := fmt.Sprintf("%s.%s", name, ft.Method(i).Name)
			method_ch, _ := utf8.DecodeRune([]byte(ft.Method(i).Name))
			if unicode.ToUpper(method_ch) != method_ch {
				continue
			}
			err_s := r.RegisterName(method_name, method.Interface())
			if err_s != nil {
				return fmt.Errorf("Registration failed for [%s.%s]: %s", name, ft.Method(i).Name, err_s)
			}

		}
	default:
		return fmt.Errorf("Cannot register invalid type: %s", reflect.TypeOf(fptr).Kind())
	}
	return
}

// Generates new *riphub.connection from net.Conn.
func (r *router) addconnection(conn net.Conn) *connection {
	return &connection{
		conn:   conn,
		router:   r,
		routes: make([]string, 0),
	}
}

