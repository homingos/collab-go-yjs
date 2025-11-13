package yrs

/*
#cgo LDFLAGS: -L${SRCDIR}/../../yrs-ffi/target/release -lyrs_ffi
#cgo CFLAGS: -I${SRCDIR}

#include <stdlib.h>
#include <stdint.h>

// Forward declarations matching official Yrs C FFI API
void* ydoc_new();
void ydoc_destroy(void* doc);
uint64_t ydoc_id(void* doc);

void* ydoc_read_transaction(void* doc);
void* ydoc_write_transaction(void* doc, uint32_t origin_len, const uint8_t* origin);
void ytransaction_commit(void* txn);
void ytransaction_commit_read(void* txn);

uint8_t* ytransaction_state_vector_v1(const void* txn, uint32_t* len);
void* yrs_decode_state_vector(uint8_t* data, size_t len);
void yrs_state_vector_free(void* sv);

uint8_t* ytransaction_state_diff_v1(const void* txn, const uint8_t* sv, uint32_t sv_len, uint32_t* len);

uint8_t ytransaction_apply(void* txn, const uint8_t* diff, uint32_t diff_len);

void ybinary_destroy(uint8_t* ptr, uint32_t len);
void ystring_destroy(char* str);

// Encode nodes update function
int ydoc_encode_nodes_update(const uint8_t* nodes_json_ptr, size_t nodes_json_len, const uint8_t** out_ptr, size_t* out_len);
*/
import "C"
import (
	"errors"
	"unsafe"
)

// Doc represents a Yrs document
type Doc struct {
	ptr unsafe.Pointer
}

// Transaction represents a Yrs transaction (read or write)
type Transaction struct {
	ptr     unsafe.Pointer
	isWrite bool
	doc     *Doc
}

// StateVector represents a Yrs state vector
type StateVector struct {
	ptr unsafe.Pointer
}

// NewDoc creates a new Yrs document with a random client ID
func NewDoc() *Doc {
	ptr := C.ydoc_new()
	if ptr == nil {
		return nil
	}
	return &Doc{ptr: ptr}
}

// Destroy frees the Yrs document
func (d *Doc) Destroy() {
	if d.ptr != nil {
		C.ydoc_destroy(d.ptr)
		d.ptr = nil
	}
}

// ClientID returns the client ID of the document
func (d *Doc) ClientID() uint64 {
	if d.ptr == nil {
		return 0
	}
	return uint64(C.ydoc_id(d.ptr))
}

// ReadTransaction creates a read transaction
func (d *Doc) ReadTransaction() *Transaction {
	if d.ptr == nil {
		return nil
	}
	ptr := C.ydoc_read_transaction(d.ptr)
	if ptr == nil {
		return nil
	}
	return &Transaction{
		ptr:     ptr,
		isWrite: false,
		doc:     d,
	}
}

// WriteTransaction creates a write transaction
func (d *Doc) WriteTransaction(origin []byte) *Transaction {
	if d.ptr == nil {
		return nil
	}

	var originPtr *C.uint8_t
	var originLen C.uint32_t
	if len(origin) > 0 {
		originPtr = (*C.uint8_t)(unsafe.Pointer(&origin[0]))
		originLen = C.uint32_t(len(origin))
	}

	ptr := C.ydoc_write_transaction(d.ptr, originLen, originPtr)
	if ptr == nil {
		return nil
	}
	return &Transaction{
		ptr:     ptr,
		isWrite: true,
		doc:     d,
	}
}

// Commit commits the transaction
func (t *Transaction) Commit() {
	if t.ptr == nil {
		return
	}
	if t.isWrite {
		C.ytransaction_commit(t.ptr)
	} else {
		C.ytransaction_commit_read(t.ptr)
	}
	t.ptr = nil
}

// StateVector returns the state vector of the document
func (t *Transaction) StateVector() []byte {
	if t.ptr == nil {
		return nil
	}

	var len C.uint32_t
	data := C.ytransaction_state_vector_v1(t.ptr, &len)
	if data == nil {
		return nil
	}
	defer C.ybinary_destroy(data, len)

	result := C.GoBytes(unsafe.Pointer(data), C.int(len))
	return result
}

// StateDiff returns the diff update based on a state vector
func (t *Transaction) StateDiff(stateVector []byte) []byte {
	if t.ptr == nil {
		return nil
	}

	var svPtr *C.uint8_t
	var svLen C.uint32_t
	if len(stateVector) > 0 {
		svPtr = (*C.uint8_t)(unsafe.Pointer(&stateVector[0]))
		svLen = C.uint32_t(len(stateVector))
	}

	var len C.uint32_t
	data := C.ytransaction_state_diff_v1(t.ptr, svPtr, svLen, &len)
	if data == nil {
		return nil
	}
	defer C.ybinary_destroy(data, len)

	return C.GoBytes(unsafe.Pointer(data), C.int(len))
}

// Apply applies an update to the document
func (t *Transaction) Apply(update []byte) error {
	if t.ptr == nil {
		return errors.New("transaction is nil")
	}
	if !t.isWrite {
		return errors.New("cannot apply update to read-only transaction")
	}
	if len(update) == 0 {
		return nil
	}

	result := C.ytransaction_apply(t.ptr, (*C.uint8_t)(unsafe.Pointer(&update[0])), C.uint32_t(len(update)))
	if result != 0 {
		return &YrsError{Code: int(result)}
	}

	return nil
}

// EncodeNodesArrayUpdate creates a Y.Array update from a JSON array
func EncodeNodesArrayUpdate(nodesJSON []byte) ([]byte, error) {
	if len(nodesJSON) == 0 {
		return nil, errors.New("empty JSON input")
	}

	var outPtr *C.uint8_t
	var outLen C.size_t

	result := C.ydoc_encode_nodes_update(
		(*C.uint8_t)(unsafe.Pointer(&nodesJSON[0])),
		C.size_t(len(nodesJSON)),
		(**C.uint8_t)(unsafe.Pointer(&outPtr)),
		&outLen,
	)

	if result != 0 {
		switch result {
		case 1:
			return nil, errors.New("invalid UTF-8 in JSON input")
		case 2:
			return nil, errors.New("invalid JSON format")
		default:
			return nil, errors.New("FFI error encoding nodes update")
		}
	}

	// Copy the update bytes from C memory
	update := C.GoBytes(unsafe.Pointer(outPtr), C.int(outLen))

	return update, nil
}

// YrsError represents a Yrs error
type YrsError struct {
	Code int
}

func (e *YrsError) Error() string {
	switch e.Code {
	case 1:
		return "ERR_CODE_IO: couldn't read data from input stream"
	case 2:
		return "ERR_CODE_VAR_INT: decoded variable integer outside bounds"
	case 3:
		return "ERR_CODE_EOS: end of stream"
	case 4:
		return "ERR_CODE_UNEXPECTED_VALUE: unexpected enum tag"
	case 5:
		return "ERR_CODE_INVALID_JSON: invalid JSON"
	default:
		return "ERR_CODE_OTHER: unknown error"
	}
}
