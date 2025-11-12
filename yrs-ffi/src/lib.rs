use std::ffi::{c_char, c_void, CString};
use std::ptr;
use std::slice;
use yrs::{Doc, ReadTxn, StateVector, Transact, Update};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;

// ============================================================================
// Transaction Wrappers for FFI
// ============================================================================

/// Wrapper for read transactions to manage lifetimes in FFI
/// We use a raw pointer to the document to ensure it stays alive
struct ReadTransaction {
    doc: *const Doc,
    // We'll create the transaction on-demand rather than storing it
    // This avoids lifetime issues
}

/// Wrapper for write transactions to manage lifetimes in FFI
/// We store the document and create transactions on-demand to avoid lifetime issues
struct WriteTransaction {
    doc: *mut Doc,
    origin: Option<Vec<u8>>,
}

// ============================================================================
// Document Management
// ============================================================================

/// Create a new Yrs document with a randomized unique client identifier.
/// Use [ydoc_destroy] in order to release created [Doc] resources.
#[no_mangle]
pub extern "C" fn ydoc_new() -> *mut Doc {
    Box::into_raw(Box::new(Doc::new()))
}

/// Releases all memory-allocated resources bound to given document.
#[no_mangle]
pub extern "C" fn ydoc_destroy(doc: *mut Doc) {
    if !doc.is_null() {
        unsafe {
            let _ = Box::from_raw(doc);
        }
    }
}

/// Returns a unique client identifier of this [Doc] instance.
#[no_mangle]
pub extern "C" fn ydoc_id(doc: *const Doc) -> u64 {
    if doc.is_null() {
        return 0;
    }
    unsafe { (*doc).client_id() }
}

// ============================================================================
// Transaction Management
// ============================================================================

/// Starts a new read-only transaction on a given document.
/// Returns `NULL` if read-only transaction couldn't be created.
#[no_mangle]
pub extern "C" fn ydoc_read_transaction(doc: *const Doc) -> *mut ReadTransaction {
    if doc.is_null() {
        return ptr::null_mut();
    }

    Box::into_raw(Box::new(ReadTransaction { doc }))
}

/// Starts a new read-write transaction on a given document.
/// `origin_len` and `origin` are optional parameters to specify a byte sequence
/// used to mark the origin of this transaction.
/// Returns `NULL` if read-write transaction couldn't be created.
#[no_mangle]
pub extern "C" fn ydoc_write_transaction(
    doc: *mut Doc,
    origin_len: u32,
    origin: *const u8,
) -> *mut WriteTransaction {
    if doc.is_null() {
        return ptr::null_mut();
    }

    unsafe {
        let origin_vec = if !origin.is_null() && origin_len > 0 {
            let origin_slice = slice::from_raw_parts(origin, origin_len as usize);
            Some(origin_slice.to_vec())
        } else {
            None
        };
        
        Box::into_raw(Box::new(WriteTransaction {
            doc,
            origin: origin_vec,
        }))
    }
}

/// Commit and dispose provided read-write transaction.
#[no_mangle]
pub extern "C" fn ytransaction_commit(txn: *mut WriteTransaction) {
    if txn.is_null() {
        return;
    }

    unsafe {
        let _ = Box::from_raw(txn);
        // Transaction is automatically committed when dropped
    }
}

/// Commit and dispose provided read-only transaction.
#[no_mangle]
pub extern "C" fn ytransaction_commit_read(txn: *mut ReadTransaction) {
    if txn.is_null() {
        return;
    }

    unsafe {
        let _ = Box::from_raw(txn);
    }
}

// ============================================================================
// State Vector Operations
// ============================================================================

/// Returns a state vector of a current transaction's document, serialized using lib0 version 1
/// encoding. Payload created by this function can then be send over the network to a remote peer.
/// The length of a generated binary will be passed within a `len` out parameter.
/// Once no longer needed, a returned binary can be disposed using [ybinary_destroy] function.
#[no_mangle]
pub extern "C" fn ytransaction_state_vector_v1(
    txn: *const ReadTransaction,
    len: *mut u32,
) -> *mut u8 {
    if txn.is_null() || len.is_null() {
        return ptr::null_mut();
    }

    unsafe {
        let txn_wrapper = &*txn;
        if txn_wrapper.doc.is_null() {
            return ptr::null_mut();
        }
        
        let doc_ref = &*txn_wrapper.doc;
        let read_txn = doc_ref.transact();
        let sv = read_txn.state_vector();
        let encoded = sv.encode_v1();

        *len = encoded.len() as u32;
        let buffer = libc::malloc(encoded.len()) as *mut u8;
        if buffer.is_null() {
            return ptr::null_mut();
        }
        ptr::copy_nonoverlapping(encoded.as_ptr(), buffer, encoded.len());
        buffer
    }
}

/// Decode a state vector from bytes (v1)
/// Returns opaque pointer that must be freed with yrs_state_vector_free
#[no_mangle]
pub extern "C" fn yrs_decode_state_vector(
    data: *const u8,
    len: usize,
) -> *mut StateVector {
    if data.is_null() {
        return ptr::null_mut();
    }

    unsafe {
        let bytes = slice::from_raw_parts(data, len);
        match StateVector::decode_v1(bytes) {
            Ok(sv) => Box::into_raw(Box::new(sv)),
            Err(_) => ptr::null_mut(),
        }
    }
}

/// Free a state vector
#[no_mangle]
pub extern "C" fn yrs_state_vector_free(sv: *mut StateVector) {
    if !sv.is_null() {
        unsafe {
            let _ = Box::from_raw(sv);
        }
    }
}

// ============================================================================
// Update Operations
// ============================================================================

/// Returns a delta difference between current state of a transaction's document and a state vector
/// `sv` encoded as a binary payload using lib0 version 1 encoding.
/// If passed `sv` pointer is null, the generated diff will be a snapshot containing entire state.
/// A length of an encoded state vector payload must be passed as `sv_len` parameter.
/// A length of generated delta diff binary will be passed within a `len` out parameter.
/// Once no longer needed, a returned binary can be disposed using [ybinary_destroy] function.
#[no_mangle]
pub extern "C" fn ytransaction_state_diff_v1(
    txn: *const ReadTransaction,
    sv: *const u8,
    sv_len: u32,
    len: *mut u32,
) -> *mut u8 {
    if txn.is_null() || len.is_null() {
        return ptr::null_mut();
    }

    unsafe {
        let txn_wrapper = &*txn;
        if txn_wrapper.doc.is_null() {
            return ptr::null_mut();
        }
        
        let doc_ref = &*txn_wrapper.doc;
        let read_txn = doc_ref.transact();
        
        let encoded = if sv.is_null() || sv_len == 0 {
            // If no state vector, return full state
            read_txn.encode_state_as_update_v1(&StateVector::default())
        } else {
            let sv_bytes = slice::from_raw_parts(sv, sv_len as usize);
            match StateVector::decode_v1(sv_bytes) {
                Ok(state_vector) => read_txn.encode_diff_v1(&state_vector),
                Err(_) => {
                    // Fallback to full state if decode fails
                    read_txn.encode_state_as_update_v1(&StateVector::default())
                }
            }
        };

        *len = encoded.len() as u32;
        let buffer = libc::malloc(encoded.len()) as *mut u8;
        if buffer.is_null() {
            return ptr::null_mut();
        }
        ptr::copy_nonoverlapping(encoded.as_ptr(), buffer, encoded.len());
        buffer
    }
}

/// Applies an diff update (generated by `ytransaction_state_diff_v1`) to a local transaction's
/// document.
/// A length of generated `diff` binary must be passed within a `diff_len` parameter.
/// Returns an error code in case if transaction succeeded failed:
/// - **0**: success
/// - `ERR_CODE_IO` (**1**): couldn't read data from input stream.
/// - `ERR_CODE_VAR_INT` (**2**): decoded variable integer outside of the expected integer size bounds.
/// - `ERR_CODE_EOS` (**3**): end of stream found when more data was expected.
/// - `ERR_CODE_UNEXPECTED_VALUE` (**4**): decoded enum tag value was not among known cases.
/// - `ERR_CODE_INVALID_JSON` (**5**): failure when trying to decode JSON content.
/// - `ERR_CODE_OTHER` (**6**): other error type than the one specified.
#[no_mangle]
pub extern "C" fn ytransaction_apply(
    txn: *mut WriteTransaction,
    diff: *const u8,
    diff_len: u32,
) -> u8 {
    if txn.is_null() || diff.is_null() {
        return 6; // ERR_CODE_OTHER
    }

    unsafe {
        let txn_wrapper = &mut *txn;
        if txn_wrapper.doc.is_null() {
            return 6; // ERR_CODE_OTHER
        }
        
        let doc_ref = &mut *txn_wrapper.doc;
        let mut write_txn = doc_ref.transact_mut();
        
        // Note: Origin is typically set during transaction creation, but since we create
        // transactions on-demand, we store it for potential future use.
        // For now, we proceed without setting origin as it's not critical for basic operations.
        
        let bytes = slice::from_raw_parts(diff, diff_len as usize);

        match Update::decode_v1(bytes) {
            Ok(update) => {
                match write_txn.apply_update(update) {
                    Ok(_) => 0, // Success
                    Err(_) => 6, // ERR_CODE_OTHER
                }
            }
            Err(_) => 1, // ERR_CODE_IO for decode errors
        }
    }
}

// ============================================================================
// Memory Management
// ============================================================================

/// Frees all memory-allocated resources bound to a given binary returned from Yrs document API.
/// Unlike strings binaries are not null-terminated and can contain null characters inside,
/// therefore a size of memory to be released must be explicitly provided.
/// Yrs binaries don't use libc malloc, so calling `free()` on them will fault.
#[no_mangle]
pub extern "C" fn ybinary_destroy(ptr: *mut u8, _len: u32) {
    if !ptr.is_null() {
        unsafe {
            libc::free(ptr as *mut c_void);
        }
    }
}

/// Frees all memory-allocated resources bound to a given UTF-8 null-terminated string returned from
/// Yrs document API. Yrs strings don't use libc malloc, so calling `free()` on them will fault.
#[no_mangle]
pub extern "C" fn ystring_destroy(str: *mut c_char) {
    if !str.is_null() {
        unsafe {
            let _ = CString::from_raw(str);
        }
    }
}

// ============================================================================
// Error Code Constants (matching the header)
// ============================================================================

pub const ERR_CODE_IO: u8 = 1;
pub const ERR_CODE_VAR_INT: u8 = 2;
pub const ERR_CODE_EOS: u8 = 3;
pub const ERR_CODE_UNEXPECTED_VALUE: u8 = 4;
pub const ERR_CODE_INVALID_JSON: u8 = 5;
pub const ERR_CODE_OTHER: u8 = 6;
