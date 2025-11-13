use std::ffi::{c_char, c_void, CString};
use std::ptr;
use std::slice;
use std::collections::HashMap;

use yrs::{Doc, ReadTxn, StateVector, Transact, Update, Any, WriteTxn};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;

// Import the Array trait so methods like len(), push_back(), remove_range() work
use yrs::types::array::Array;

// ============================================================================
// Transaction Wrappers
// ============================================================================

#[repr(C)]
pub struct ReadTransaction {
    doc: *const Doc,
}

#[repr(C)]
pub struct WriteTransaction {
    doc: *mut Doc,
    _origin: Option<Vec<u8>>,
}

// ============================================================================
// Document Management
// ============================================================================

#[no_mangle]
pub extern "C" fn ydoc_new() -> *mut Doc {
    Box::into_raw(Box::new(Doc::new()))
}

#[no_mangle]
pub extern "C" fn ydoc_destroy(doc: *mut Doc) {
    if !doc.is_null() {
        unsafe { drop(Box::from_raw(doc)); }
    }
}

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

#[no_mangle]
pub extern "C" fn ydoc_read_transaction(doc: *const Doc) -> *mut ReadTransaction {
    if doc.is_null() {
        return ptr::null_mut();
    }
    Box::into_raw(Box::new(ReadTransaction { doc }))
}

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
            Some(slice::from_raw_parts(origin, origin_len as usize).to_vec())
        } else {
            None
        };

        Box::into_raw(Box::new(WriteTransaction { doc, _origin: origin_vec }))
    }
}

#[no_mangle]
pub extern "C" fn ytransaction_commit(txn: *mut WriteTransaction) {
    if !txn.is_null() {
        unsafe { drop(Box::from_raw(txn)); }
    }
}

#[no_mangle]
pub extern "C" fn ytransaction_commit_read(txn: *mut ReadTransaction) {
    if !txn.is_null() {
        unsafe { drop(Box::from_raw(txn)); }
    }
}

// ============================================================================
// State Vector Operations
// ============================================================================

#[no_mangle]
pub extern "C" fn ytransaction_state_vector_v1(
    txn: *const ReadTransaction,
    len: *mut u32,
) -> *mut u8 {
    if txn.is_null() || len.is_null() {
        return ptr::null_mut();
    }

    unsafe {
        let doc = &*((*txn).doc);
        let read_txn = doc.transact();
        let sv = read_txn.state_vector();
        let encoded = sv.encode_v1();

        *len = encoded.len() as u32;

        let buffer = libc::malloc(encoded.len()) as *mut u8;
        ptr::copy_nonoverlapping(encoded.as_ptr(), buffer, encoded.len());
        buffer
    }
}

#[no_mangle]
pub extern "C" fn yrs_decode_state_vector(
    data: *const u8,
    len: usize,
) -> *mut StateVector {
    if data.is_null() {
        return ptr::null_mut();
    }

    unsafe {
        match StateVector::decode_v1(slice::from_raw_parts(data, len)) {
            Ok(sv) => Box::into_raw(Box::new(sv)),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn yrs_state_vector_free(sv: *mut StateVector) {
    if !sv.is_null() {
        unsafe { drop(Box::from_raw(sv)); }
    }
}

// ============================================================================
// Update Operations
// ============================================================================

#[no_mangle]
pub extern "C" fn ytransaction_state_diff_v1(
    txn: *const ReadTransaction,
    sv_ptr: *const u8,
    sv_len: u32,
    len: *mut u32,
) -> *mut u8 {
    if txn.is_null() || len.is_null() {
        return ptr::null_mut();
    }

    unsafe {
        let doc = &*((*txn).doc);
        let read_txn = doc.transact();

        let encoded = if sv_ptr.is_null() || sv_len == 0 {
            read_txn.encode_state_as_update_v1(&StateVector::default())
        } else {
            let bytes = slice::from_raw_parts(sv_ptr, sv_len as usize);
            if let Ok(sv) = StateVector::decode_v1(bytes) {
                read_txn.encode_diff_v1(&sv)
            } else {
                read_txn.encode_state_as_update_v1(&StateVector::default())
            }
        };

        *len = encoded.len() as u32;

        let buffer = libc::malloc(encoded.len()) as *mut u8;
        ptr::copy_nonoverlapping(encoded.as_ptr(), buffer, encoded.len());
        buffer
    }
}

#[no_mangle]
pub extern "C" fn ytransaction_apply(
    txn: *mut WriteTransaction,
    diff: *const u8,
    diff_len: u32,
) -> u8 {
    if txn.is_null() || diff.is_null() {
        return 6;
    }

    unsafe {
        let doc = &mut *((*txn).doc);
        let mut wtxn = doc.transact_mut();

        let bytes = slice::from_raw_parts(diff, diff_len as usize);

        match Update::decode_v1(bytes) {
            Ok(update) => match wtxn.apply_update(update) {
                Ok(_) => 0,
                Err(_) => 6,
            },
            Err(_) => 1,
        }
    }
}

// ============================================================================
// Memory Management
// ============================================================================

#[no_mangle]
pub extern "C" fn ybinary_destroy(ptr: *mut u8, _len: u32) {
    if !ptr.is_null() {
        unsafe { libc::free(ptr as *mut c_void); }
    }
}

#[no_mangle]
pub extern "C" fn ystring_destroy(str: *mut c_char) {
    if !str.is_null() {
        unsafe { drop(CString::from_raw(str)); }
    }
}

// ============================================================================
// JSON → Any (yrs 0.21.x)
// ============================================================================

fn json_to_any(val: &serde_json::Value) -> Any {
    match val {
        serde_json::Value::Null => Any::Null,
        serde_json::Value::Bool(b) => Any::Bool(*b),
        serde_json::Value::Number(n) => Any::Number(n.as_f64().unwrap_or(0.0)),
        serde_json::Value::String(s) => Any::String(s.clone().into()),

        serde_json::Value::Array(arr) => {
            let vec: Vec<Any> = arr.iter().map(json_to_any).collect();
            Any::Array(vec.into())
        }

        serde_json::Value::Object(map) => {
            let hashmap: HashMap<String, Any> =
                map.iter().map(|(k, v)| (k.clone(), json_to_any(v))).collect();
            Any::Map(hashmap.into())
        }
    }
}

// ============================================================================
// JSON → Y.Array Update Encoder (yrs 0.21.x compliant)
// ============================================================================

#[no_mangle]
pub extern "C" fn ydoc_encode_nodes_update(
    nodes_json_ptr: *const u8,
    nodes_json_len: usize,
    out_ptr: *mut *const u8,
    out_len: *mut usize,
) -> i32 {
    // Convert bytes to string
    let nodes_json = unsafe { std::slice::from_raw_parts(nodes_json_ptr, nodes_json_len) };
    let nodes_str = match std::str::from_utf8(nodes_json) {
        Ok(s) => s,
        Err(_) => return 1,
    };

    // Parse JSON
    let nodes: serde_json::Value = match serde_json::from_str(nodes_str) {
        Ok(val) => val,
        Err(_) => return 2,
    };

    // Create Y.Doc
    let doc = Doc::new();
    let mut txn = doc.transact_mut();
    let yarray = txn.get_or_insert_array("nodes");

    // Clear existing content
    let len = yarray.len(&txn);
    if len > 0 {
        yarray.remove_range(&mut txn, 0, len);
    }

    // Insert JSON array items as Any values
    if let Some(arr) = nodes.as_array() {
        for node in arr {
            let any_value = json_to_any(node);
            yarray.push_back(&mut txn, any_value);
        }
    }

    // Encode update
    let update = txn.encode_update_v1();

    let boxed = update.into_boxed_slice();
    let ptr = boxed.as_ptr();
    let len = boxed.len();
    std::mem::forget(boxed);

    unsafe {
        *out_ptr = ptr;
        *out_len = len;
    }

    0
}

// ============================================================================
// Error Codes
// ============================================================================

pub const ERR_CODE_IO: u8 = 1;
pub const ERR_CODE_VAR_INT: u8 = 2;
pub const ERR_CODE_EOS: u8 = 3;
pub const ERR_CODE_UNEXPECTED_VALUE: u8 = 4;
pub const ERR_CODE_INVALID_JSON: u8 = 5;
pub const ERR_CODE_OTHER: u8 = 6;