#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::io::{Read, Write};

use crc32c::crc32c_append;
use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use snap::raw::{Decoder, Encoder};

#[unsafe(no_mangle)]
pub extern "C" fn alloc(len: usize) -> *mut u8 {
  let buffer = Vec::with_capacity(len);
  let (ptr, _, _) = buffer.into_raw_parts();
  ptr
}

#[unsafe(no_mangle)]
pub extern "C" fn dealloc(ptr: *mut u8, len: usize) {
  if ptr.is_null() {
    return;
  }

  unsafe {
    let _ = Vec::from_raw_parts(ptr, len, len);
  }
}

#[unsafe(no_mangle)]
pub extern "C" fn crc32c(ptr: *const u8, len: usize) -> u32 {
  let input = unsafe { core::slice::from_raw_parts(ptr, len) };
  crc32c_append(0, input)
}

#[unsafe(no_mangle)]
pub extern "C" fn lz4_compress(ptr: *const u8, len: usize) -> u64 {
  let input = unsafe { core::slice::from_raw_parts(ptr, len) };
  let mut output = vec![];
  let mut encoder = FrameEncoder::new(&mut output);

  encoder.write_all(input).unwrap();
  encoder.finish().unwrap();

  output.shrink_to_fit();
  let (ptr, len, _) = output.into_raw_parts();
  ((ptr as u64) << 32) + (len as u64)
}

#[unsafe(no_mangle)]
pub extern "C" fn lz4_decompress(ptr: *const u8, len: usize) -> u64 {
  let input = unsafe { core::slice::from_raw_parts(ptr, len) };
  let mut output = vec![];
  let mut decoder = FrameDecoder::new(input);

  decoder.read_to_end(&mut output).unwrap();

  output.shrink_to_fit();
  let (ptr, len, _) = output.into_raw_parts();
  ((ptr as u64) << 32) + (len as u64)
}

#[unsafe(no_mangle)]
pub extern "C" fn snappy_compress(ptr: *const u8, len: usize) -> u64 {
  let input = unsafe { core::slice::from_raw_parts(ptr, len) };
  let mut enc = Encoder::new();

  let mut output = enc.compress_vec(input).unwrap();

  output.shrink_to_fit();
  let (ptr, len, _) = output.into_raw_parts();
  ((ptr as u64) << 32) + (len as u64)
}

#[unsafe(no_mangle)]
pub extern "C" fn snappy_decompress(ptr: *const u8, len: usize) -> u64 {
  let input = unsafe { core::slice::from_raw_parts(ptr, len) };
  let mut enc = Decoder::new();

  let mut output = enc.decompress_vec(input).unwrap();

  output.shrink_to_fit();
  let (ptr, len, _) = output.into_raw_parts();
  ((ptr as u64) << 32) + (len as u64)
}
