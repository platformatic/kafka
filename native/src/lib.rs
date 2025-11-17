#![deny(clippy::all)]

use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::env;
use std::ffi::{c_char, c_int, c_uchar, c_uint, c_void, CStr, CString};
use std::path::Path;

#[allow(warnings)]
mod bindings {
  include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use bindings::*;

#[link(name = "krb5")]
extern "C" {
  fn krb5_get_error_message(context: *mut c_void, code: c_int) -> *const c_char;
  fn krb5_free_error_message(context: *mut c_void, message: *const c_char);

  fn krb5_init_context(context: *mut *mut c_void) -> i32;
  fn krb5_free_context(context: *mut c_void);

  fn krb5_cc_default(context: *mut c_void, ccache: *mut *mut c_void) -> c_int;
  fn krb5_cc_initialize(context: *mut c_void, cache: *mut c_void, principal: *mut c_void) -> c_int;
  fn krb5_cc_close(context: *mut c_void, cache: *mut c_void) -> c_int;
  fn krb5_cc_store_cred(context: *mut c_void, cache: *mut c_void, creds: *mut c_void) -> c_int;

  fn krb5_parse_name(context: *mut c_void, name: *const c_char, principal: *mut *mut c_void) -> c_int;
  fn krb5_free_principal(context: *mut c_void, principal: *mut c_void);

  fn krb5_kt_resolve(context: *mut c_void, name: *const c_char, keytab: *mut *mut c_void) -> c_int;
  fn krb5_kt_close(context: *mut c_void, keytab: *mut c_void) -> c_int;

  fn krb5_get_init_creds_password(
    context: *mut c_void,
    creds: *mut c_void,
    principal: *mut c_void,
    password: *const c_char,
    prompter: *const c_void,
    data: *const c_void,
    start_time: c_uint,
    in_tkt_service: *const c_char,
    options: *const c_void,
  ) -> c_int;

  fn krb5_get_init_creds_keytab(
    context: *mut c_void,
    creds: *mut c_void,
    principal: *mut c_void,
    keytab: *mut c_void,
    start_time: c_uint,
    in_tkt_service: *const c_char,
    options: *const c_void,
  ) -> c_int;

  fn krb5_free_cred_contents(context: *mut c_void, creds: *mut c_void);

  fn gss_display_status(
    minor_status: *mut c_uint,
    status_value: c_uint,
    status_type: c_uint,
    mech_type: *const c_void,
    message_context: *mut c_uint,
    status_string: *mut GssBufferDesc,
  ) -> c_uint;

  fn gss_init_sec_context(
    minor_status: *mut c_uint,
    initiator_cred_handle: *const c_void,
    context_handle: *mut *mut c_void,
    target_name: *const c_void,
    mech_type: *const c_void,
    req_flags: c_uint,
    time_req: c_uint,
    input_chan_bindings: *const c_void,
    input_token: *const GssBufferDesc,
    actual_mech_type: *mut *const c_void,
    output_token: *mut GssBufferDesc,
    ret_flags: *mut c_uint,
    time_rec: *mut c_uint,
  ) -> c_uint;

  fn gss_delete_sec_context(
    minor_status: *mut c_uint,
    context_handle: *mut *mut c_void,
    output_token: *mut GssBufferDesc,
  ) -> c_uint;

  fn gss_import_name(
    minor_status: *mut c_uint,
    input_name_buffer: *const GssBufferDesc,
    input_name_type: *const c_void,
    output_name: *mut *mut c_void,
  ) -> c_uint;
  fn gss_release_name(minor_status: *mut c_uint, name: *mut *mut c_void) -> c_uint;

  fn gss_wrap(
    minor_status: *mut c_uint,
    context_handle: *const c_void,
    conf_req_flag: c_int,
    qop_req: c_uint,
    input_message_buffer: *const GssBufferDesc,
    conf_state: *mut c_int,
    output_message_buffer: *mut GssBufferDesc,
  ) -> c_uint;

  fn gss_unwrap(
    minor_status: *mut c_uint,
    context_handle: *const c_void,
    input_message_buffer: *const GssBufferDesc,
    output_message_buffer: *mut GssBufferDesc,
    conf_state: *mut c_int,
    qop_state: *mut c_uint,
  ) -> c_uint;

  fn gss_release_buffer(minor_status: *mut c_uint, buffer: *mut GssBufferDesc) -> c_uint;
}

// OID 1.2.840.113554.1.2.2
#[cfg(target_os = "macos")]
const GSS_MECH_KRB5_BYTES: [u8; 9] = [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02];

#[cfg(target_os = "macos")]
const GSS_MECH_KRB5: GssOidDesc = GssOidDesc {
  length: 9,
  elements: GSS_MECH_KRB5_BYTES.as_ptr(),
};

// OID 1.2.840.113554.1.2.1.4
#[cfg(target_os = "macos")]
const GSS_C_NT_HOSTBASED_SERVICE_ELEMENTS: [u8; 10] = [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x01, 0x04];

#[cfg(target_os = "macos")]
const GSS_C_NT_HOSTBASED_SERVICE: GssOidDesc = GssOidDesc {
  length: 10,
  elements: GSS_C_NT_HOSTBASED_SERVICE_ELEMENTS.as_ptr(),
};

#[repr(C)]
#[derive(Debug)]
pub struct Krb5Data {
  pub magic: c_int,
  pub length: c_uint,
  pub data: *mut c_char,
}

#[repr(C)]
#[derive(Debug)]
pub struct Krb5Principal {
  pub magic: c_int,
  pub realm: Krb5Data,
  pub data: *mut Krb5Data,
  pub length: c_int,
  pub type_: c_int,
}

#[repr(C)]
#[derive(Debug)]
pub struct Krb5Keyblock {
  pub magic: c_int,
  pub enctype: c_int,
  pub length: c_uint,
  pub contents: *mut u8,
}

#[repr(C)]
#[derive(Debug)]
pub struct Krb5TicketTimes {
  pub authtime: c_int,
  pub starttime: c_int,
  pub endtime: c_int,
  pub renew_till: c_int,
}

#[repr(C)]
#[derive(Debug)]
pub struct Krb5Address {
  pub magic: c_int,
  pub addrtype: c_int,
  pub length: c_uint,
  pub contents: *mut u8,
}

#[repr(C)]
#[derive(Debug)]
pub struct Krb5Authdata {
  pub magic: c_int,
  pub ad_type: c_int,
  pub length: c_uint,
  pub contents: *mut u8,
}

#[repr(C)]
#[derive(Debug)]
pub struct Krb5Creds {
  pub magic: c_int,
  pub client: *const Krb5Principal,
  pub server: *const Krb5Principal,
  pub keyblock: Krb5Keyblock,
  pub times: Krb5TicketTimes,
  pub is_skey: c_int,
  pub ticket_flags: c_uint,
  pub addresses: *const *const Krb5Address,
  pub ticket: Krb5Data,
  pub second_ticket: Krb5Data,
  pub authdata: *const *const Krb5Authdata,
}

#[repr(C)]
#[derive(Debug)]
struct GssOidDesc {
  length: c_uint,
  elements: *const c_uchar,
}

#[repr(C)]
#[derive(Debug)]
struct GssBufferDesc {
  length: usize,
  value: *mut c_void,
}

#[napi(js_name = "GSSAPI")]
pub struct GSSAPI {
  config_path: String,
  cache_path: String,
  context: *mut c_void,
  cache: *mut c_void,
  gss: *mut c_void,
}

use std::sync::{Mutex, MutexGuard, OnceLock};

static GSSAPI_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn gssapi_lock() -> &'static Mutex<()> {
  GSSAPI_LOCK.get_or_init(|| Mutex::new(()))
}

#[napi(object)]
pub struct StepResult {
  pub output: Buffer,
  pub completed: bool,
}

struct EnvManager<'a> {
  _guard: MutexGuard<'a, ()>,
  prev_krbconfig: Option<String>,
  prev_krbcache: Option<String>,
}

impl<'a> EnvManager<'a> {
  pub fn new(config_path: &str, cache_path: &str) -> Self {
    let _guard = gssapi_lock().lock().unwrap();
    let prev_krbconfig = env::var("KRB5_CONFIG").ok();
    let prev_krbcache = env::var("KRB5CCNAME").ok();

    env::set_var("KRB5_CONFIG", config_path);
    env::set_var("KRB5CCNAME", cache_path);

    Self {
      _guard,
      prev_krbconfig,
      prev_krbcache,
    }
  }

  pub fn new_from_api(api: &GSSAPI) -> Self {
    EnvManager::new(&api.config_path, &api.cache_path)
  }
}

impl<'a> Drop for EnvManager<'a> {
  fn drop(&mut self) {
    if let Some(prev) = &self.prev_krbconfig {
      env::set_var("KRB5_CONFIG", prev);
    } else {
      env::remove_var("KRB5_CONFIG");
    }

    if let Some(prev) = &self.prev_krbcache {
      env::set_var("KRB5CCNAME", prev);
    } else {
      env::remove_var("KRB5CCNAME");
    }
  }
}

// TODO: Use RAII wrappers for everything
// TODO: Make all function async
#[napi]
impl GSSAPI {
  #[napi(constructor)]
  pub unsafe fn new(kdc: String, realm: String) -> Result<Self> {
    let uuid = uuid::Uuid::new_v4().to_string();
    let temp_dir = env::temp_dir();

    let config_path = temp_dir
      .join(format!("plt-kafka-krb5-{}.conf", uuid))
      .to_string_lossy()
      .to_string();

    let cache_path = temp_dir
      .join(format!("plt-kafka-krb5-{}.cache", uuid))
      .to_string_lossy()
      .to_string();

    let _env = EnvManager::new(&config_path, &cache_path);

    // Write the config file
    {
      let config = format!(
        r#"
[libdefaults]
  default_realm = {0}
  default_ccache_name = FILE:{2}

[realms]
  {0} = {{
    kdc = {1}
  }}
"#,
        realm, kdc, cache_path
      );

      if let Err(e) = std::fs::write(&config_path, config) {
        return Err(Error::from_reason(format!("Failed to write Kerberos config: {}", e)));
      }
    }

    // Create the Kerberos context
    let mut context = std::ptr::null_mut();
    let ret = krb5_init_context(&mut context);

    if ret != 0 {
      let _ = std::fs::remove_file(&config_path);
      let _ = std::fs::remove_file(&cache_path);

      return Err(Error::from_reason(format!(
        "krb5_init_context failed with error code {}.",
        ret
      )));
    }

    // Get the default cache
    let mut cache = std::ptr::null_mut();
    let ret = krb5_cc_default(context, &mut cache);

    if ret != 0 {
      krb5_free_context(context);

      let _ = std::fs::remove_file(&config_path);

      return Err(Error::from_reason(format!(
        "krb5_cc_default failed with error code {}.",
        ret
      )));
    }

    Ok(Self {
      config_path,
      cache_path,
      context,
      cache,
      gss: std::ptr::null_mut(),
    })
  }

  #[napi]
  pub unsafe fn authenticate_with_password(&self, username: String, password: String) -> Result<()> {
    let _env = EnvManager::new_from_api(&self);

    // Parse the username to create a principal
    let username_c = CString::new(username).unwrap();
    let mut principal = std::ptr::null_mut();
    let ret = krb5_parse_name(self.context, username_c.as_ptr(), &mut principal);

    if ret != 0 {
      return Err(self.format_kerberos_error("krb5_parse_name failed", ret));
    }

    let password_c = CString::new(password).unwrap();
    let mut creds: Krb5Creds = std::mem::zeroed();

    let ret = krb5_get_init_creds_password(
      self.context,
      &mut creds as *mut _ as *mut c_void,
      principal,
      password_c.as_ptr(),
      std::ptr::null(),
      std::ptr::null(),
      0,
      std::ptr::null(),
      std::ptr::null(),
    );

    if ret != 0 {
      krb5_free_principal(self.context, principal);

      if ret == KRB5_KDC_UNREACH {
        return Err(Error::from_reason(format!("Unable to reach the KDC.")));
      } else if ret == KRB5_REALM_CANT_RESOLVE {
        return Err(Error::from_reason(format!("Cannot resolve the realm.")));
      }

      return Err(self.format_kerberos_error("krb5_get_init_creds_password failed", ret));
    }

    let ret = krb5_cc_initialize(self.context, self.cache, principal);

    if ret != 0 {
      krb5_free_principal(self.context, principal);
      return Err(self.format_kerberos_error("krb5_cc_initialize failed", ret));
    }

    let ret = krb5_cc_store_cred(self.context, self.cache, &mut creds as *mut _ as *mut c_void);

    krb5_free_principal(self.context, principal);
    krb5_free_cred_contents(self.context, &mut creds as *mut _ as *mut c_void);

    if ret != 0 {
      return Err(self.format_kerberos_error("krb5_cc_store_cred failed", ret));
    }

    Ok(())
  }

  #[napi]
  pub unsafe fn authenticate_with_keytab(&self, username: String, keytab: String) -> Result<()> {
    let _env = EnvManager::new_from_api(&self);

    if !Path::new(&keytab).exists() {
      return Err(Error::from_reason(format!("Keytab file not found: {}", keytab)));
    }

    // Parse the username to create a principal
    let username_c = CString::new(username).unwrap();
    let mut principal = std::ptr::null_mut();
    let ret = krb5_parse_name(self.context, username_c.as_ptr(), &mut principal);

    if ret != 0 {
      return Err(self.format_kerberos_error("krb5_parse_name failed", ret));
    }

    // Resolve keytab
    let keytab_str = format!("FILE:{}", keytab);
    let keytab_c = CString::new(keytab_str).unwrap();
    let mut keytab = std::ptr::null_mut();
    let ret = krb5_kt_resolve(self.context, keytab_c.as_ptr(), &mut keytab);

    if ret != 0 {
      krb5_free_principal(self.context, principal);
      return Err(self.format_kerberos_error("krb5_kt_resolve failed", ret));
    }

    // Get credentials from keytab
    let mut creds: Krb5Creds = std::mem::zeroed();
    let ret = krb5_get_init_creds_keytab(
      self.context,
      &mut creds as *mut _ as *mut c_void,
      principal,
      keytab,
      0,
      std::ptr::null(),
      std::ptr::null(),
    );
    krb5_kt_close(self.context, keytab);

    if ret != 0 {
      krb5_free_principal(self.context, principal);

      if ret == KRB5_KDC_UNREACH {
        return Err(Error::from_reason(format!("Unable to reach the KDC.")));
      } else if ret == KRB5_REALM_CANT_RESOLVE {
        return Err(Error::from_reason(format!("Cannot resolve the realm.")));
      }

      return Err(self.format_kerberos_error("krb5_get_init_creds_keytab failed", ret));
    }

    let ret = krb5_cc_initialize(self.context, self.cache, principal);

    if ret != 0 {
      krb5_free_principal(self.context, principal);
      return Err(self.format_kerberos_error("krb5_cc_initialize failed", ret));
    }

    let ret = krb5_cc_store_cred(self.context, self.cache, &mut creds as *mut _ as *mut c_void);

    krb5_free_principal(self.context, principal);
    krb5_free_cred_contents(self.context, &mut creds as *mut _ as *mut c_void);

    if ret != 0 {
      return Err(self.format_kerberos_error("krb5_cc_store_cred failed", ret));
    }

    Ok(())
  }

  #[napi]
  pub unsafe fn step(&mut self, service: String, input: Option<Buffer>) -> Result<StepResult> {
    let _env = EnvManager::new_from_api(&self);
    let mut minor: c_uint = 0;

    let service_buf = GssBufferDesc {
      length: service.len(),
      value: service.as_ptr() as *mut c_void,
    };

    let mut target_name = std::ptr::null_mut();
    let ret = gss_import_name(
      &mut minor,
      &service_buf,
      &GSS_C_NT_HOSTBASED_SERVICE as *const _ as *const c_void,
      &mut target_name,
    );

    if ret != 0 {
      return Err(self.format_gss_error("gss_import_name failed", ret, minor));
    }

    let mut output = GssBufferDesc {
      length: 0,
      value: std::ptr::null_mut(),
    };

    let input_buf = input.map(|buf| GssBufferDesc {
      length: buf.len(),
      value: buf.as_ptr() as *mut c_void,
    });

    let ret = gss_init_sec_context(
      &mut minor,
      std::ptr::null(),
      &mut self.gss,
      target_name,
      &GSS_MECH_KRB5 as *const _ as *const c_void,
      0,
      0,
      std::ptr::null(),
      input_buf.as_ref().map_or(std::ptr::null(), |b| b as *const _),
      std::ptr::null_mut(),
      &mut output,
      std::ptr::null_mut(),
      std::ptr::null_mut(),
    );
    let init_minor = minor;

    const GSS_S_COMPLETE: c_uint = 0;
    const GSS_S_CONTINUE_NEEDED: c_uint = 1;

    gss_release_name(&mut minor, &mut target_name);

    if ret != GSS_S_COMPLETE && ret != GSS_S_CONTINUE_NEEDED {
      return Err(self.format_gss_error("gss_init_sec_context failed", ret, init_minor));
    }

    let result = Buffer::from(std::slice::from_raw_parts(
      output.value as *const c_uchar,
      output.length,
    ));
    gss_release_buffer(&mut minor, &mut output);

    Ok(StepResult {
      output: result,
      completed: ret == GSS_S_COMPLETE,
    })
  }

  #[napi]
  pub unsafe fn wrap(&self, data: Buffer) -> Result<Buffer> {
    let mut minor: c_uint = 0;

    let input = GssBufferDesc {
      length: data.len(),
      value: data.as_ptr() as *mut c_void,
    };

    let mut output = GssBufferDesc {
      length: 0,
      value: std::ptr::null_mut(),
    };

    let ret = gss_wrap(&mut minor, self.gss, 0, 0, &input, std::ptr::null_mut(), &mut output);

    if ret != 0 {
      return Err(self.format_gss_error("gss_wrap failed", ret, minor));
    }

    let result = Buffer::from(std::slice::from_raw_parts(
      output.value as *const c_uchar,
      output.length,
    ));

    gss_release_buffer(&mut minor, &mut output);
    Ok(result)
  }

  #[napi]
  pub unsafe fn unwrap(&self, data: Buffer) -> Result<Buffer> {
    let mut minor: c_uint = 0;

    let input = GssBufferDesc {
      length: data.len(),
      value: data.as_ptr() as *mut c_void,
    };

    let mut output = GssBufferDesc {
      length: 0,
      value: std::ptr::null_mut(),
    };

    let ret = gss_unwrap(
      &mut minor,
      self.gss,
      &input,
      &mut output,
      std::ptr::null_mut(),
      std::ptr::null_mut(),
    );

    if ret != 0 {
      return Err(self.format_gss_error("gss_unwrap failed", ret, minor));
    }

    let result = Buffer::from(std::slice::from_raw_parts(
      output.value as *const c_uchar,
      output.length,
    ));

    gss_release_buffer(&mut minor, &mut output);
    Ok(result)
  }

  unsafe fn format_kerberos_error(&self, prefix: &str, code: c_int) -> Error {
    let message = krb5_get_error_message(self.context, code);
    let error = Error::from_reason(format!(
      "{}: {}. (error code {})",
      prefix,
      CStr::from_ptr(message).to_string_lossy(),
      code
    ));
    krb5_free_error_message(self.context, message);

    return error;
  }

  unsafe fn format_gss_error(&self, prefix: &str, major: c_uint, minor: c_uint) -> Error {
    let mut msg_ctx: c_uint = 0;
    let mut status_string = GssBufferDesc {
      length: 0,
      value: std::ptr::null_mut(),
    };
    let mut min: c_uint = 0;

    let ret = gss_display_status(
      &mut min,
      major,
      GSS_C_GSS_CODE,
      std::ptr::null(),
      &mut msg_ctx,
      &mut status_string,
    );

    if ret != 0 {
      return Error::from_reason(format!("{}: unknown error. (error code {})", prefix, major));
    }

    let mut error_message = std::str::from_utf8(std::slice::from_raw_parts(
      status_string.value as *const c_uchar,
      status_string.length,
    ))
    .unwrap()
    .to_string();

    gss_release_buffer(&mut min, &mut status_string);
    if minor != 0 {
      let ret = gss_display_status(
        &mut min,
        minor,
        GSS_C_MECH_CODE,
        std::ptr::null(),
        &mut msg_ctx,
        &mut status_string,
      );

      if ret == 0 {
        error_message.push_str(
          std::str::from_utf8(std::slice::from_raw_parts(
            status_string.value as *const c_uchar,
            status_string.length,
          ))
          .unwrap(),
        );

        gss_release_buffer(&mut min, &mut status_string);
      }
    }

    let error = Error::from_reason(format!(
      "{}: {}. (error code {} - {})",
      prefix, error_message, major, minor
    ));

    error
  }
}

impl Drop for GSSAPI {
  fn drop(&mut self) {
    unsafe {
      let _ = std::fs::remove_file(self.config_path.clone());
      let _ = std::fs::remove_file(self.cache_path.clone());

      if !self.gss.is_null() {
        let mut minor: c_uint = 0;
        gss_delete_sec_context(&mut minor, &mut self.gss, std::ptr::null_mut());
      }

      if !self.cache.is_null() {
        krb5_cc_close(self.context, self.cache);
      }

      if !self.context.is_null() {
        krb5_free_context(self.context);
      }
    }
  }
}
