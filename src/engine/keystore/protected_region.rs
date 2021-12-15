use std::ops::Deref;
use std::sync::Arc;

#[cfg(all(target_arch = "x86", not(target_env = "sgx"), target_feature = "sse"))]
use ::core::arch::x86 as arch;
#[cfg(all(target_arch = "x86_64", not(target_env = "sgx")))]
use ::core::arch::x86_64 as arch;

pub struct Pkey {
    handle: Option<libc::c_int>,
}

impl Pkey {
    pub fn new(allow_unprotected: bool) -> std::io::Result<Arc<Self>> {
        // Check if protection keys are supported
        if !is_ospke_supported() {
            return if allow_unprotected {
                // Return an empty handle if protection keys are not supported
                log::error!(
                    "Protections keys are not supported by this CPU. \
                    Skipping keystore memory protection"
                );
                Ok(Arc::new(Self { handle: None }))
            } else {
                // Return an error
                Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "protection keys are not supported by this CPU",
                ))
            };
        }

        // SAFETY: syscall will either return -1 if SYS_pkey_alloc is not supported
        // or return result according to https://man7.org/linux/man-pages/man2/pkey_alloc.2.html
        let pkey = unsafe { libc::syscall(libc::SYS_pkey_alloc, 0usize, PKEY_DISABLE_ACCESS) };

        if pkey < 0 && allow_unprotected {
            // Return an empty handle if no protection keys left
            log::error!("Protection keys allocation failed. Skipping keystore memory protection");
            Ok(Arc::new(Self { handle: None }))
        } else if pkey < 0 {
            // Return an error if no protection keys left
            Err(std::io::Error::last_os_error())
        } else {
            // There are available protection keys
            Ok(Arc::new(Self {
                handle: Some(pkey as libc::c_int),
            }))
        }
    }

    pub fn make_region<T>(self: &Arc<Self>, initial: T) -> std::io::Result<Arc<ProtectedRegion<T>>>
    where
        T: Sized,
    {
        ProtectedRegion::new(self, initial)
    }

    fn set(&self, rights: usize) {
        if let Some(handle) = self.handle {
            // SAFETY: handle will only be Some if WRPKRU command is supported
            unsafe { pkey_set(handle, rights) };
        }
    }
}

impl Drop for Pkey {
    fn drop(&mut self) {
        let handle = match self.handle {
            Some(handle) => handle as usize,
            None => return,
        };

        // SAFETY: syscall will either return -1 if SYS_pkey_free is not supported
        // or return result according to https://man7.org/linux/man-pages/man2/pkey_alloc.2.html
        //
        // All protected regions contain pkey as Arc so it will only be destroyed
        // if there are no regions left.
        if unsafe { libc::syscall(libc::SYS_pkey_free, handle) } < 0 {
            log::error!("failed to free pkey: {}", std::io::Error::last_os_error());
        }
    }
}

pub struct ProtectedRegion<T> {
    pkey: Arc<Pkey>,
    ptr: *mut libc::c_void,
    _marker: std::marker::PhantomData<T>,
}

impl<T> ProtectedRegion<T> {
    fn new(pkey: &Arc<Pkey>, initial: T) -> std::io::Result<Arc<Self>>
    where
        T: Sized,
    {
        if std::mem::size_of::<T>() > PAGE_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "protected region is not supported for huge pages",
            ));
        }

        // SAFETY: all parameters are passed according to
        // https://man7.org/linux/man-pages/man2/mmap.2.html
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                PAGE_SIZE,
                libc::PROT_NONE,
                libc::MAP_ANON | libc::MAP_PRIVATE,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }

        // SAFETY: it is called with backward capability with mprotect
        // https://man7.org/linux/man-pages/man2/mprotect.2.html
        let res = unsafe {
            libc::syscall(
                libc::SYS_pkey_mprotect,
                ptr as usize,
                PAGE_SIZE,
                libc::PROT_READ | libc::PROT_WRITE,
                pkey.handle.unwrap_or_default(),
            )
        };
        if res < 0 {
            return Err(std::io::Error::last_os_error());
        }

        // Enable memory access
        pkey.set(0);

        // SAFETY: ptr is always aligned to PAGE_SIZE (4KB) and not null
        unsafe { (ptr as *mut T).write(initial) };

        // Disable memory access
        pkey.set(PKEY_DISABLE_ACCESS);

        Ok(Arc::new(Self {
            pkey: pkey.clone(),
            ptr,
            _marker: std::marker::PhantomData::default(),
        }))
    }

    pub fn lock(&'_ self) -> ProtectedRegionGuard<'_, T> {
        ProtectedRegionGuard::new(self)
    }
}

impl<T> Drop for ProtectedRegion<T> {
    fn drop(&mut self) {
        // Enable memory access to run destructor
        self.pkey.set(0);

        // SAFETY: region still exists, properly aligned and accessible to read/write
        unsafe { std::ptr::drop_in_place(self.ptr as *mut T) };

        // Disable memory access
        self.pkey.set(PKEY_DISABLE_ACCESS);

        // SAFETY: region still exists, ptr and length were initialized once on creation
        if unsafe { libc::munmap(self.ptr, PAGE_SIZE) } < 0 {
            log::error!("failed to unmap file: {}", std::io::Error::last_os_error());
        }
    }
}

pub struct ProtectedRegionGuard<'a, T> {
    region: &'a ProtectedRegion<T>,
    _marker: std::marker::PhantomData<*const u8>,
}

impl<'a, T> ProtectedRegionGuard<'a, T> {
    fn new(region: &'a ProtectedRegion<T>) -> Self {
        region.pkey.set(0);
        Self {
            region,
            _marker: Default::default(),
        }
    }
}

impl<T> Deref for ProtectedRegionGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: ptr always points to the allocated page
        unsafe { &*(self.region.ptr as *const T) }
    }
}

impl<T> Drop for ProtectedRegionGuard<'_, T> {
    fn drop(&mut self) {
        self.region.pkey.set(PKEY_DISABLE_ACCESS);
    }
}

/// See https://www.felixcloutier.com/x86/wrpkru
fn is_ospke_supported() -> bool {
    const EAX_VENDOR_INFO: u32 = 0x0;
    const EAX_STRUCTURED_EXTENDED_FEATURE_INFO: u32 = 0x7;
    const OSPKE_BIT: u32 = 0b10000;

    // Check if extended feature info leaf is supported
    let vendor_leaf = cpuid_count(EAX_VENDOR_INFO, 0);
    if vendor_leaf.eax < EAX_STRUCTURED_EXTENDED_FEATURE_INFO {
        return false;
    }

    // Check if CR4.PKE=1
    let info = cpuid_count(EAX_STRUCTURED_EXTENDED_FEATURE_INFO, 0);
    info.ecx & OSPKE_BIT == OSPKE_BIT
}

struct CpuIdResult {
    eax: u32,
    ecx: u32,
}

fn cpuid_count(eax: u32, ecx: u32) -> CpuIdResult {
    // Safety: CPUID is supported on all x86_64 CPUs and all x86 CPUs with
    // SSE, but not by SGX.
    let result = unsafe { self::arch::__cpuid_count(eax, ecx) };
    CpuIdResult {
        eax: result.eax,
        ecx: result.ecx,
    }
}

#[link(name = "pkey-sys")]
extern "C" {
    fn pkey_set(pkeyu: libc::c_int, rights: libc::size_t) -> libc::c_int;
}

const PKEY_DISABLE_ACCESS: usize = 1;

const PAGE_SIZE: usize = 4096;

#[cfg(test)]
mod tests {
    use super::*;

    struct TestStruct {
        test: bool,
        value: u32,
    }

    impl Drop for TestStruct {
        fn drop(&mut self) {
            println!("dropped {}", self.value);
        }
    }

    #[test]
    fn test_protected_region() {
        assert!(is_ospke_supported());

        let pkey = Pkey::new(false).unwrap();

        {
            let region = Arc::new(
                pkey.make_region(TestStruct {
                    test: true,
                    value: 123,
                })
                .unwrap(),
            );

            let guard = region.lock();
            println!("{}", guard.value);
        }
    }
}
