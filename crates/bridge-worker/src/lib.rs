//! Cloudflare Workers 运行时适配，平台绑定仅在 wasm32 目标下编译。

#[cfg(any(target_arch = "wasm32", test))]
mod delivery;
#[cfg(target_arch = "wasm32")]
mod runtime;
#[cfg(target_arch = "wasm32")]
pub use runtime::*;
