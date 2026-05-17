//! Proc macros for ergonomic Boardwalk drivers.
//!
//! ```ignore
//! use boardwalk::{Device, DeviceConfig, DeviceError};
//! use boardwalk_macros::device;
//!
//! pub struct Led { pub on: bool }
//!
//! #[device]
//! impl Led {
//!     fn config(&self, cfg: &mut DeviceConfig) {
//!         cfg.type_("led").state(self.state())
//!             .when("off", &["turn-on"]).when("on", &["turn-off"]);
//!     }
//!     fn state(&self) -> &str { if self.on { "on" } else { "off" } }
//!
//!     #[transition]
//!     async fn turn_on(&mut self) -> Result<(), DeviceError> {
//!         self.on = true; Ok(())
//!     }
//!     #[transition]
//!     async fn turn_off(&mut self) -> Result<(), DeviceError> {
//!         self.on = false; Ok(())
//!     }
//! }
//! ```
//!
//! `#[device]` generates a `Device` trait impl that forwards `config`
//! and `state` to the user's inherent methods and dispatches transition
//! names (snake_case → kebab-case) to the `#[transition]` methods.

use proc_macro::TokenStream;
use quote::quote;
use syn::{ImplItem, ItemImpl, parse_macro_input};

/// Marker attribute. Recognized by `#[device]` on methods.
/// On its own (without `#[device]`), it's a no-op.
#[proc_macro_attribute]
pub fn transition(_attr: TokenStream, input: TokenStream) -> TokenStream {
    input
}

#[proc_macro_attribute]
pub fn device(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let mut impl_block = parse_macro_input!(input as ItemImpl);
    let self_ty = impl_block.self_ty.clone();
    let (impl_generics, _ty_generics, where_clause) = impl_block.generics.split_for_impl();

    // Collect transition methods and strip the #[transition] attribute
    // so the generated impl block has clean inherent methods.
    let mut transitions: Vec<(syn::Ident, String)> = Vec::new();
    for item in &mut impl_block.items {
        if let ImplItem::Fn(method) = item {
            let mut keep = Vec::with_capacity(method.attrs.len());
            let mut is_transition = false;
            for attr in method.attrs.drain(..) {
                let is_tr = attr
                    .path()
                    .segments
                    .last()
                    .map(|s| s.ident == "transition")
                    .unwrap_or(false);
                if is_tr {
                    is_transition = true;
                } else {
                    keep.push(attr);
                }
            }
            method.attrs = keep;
            if is_transition {
                let name = method.sig.ident.clone();
                let wire = name.to_string().replace('_', "-");
                transitions.push((name, wire));
            }
        }
    }

    let arms = transitions.iter().map(|(method, wire)| {
        quote! {
            #wire => self.#method().await,
        }
    });

    let expanded = quote! {
        #impl_block

        impl #impl_generics ::boardwalk::Device for #self_ty #where_clause {
            fn config(&self, cfg: &mut ::boardwalk::DeviceConfig) {
                <#self_ty>::config(self, cfg)
            }
            fn state(&self) -> &str {
                <#self_ty>::state(self)
            }
            fn transition<'__a>(
                &'__a mut self,
                name: &'__a str,
                _input: ::boardwalk::TransitionInput,
            ) -> ::futures::future::BoxFuture<
                '__a,
                ::std::result::Result<(), ::boardwalk::DeviceError>,
            > {
                ::std::boxed::Box::pin(async move {
                    match name {
                        #(#arms)*
                        other => ::std::result::Result::Err(
                            ::boardwalk::DeviceError::Invalid(
                                ::std::format!("unknown transition `{}`", other),
                            ),
                        ),
                    }
                })
            }
        }
    };

    expanded.into()
}
