//! Proc macros for ergonomic Boardwalk actors.
//!
//! ```ignore
//! use boardwalk::core::{TransitionInput, TransitionOutcome};
//! use boardwalk::runtime::{TransitionCtx, TransitionError};
//! use boardwalk_macros::{actor, transition};
//!
//! pub struct Led { pub on: bool }
//!
//! // The user supplies `impl Resource for Led { ... }` separately.
//!
//! #[actor]
//! impl Led {
//!     #[transition]
//!     async fn turn_on(
//!         &mut self,
//!         _ctx: TransitionCtx,
//!         _input: TransitionInput,
//!     ) -> Result<TransitionOutcome, TransitionError> {
//!         self.on = true;
//!         /* return TransitionOutcome::Completed { ... } */
//! #       unimplemented!()
//!     }
//! }
//! ```
//!
//! `#[actor]` generates an `Actor` trait impl whose `transition` method
//! dispatches the kebab-cased wire name to the `#[transition]`-marked
//! inherent methods. `#[device]` is the legacy variant retained until
//! the public device surface is deleted; new code should prefer
//! `#[actor]`.

use proc_macro::TokenStream;
use quote::quote;
use syn::{ImplItem, ItemImpl, parse_macro_input};

/// Marker attribute. Recognized by `#[actor]` (and the legacy
/// `#[device]`) on methods. On its own, it's a no-op.
#[proc_macro_attribute]
pub fn transition(_attr: TokenStream, input: TokenStream) -> TokenStream {
    input
}

/// Generate an `Actor` trait impl for an inherent `impl` block.
///
/// Each `#[transition]`-marked method is dispatched by its kebab-cased
/// wire name. Methods must take `&mut self`, a `TransitionCtx`, and a
/// `TransitionInput`, and return
/// `Result<TransitionOutcome, TransitionError>`. The user is still
/// responsible for `impl Resource for X` (Actor's supertrait).
#[proc_macro_attribute]
pub fn actor(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let mut impl_block = parse_macro_input!(input as ItemImpl);
    let self_ty = impl_block.self_ty.clone();
    let (impl_generics, _ty_generics, where_clause) = impl_block.generics.split_for_impl();

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
            #wire => self.#method(__ctx, __input).await,
        }
    });

    let expanded = quote! {
        #impl_block

        impl #impl_generics ::boardwalk::runtime::Actor for #self_ty #where_clause {
            fn transition<'__a>(
                &'__a mut self,
                __ctx: ::boardwalk::runtime::TransitionCtx,
                __name: &'__a str,
                __input: ::boardwalk::core::TransitionInput,
            ) -> ::boardwalk::runtime::DynFuture<
                '__a,
                ::std::result::Result<
                    ::boardwalk::core::TransitionOutcome,
                    ::boardwalk::runtime::TransitionError,
                >,
            > {
                ::std::boxed::Box::pin(async move {
                    match __name {
                        #(#arms)*
                        other => ::std::result::Result::Err(
                            ::boardwalk::runtime::TransitionError::NotAllowed(
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

// Legacy: deleted alongside the public Device surface in a later phase.
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
