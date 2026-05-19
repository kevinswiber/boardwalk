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
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Attribute, ImplItem, ItemImpl, parse_macro_input};

/// Marker attribute. Recognized by `#[actor]` (and the legacy
/// `#[device]`) on methods. On its own, it's a no-op.
#[proc_macro_attribute]
pub fn transition(_attr: TokenStream, input: TokenStream) -> TokenStream {
    input
}

/// Resolve the path prefix used by generated code to reach the
/// `boardwalk` crate, honoring any rename in the consumer's
/// `Cargo.toml`. Falls back to `::boardwalk` if resolution fails.
fn boardwalk_path() -> TokenStream2 {
    match crate_name("boardwalk") {
        Ok(FoundCrate::Itself) => quote!(crate),
        Ok(FoundCrate::Name(name)) => {
            let ident = format_ident!("{}", name);
            quote!(::#ident)
        }
        Err(_) => quote!(::boardwalk),
    }
}

/// Return the `#[cfg(...)]` attributes from a method so the generated
/// match arm matches the method's compilation gating.
fn cfg_attrs(attrs: &[Attribute]) -> Vec<Attribute> {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("cfg") || attr.path().is_ident("cfg_attr"))
        .cloned()
        .collect()
}

/// Generate an `Actor` trait impl for an inherent `impl` block.
///
/// Each `#[transition]`-marked method is dispatched by its kebab-cased
/// wire name. Methods must take `&mut self`, a `TransitionCtx`, and a
/// `TransitionInput`, and return
/// `Result<TransitionOutcome, TransitionError>`. The user is still
/// responsible for `impl Resource for X` (Actor's supertrait).
///
/// `#[cfg(...)]` attributes on a `#[transition]` method are mirrored
/// onto its generated match arm, so transitions gated behind a feature
/// or `cfg(test)` only participate in dispatch when the method is
/// compiled in.
#[proc_macro_attribute]
pub fn actor(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let mut impl_block = parse_macro_input!(input as ItemImpl);
    let self_ty = impl_block.self_ty.clone();
    let (impl_generics, _ty_generics, where_clause) = impl_block.generics.split_for_impl();

    let mut transitions: Vec<(syn::Ident, String, Vec<Attribute>)> = Vec::new();
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
                let cfgs = cfg_attrs(&method.attrs);
                transitions.push((name, wire, cfgs));
            }
        }
    }

    let bw = boardwalk_path();
    let arms = transitions.iter().map(|(method, wire, cfgs)| {
        quote! {
            #(#cfgs)*
            #wire => self.#method(__ctx, __input).await,
        }
    });

    let expanded = quote! {
        #impl_block

        impl #impl_generics #bw::runtime::Actor for #self_ty #where_clause {
            fn transition<'__a>(
                &'__a mut self,
                __ctx: #bw::runtime::TransitionCtx,
                __name: &'__a str,
                __input: #bw::core::TransitionInput,
            ) -> #bw::runtime::DynFuture<
                '__a,
                ::std::result::Result<
                    #bw::core::TransitionOutcome,
                    #bw::runtime::TransitionError,
                >,
            > {
                ::std::boxed::Box::pin(async move {
                    match __name {
                        #(#arms)*
                        other => ::std::result::Result::Err(
                            #bw::runtime::TransitionError::NotAllowed(
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
