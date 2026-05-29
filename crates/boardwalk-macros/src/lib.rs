//! Proc macros for ergonomic Boardwalk actors.
//!
//! ```ignore
//! use boardwalk::{TransitionInput, TransitionOutcome};
//! use boardwalk::runtime::{TransitionCtx, TransitionError};
//! use boardwalk_macros::{actor, on_start, transition};
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
//!
//!     #[on_start]
//!     async fn boot(
//!         &mut self,
//!         _ctx: boardwalk::runtime::ActorCtx,
//!     ) -> Result<(), boardwalk::runtime::ActorError> {
//!         Ok(())
//!     }
//! }
//! ```
//!
//! `#[actor]` generates an `Actor` trait impl whose `transition` method
//! dispatches the kebab-cased wire name to the `#[transition]`-marked
//! inherent methods.

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Attribute, ImplItem, ItemImpl, parse_macro_input};

/// Marker attribute. Recognized by `#[actor]` on methods. On its own,
/// it's a no-op.
#[proc_macro_attribute]
pub fn transition(_attr: TokenStream, input: TokenStream) -> TokenStream {
    input
}

/// Marker attribute. Recognized by `#[actor]` on methods. On its own,
/// it's a no-op.
#[proc_macro_attribute]
pub fn on_start(_attr: TokenStream, input: TokenStream) -> TokenStream {
    input
}

/// Marker attribute. Recognized by `#[actor]` on methods. On its own,
/// it's a no-op.
#[proc_macro_attribute]
pub fn on_stop(_attr: TokenStream, input: TokenStream) -> TokenStream {
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
    let mut on_start_method: Option<(syn::Ident, Vec<Attribute>)> = None;
    let mut on_stop_method: Option<(syn::Ident, Vec<Attribute>)> = None;
    for item in &mut impl_block.items {
        if let ImplItem::Fn(method) = item {
            let mut keep = Vec::with_capacity(method.attrs.len());
            let mut is_transition = false;
            let mut is_on_start = false;
            let mut is_on_stop = false;
            for attr in method.attrs.drain(..) {
                let marker = attr
                    .path()
                    .segments
                    .last()
                    .map(|s| s.ident.to_string())
                    .unwrap_or_default();
                if marker == "transition" {
                    is_transition = true;
                } else if marker == "on_start" {
                    is_on_start = true;
                } else if marker == "on_stop" {
                    is_on_stop = true;
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
            if is_on_start && on_start_method.is_none() {
                on_start_method = Some((method.sig.ident.clone(), cfg_attrs(&method.attrs)));
            }
            if is_on_stop && on_stop_method.is_none() {
                on_stop_method = Some((method.sig.ident.clone(), cfg_attrs(&method.attrs)));
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
    let on_start_fn = on_start_method.map(|(method, cfgs)| {
        quote! {
            #(#cfgs)*
            fn on_start<'__a>(
                &'__a mut self,
                __ctx: #bw::runtime::ActorCtx,
            ) -> #bw::runtime::DynFuture<
                '__a,
                ::std::result::Result<(), #bw::runtime::ActorError>,
            > {
                ::std::boxed::Box::pin(async move { self.#method(__ctx).await })
            }
        }
    });
    let on_stop_fn = on_stop_method.map(|(method, cfgs)| {
        quote! {
            #(#cfgs)*
            fn on_stop<'__a>(
                &'__a mut self,
                __ctx: #bw::runtime::ActorCtx,
            ) -> #bw::runtime::DynFuture<
                '__a,
                ::std::result::Result<(), #bw::runtime::ActorError>,
            > {
                ::std::boxed::Box::pin(async move { self.#method(__ctx).await })
            }
        }
    });

    let expanded = quote! {
        #impl_block

        impl #impl_generics #bw::runtime::Actor for #self_ty #where_clause {
            fn transition<'__a>(
                &'__a mut self,
                __ctx: #bw::runtime::TransitionCtx,
                __name: &'__a str,
                __input: #bw::TransitionInput,
            ) -> #bw::runtime::DynFuture<
                '__a,
                ::std::result::Result<
                    #bw::TransitionOutcome,
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

            #on_start_fn
            #on_stop_fn
        }
    };

    expanded.into()
}
