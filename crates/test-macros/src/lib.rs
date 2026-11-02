//! `#[dbos_test_support::test]`, re-exported from `dbos-test-support`; see `block_on` there.
//!
//! A crate of its own only because an attribute has to be a procedural macro, and a
//! procedural-macro crate can export nothing else.

use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_macro_input};

/// Runs an `async fn` test on the runtime `DBOS_TEST_RUNTIME` names: tokio unless it says
/// otherwise.
///
/// What `#[tokio::test]` is to tokio, `#[sqlx::test]` to sqlx's runtimes: the body becomes an
/// `async` block, and a plain `#[test]` drives it to completion through
/// `dbos_test_support::block_on`.
#[proc_macro_attribute]
pub fn test(args: TokenStream, item: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "`#[dbos_test_support::test]` takes no arguments",
        )
        .into_compile_error()
        .into();
    }
    let mut function = parse_macro_input!(item as ItemFn);
    if function.sig.asyncness.take().is_none() {
        return syn::Error::new_spanned(function.sig.fn_token, "the test must be an `async fn`")
            .into_compile_error()
            .into();
    }
    let body = &function.block;
    let block = quote! {
        {
            ::dbos_test_support::block_on(async move #body)
        }
    };
    *function.block = syn::parse2(block).expect("a block parses");
    quote! {
        #[::core::prelude::v1::test]
        #function
    }
    .into()
}
