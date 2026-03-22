extern crate proc_macro;

mod args;
mod bound;
mod expand;
mod lifetime;
mod parse;
mod receiver;
mod verbatim;

use args::Args;
use parse::Item;
use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;

pub(crate) use expand::expand as async_trait_expand;

pub fn async_trait_nobox(args: TokenStream, input: TokenStream) -> TokenStream {
    #[cfg(feature = "smallbox")]
    let use_smallbox = true;
    #[cfg(not(feature = "smallbox"))]
    let use_smallbox = false;

    let args = parse_macro_input!(args as Args);
    let mut item = parse_macro_input!(input as Item);
    async_trait_expand(&mut item, args.local, use_smallbox);
    TokenStream::from(quote! {
        #[allow(manual_async_fn)]
        #item
    })
}
