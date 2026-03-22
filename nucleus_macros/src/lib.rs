#![allow(clippy::collapsible_if)]

use proc_macro::TokenStream;
use quote::quote;
use syn::{Attribute, ImplItem, ItemImpl, PathArguments, parse_macro_input, parse_quote};

#[cfg(feature = "static_registry")]
use crate::handler_macro::define_registry_inner;
use crate::handler_macro::handler_inner;

mod async_trait;
mod handler_macro;

#[proc_macro_attribute]
#[deprecated(since = "0.11.1", note = "use #[handler(delayed)] instead")]
pub fn handle_delayed(_args: TokenStream, input: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(input as ItemImpl);

    let async_trait_attr: Attribute = parse_quote!(#[async_trait::async_trait]);
    item.attrs.insert(0, async_trait_attr);

    expand_handle_delayed(&mut item);
    TokenStream::from(quote!(#item))
}

fn expand_handle_delayed(input: &mut ItemImpl) {
    let generic = match input
        .trait_
        .clone()
        .unwrap()
        .1
        .segments
        .last()
        .cloned()
        .unwrap()
        .arguments
    {
        PathArguments::AngleBracketed(ref args) => args.args.iter().next().cloned().unwrap(),
        _ => panic!("expected angle bracketed arguments"),
    };

    let nucleus_path = nucleus_path();

    let handler = input.items.iter().find(|item| match item {
        ImplItem::Fn(method) => method.sig.ident == "handle_delayed",
        _ => false,
    });
    if handler.is_none() {
        panic!("expected handle_delayed method");
    }

    input.items.push(ImplItem::Verbatim(quote! {
        async fn handle(&mut self, _msg: #generic, _ctx: &mut #nucleus_path::actor::ActorContext<Self>) -> <#generic as #nucleus_path::actor::message::Message>::Result {
            panic!("use handle_delayed")
        }
    }));
}

fn nucleus_path() -> syn::Path {
    // if we are in the nucleus crate itself, use `crate`, otherwise use `::nucleus`
    // this is because macros can be used inside the nucleus crate itself
    if std::env::var("CARGO_PKG_NAME").unwrap() == "nucleus" {
        // panic!("{}", std::env::var("CARGO_PKG_NAME").unwrap());
        syn::parse_quote!(crate)
    } else {
        syn::parse_quote!(::nucleus)
    }
}

#[cfg(feature = "static_registry")]
#[proc_macro]
pub fn define_registry(input: TokenStream) -> TokenStream {
    define_registry_inner(input)
}

#[cfg(feature = "static_registry")]
#[proc_macro]
pub fn define_default_registry(_input: TokenStream) -> TokenStream {
    define_registry_inner(quote! { DEFAULT_REGISTRY }.into())
}

#[proc_macro_attribute]
pub fn handler(args: TokenStream, input: TokenStream) -> TokenStream {
    handler_inner(args, input)
}

#[proc_macro_attribute]
pub fn async_trait_nobox(args: TokenStream, input: TokenStream) -> TokenStream {
    async_trait::async_trait_nobox(args, input)
}

mod test {
    #[test]
    fn test_basic() {
        use quote::ToTokens as _;
        use quote::quote;

        let input = quote! {
            impl Handler<TestMsg> for TestActor {
                async fn handle_delayed(&mut self, msg: TestMsg, ctx: &mut ActorContext<Self>, sender: Sender<<TestMsg as Message>::Result>) {
                    println!("hello");
                }
            }
        };

        let mut input_item = syn::parse2::<syn::ItemImpl>(input).unwrap();
        crate::expand_handle_delayed(&mut input_item);

        let expected = quote! {
            impl Handler<TestMsg> for TestActor {
                async fn handle_delayed(&mut self, msg: TestMsg, ctx: &mut ActorContext<Self>, sender: Sender< <TestMsg as Message>::Result>) {
                    println!("hello");
                }

                async fn handle(&mut self, _msg: TestMsg, _ctx: &mut ::nucleus::actor::ActorContext<Self>) -> <TestMsg as ::nucleus::actor::message::Message>::Result {
                    panic!("use handle_delayed")
                }
            }
        };

        assert_eq!(
            input_item.to_token_stream().to_string(),
            expected.to_string()
        );
    }

    #[test]
    fn test_impl_block() {
        use quote::quote;

        let input = quote! {
            #[handler(local)]
            impl TestActor {
                fn handle_set_num_message(&mut self, message: SetNumMessage, _ctx: &mut ActorContext<Self>) {
                    info!("Setting to number: {}", message.num);
                    self.last_num = message.num;
                }
            }
        };

        let input_item = syn::parse2::<syn::ItemImpl>(input).unwrap();
        let _expanded = crate::handler_macro::handler_inner_real(input_item, vec![]);

        println!("{}", _expanded.to_string());
    }
}
