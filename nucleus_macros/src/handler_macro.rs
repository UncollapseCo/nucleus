use std::{ops::Deref, sync::atomic::AtomicUsize};

use super::*;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Ident, LitStr, Path, Token, parse::Parse, punctuated::Punctuated};

#[cfg(feature = "static_registry")]
pub(crate) fn define_registry_inner(input: TokenStream) -> TokenStream {
    let name = parse_macro_input!(input as Ident);

    let nucleus_path = nucleus_path();

    let expanded: TokenStream2 = quote! {
        #[#nucleus_path::__private::linkme::distributed_slice]
        #[linkme(crate = #nucleus_path::__private::linkme)]
        pub static #name: [fn(#nucleus_path::net::remote::HandlerRegistryBuilder) -> #nucleus_path::net::remote::HandlerRegistryBuilder];
    };

    TokenStream::from(expanded)
}

pub static REGISTRY_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
struct HandlerArgs {
    // new option to replace #[handle_delayed]
    delayed: bool,

    // implies the Message is not Serializable, but should register a local handler anyway. This enables the use of weak actor refs.
    local: bool,

    // if this feature is disabled, the body of the functions will be excluded from compilation and replaced with a panic!
    feature_gate: Option<LitStr>,

    // do not use static registries to register the handler remotely, let the user do that manually
    // this still implements the HandlerMarker trait
    manual_register: bool,

    // which registries this handler should be registered in
    registry_names: Vec<Path>,
}

pub(crate) enum AttrArg {
    Word(Ident),
    NameValue { name: Ident, value: RegistryValue },
}

pub(crate) enum RegistryValue {
    Single(Path),
    Multiple(Vec<Path>),
    Literal(LitStr),
}

impl Parse for AttrArg {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;

        // Check if there's an `=` following
        if input.peek(Token![=]) {
            let _eq: Token![=] = input.parse()?;

            // Check if it's a parenthesized list
            let value = if input.peek(syn::token::Paren) {
                let content;
                syn::parenthesized!(content in input);
                let paths = Punctuated::<Path, Token![,]>::parse_terminated(&content)?;
                RegistryValue::Multiple(paths.into_iter().collect())
            } else if input.peek(syn::LitStr) {
                // Parse string literal
                RegistryValue::Literal(input.parse()?)
            } else {
                // Parse single path
                RegistryValue::Single(input.parse()?)
            };

            Ok(AttrArg::NameValue { name, value })
        } else {
            Ok(AttrArg::Word(name))
        }
    }
}

impl AttrArg {
    fn is_ident(&self, ident: &str) -> bool {
        match self {
            AttrArg::Word(name) => name == ident,
            AttrArg::NameValue { name, .. } => name == ident,
        }
    }

    fn get_ident(&self) -> Option<&Ident> {
        match self {
            AttrArg::Word(name) => Some(name),
            AttrArg::NameValue { name, .. } => Some(name),
        }
    }

    fn unwrap_litstr(&self) -> LitStr {
        match self {
            AttrArg::NameValue { value, .. } => match value {
                RegistryValue::Literal(litstr) => litstr.clone(),
                _ => panic!("expected string literal"),
            },
            _ => panic!("expected name value"),
        }
    }
}

pub fn handler_inner(args: TokenStream, input: TokenStream) -> TokenStream {
    let handler_item = parse_macro_input!(input as ItemImpl);
    let args = if args.is_empty() {
        vec![]
    } else {
        parse_macro_input!(args with syn::punctuated::Punctuated<AttrArg, syn::Token![,]>::parse_terminated)
            .into_iter()
            .collect()
    };

    let expanded = handler_inner_real(handler_item, args);
    TokenStream::from(expanded)
}

fn parse_handler_args(mut args: Vec<AttrArg>, base_args: Option<HandlerArgs>) -> HandlerArgs {
    let mut handler_args = base_args.unwrap_or(HandlerArgs {
        delayed: false,
        local: false,
        feature_gate: None,
        manual_register: false,
        registry_names: vec![],
    });

    if !args.is_empty() {
        args.retain(|meta| {
            if meta.is_ident("delayed") {
                handler_args.delayed = true;
                return false;
            } else if meta.is_ident("manual") {
                handler_args.manual_register = true;
                return false;
            } else if meta.is_ident("feature") {
                let r = meta.unwrap_litstr();
                handler_args.feature_gate = Some(r);
                return false;
            } else if meta.is_ident("local") {
                handler_args.local = true;
                return false;
            } else if meta.is_ident("registry") || meta.is_ident("registries") {
                let r = match meta {
                    AttrArg::NameValue { value, .. } => match value {
                        RegistryValue::Single(path) => vec![path.clone()],
                        RegistryValue::Multiple(paths) => paths.clone(),
                        RegistryValue::Literal(_) => {
                            panic!("expected path or list of paths for registry argument")
                        }
                    },
                    _ => panic!("expected name value for registry argument"),
                };
                handler_args.registry_names.extend(r);
                return false;
            }

            true
        });

        if !args.is_empty() {
            panic!(
                "unknown handler arguments: {}",
                args.iter()
                    .map(|m| m.get_ident().unwrap().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }

    if !handler_args.manual_register && handler_args.registry_names.is_empty() {
        // default registry
        handler_args
            .registry_names
            .push(parse_quote!(crate::DEFAULT_REGISTRY));
    }

    handler_args
}

pub(crate) fn handler_inner_real(handler_item: ItemImpl, args: Vec<AttrArg>) -> TokenStream2 {
    let is_trait_impl = handler_item.trait_.is_some();

    let handler_args = parse_handler_args(args, None);
    if is_trait_impl {
        expand_handler_trait_impl(handler_item, &handler_args)
    } else {
        expand_type_impl_block(handler_item, &handler_args)
    }
}

fn expand_type_impl_block(mut item_impl: ItemImpl, handler_args: &HandlerArgs) -> TokenStream2 {
    // Currently no special handling for type impls

    let mut handler_impls = vec![];

    let actor_type_path = {
        if let syn::Type::Path(t) = item_impl.self_ty.deref().clone() {
            t
        } else {
            unreachable!();
        }
    };

    for fun in item_impl.items.iter_mut() {
        if let ImplItem::Fn(fun) = fun {
            // we need to look at the args of the function. There should be &mut self, message, ctx, and optionally Sender<M::Result>
            // if there is a sender, this handler is "delayed", we determine this just by the count of arguments for now
            let is_delayed = fun.sig.inputs.len() == 4;
            // we do reordering of the args, in case developer swaps them around
            // assert that first is &mut self
            let inputs = &mut fun.sig.inputs;
            let self_arg = inputs.iter().next().expect("expected &mut self");
            if let syn::FnArg::Receiver(_) = self_arg {
                // ok
            } else {
                panic!("first argument must be &mut self");
            }

            // find ctx - should be of type &mut ActorContext<Self> - check for ActorContext identifier
            let ctx_arg_index = inputs
                .iter()
                .position(|arg| {
                    if let syn::FnArg::Typed(pat_type) = arg
                        && let syn::Type::Reference(type_ref) = pat_type.ty.deref()
                        && let syn::Type::Path(type_path) = type_ref.elem.deref()
                        && type_path.path.segments.last().unwrap().ident == "ActorContext"
                    {
                        true
                    } else {
                        false
                    }
                })
                .expect("expected ActorContext argument");

            let sender_index = {
                if is_delayed {
                    // find the Sender<> argument, check for Sender identifier
                    let sender_arg_index = inputs.iter().position(|arg| {
                        if let syn::FnArg::Typed(pat_type) = arg
                            && let syn::Type::Path(type_path) = pat_type.ty.deref()
                            && type_path.path.segments.last().unwrap().ident == "Sender"
                        {
                            true
                        } else {
                            false
                        }
                    });

                    Some(sender_arg_index.expect("expected Sender argument"))
                } else {
                    None
                }
            };

            // msg has to be the remaining argument
            let msg_arg_index = {
                let mut res_index = 0;
                for i in 0..inputs.len() {
                    // self, ctx, sender
                    if i != 0 && i != ctx_arg_index && Some(i) != sender_index {
                        res_index = i;
                        break;
                    }
                }
                if res_index == 0 {
                    panic!(
                        "expected message argument - argcount: {}, self: 0, ctx: {}, sender: {:?}",
                        inputs.len(),
                        ctx_arg_index,
                        sender_index
                    );
                }
                res_index
            };

            let msg_arg_type = {
                if let syn::FnArg::Typed(pat_type) = inputs.iter().nth(msg_arg_index).unwrap() {
                    pat_type.ty.deref().clone()
                } else {
                    panic!("expected typed argument for message");
                }
            };

            // reorder inputs to be: &mut self, message, ctx, (optional) sender
            let mut reordered_inputs = Punctuated::<syn::FnArg, Token![,]>::new();
            reordered_inputs.push(inputs.iter().next().unwrap().clone()); // &mut self
            reordered_inputs.push(inputs.iter().nth(msg_arg_index).unwrap().clone()); // message
            reordered_inputs.push(inputs.iter().nth(ctx_arg_index).unwrap().clone()); // ctx
            if let Some(sender_idx) = sender_index {
                reordered_inputs.push(inputs.iter().nth(sender_idx).unwrap().clone()); // sender
            }
            *inputs = reordered_inputs;

            let fn_name = fun.sig.ident.clone();
            let nucleus_path = nucleus_path();

            let mut handler_impl: ItemImpl = if is_delayed {
                parse_quote! {
                    impl #nucleus_path::actor::message::Handler<#msg_arg_type> for #actor_type_path {
                        async fn handle_delayed(&mut self, message: #msg_arg_type, ctx: &mut #nucleus_path::actor::ActorContext<Self>, sender: #nucleus_path::actor::Sender<<#msg_arg_type as #nucleus_path::actor::message::Message>::Result>) {
                            Self::#fn_name(self, message, ctx, sender).await
                        }
                    }
                }
            } else {
                parse_quote! {
                    impl #nucleus_path::actor::message::Handler<#msg_arg_type> for #actor_type_path {
                        async fn handle(&mut self, message: #msg_arg_type, ctx: &mut #nucleus_path::actor::ActorContext<Self>) -> <#msg_arg_type as #nucleus_path::actor::message::Message>::Result {
                            Self::#fn_name(self, message, ctx).await
                        }
                    }
                }
            };

            // check and remove handler() attr from the function
            let mut handler_attr = None;
            fun.attrs.retain(|attr| match &attr.meta {
                syn::Meta::Path(path)
                    if path.segments.last().map(|s| &s.ident) == Some(&parse_quote!(handler)) =>
                {
                    handler_attr = Some(attr.clone());
                    false
                }
                syn::Meta::List(l)
                    if l.path.segments.last().map(|s| &s.ident) == Some(&parse_quote!(handler)) =>
                {
                    handler_attr = Some(attr.clone());
                    false
                }
                _ => true,
            });

            let mut args_inner = if let Some(handler_attr) = handler_attr {
                match handler_attr.meta {
                    syn::Meta::List(meta_list) => {
                        let args: Vec<AttrArg> = syn::parse::Parser::parse2(
                            Punctuated::<AttrArg, syn::Token![,]>::parse_terminated,
                            meta_list.tokens,
                        )
                        .expect("failed to parse handler arguments")
                        .into_iter()
                        .collect();

                        parse_handler_args(args, Some(handler_args.clone()))
                    }
                    _ => handler_args.clone(),
                }
            } else {
                handler_args.clone()
            };
            args_inner.delayed = is_delayed;

            // we clone the attrs to pass on attrs such as #[cfg(test)]
            handler_impl.attrs = fun.attrs.clone();

            let expanded_handler_impl = expand_handler_trait_impl(handler_impl, &args_inner);

            handler_impls.push(expanded_handler_impl);
        } else {
            panic!("only functions are supported in impl blocks for handlers");
        }
    }

    quote! {
        #item_impl

        #(#handler_impls)*
    }
}

fn expand_handler_trait_impl(mut item_impl: ItemImpl, handler_args: &HandlerArgs) -> TokenStream2 {
    let nucleus_path = nucleus_path();
    let async_trait_attr: Attribute = parse_quote!(#[#nucleus_path::async_trait_nobox]);
    item_impl.attrs.insert(0, async_trait_attr);

    if handler_args.delayed {
        expand_handle_delayed(&mut item_impl);
    }

    let feature_gate = handler_args.feature_gate.as_ref().map(|feature| {
        quote! {
            #[cfg(feature = #feature)]
        }
    });

    if feature_gate.is_some() {
        let feature_lit = handler_args.feature_gate.as_ref().unwrap();
        let negative_gate = quote! {
            #[cfg(not(feature = #feature_lit))]
        };

        let mut empty_impls = vec![];

        for i in item_impl.items.iter_mut() {
            if let ImplItem::Fn(f) = i {
                let mut cloned = f.clone();
                cloned.block.stmts = vec![parse_quote! {
                    panic!("This function is disabled because the feature '{}' is not enabled", #feature_lit);
                }];
                cloned.attrs.insert(0, parse_quote!(#negative_gate));
                empty_impls.push(ImplItem::Fn(cloned));

                f.attrs.insert(
                    0,
                    parse_quote!(
                        #feature_gate
                    ),
                );
            }
        }

        item_impl.items.extend(empty_impls);
    }

    // register_#generics
    let fn_name = format!(
        "register_{}",
        REGISTRY_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    );
    let fn_ident = Ident::new(&fn_name, proc_macro2::Span::call_site());

    let nucleus_message_type = match item_impl
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

    let nucleus_actor_type = item_impl.self_ty.clone();
    let registry_names = handler_args.registry_names.clone();

    #[cfg(feature = "static_registry")]
    let use_static_registry = true;
    #[cfg(not(feature = "static_registry"))]
    let use_static_registry = false;

    let linkme_impl = if !handler_args.manual_register && use_static_registry {
        let register_method = if handler_args.local {
            quote! { with_local_handler }
        } else {
            quote! { with_handler }
        };

        quote! {
            #(
                #feature_gate
                #[#nucleus_path::__private::linkme::distributed_slice(#registry_names)]
                #[linkme(crate = #nucleus_path::__private::linkme)]
                fn #fn_ident (builder: #nucleus_path::net::remote::HandlerRegistryBuilder) -> #nucleus_path::net::remote::HandlerRegistryBuilder {
                    builder.#register_method::<#nucleus_actor_type, #nucleus_message_type>()
                }
            )*
        }
    } else {
        quote! {}
    };

    let handler_marker_impl = if use_static_registry {
        let mut cloned = item_impl.clone();
        cloned.items.clear();
        cloned.trait_.as_mut().unwrap().1 = parse_quote! {
            #nucleus_path::actor::message::HandlerMarker<#nucleus_message_type>
        };

        quote! {
            #cloned
        }
    } else {
        quote! {}
    };

    let expanded: TokenStream2 = quote! {
        #item_impl

        #handler_marker_impl

        // Register the init function in the specified registries
        #linkme_impl
    };

    expanded
}

#[test]
fn test_generics() {
    let code = r#"
    impl<A, T> Handler<PubsubMsgWrapper<T>> for A
    where
        A: Actor + Handler<PubsubMessage<T>>,
        T: Topic,
    {
        async fn handle_delayed(
            &mut self,
            message: PubsubMsgWrapper<T>,
            ctx: &mut ActorContext<Self>,
            sender: Sender<()>,
        ) {
            
        }
    }
    "#;

    let item_impl: ItemImpl = syn::parse_str(code).unwrap();
    let args = vec![AttrArg::Word(Ident::new(
        "delayed",
        proc_macro2::Span::call_site(),
    ))];

    let expanded = handler_inner_real(item_impl, args);

    println!("{}", expanded);
}
