//! The `#[event]`, `#[command]` and `#[query]` attributes of kernable-sdk. Use them through
//! the SDK (`kernable::event`), which documents them; this crate only generates the code.
//!
//! Each turns a braced struct into a message of a domain: it derives serde with camelCase field
//! names (the log's), writes an event's `None` as `null` and leaves a command's or query's out,
//! implements the SDK's message traits, and
//! declares a constant of the struct's name that stands for the type where the SDK takes one:
//! `tenant.apply(TenantCreated, |s, d, _, _| ...)`.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as Tokens};
use quote::{format_ident, quote};
use syn::{
    Error, Fields, Ident, ItemStruct, LitInt, LitStr, Path, Token, Type, bracketed, parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

/// An event of a domain: `#[event("tenancy")]`, optionally with `version = 2`,
/// `upcasts = path` (a `fn(EventSchema) -> EventSchema` adding the steps), `personal(fields =
/// [email, name], subject = path)` (repeatable), `erases = path` and `type = "OtherName"`.
#[proc_macro_attribute]
pub fn event(args: TokenStream, item: TokenStream) -> TokenStream {
    expand(Kind::Event, args, item)
}

/// A command of a domain's aggregate: `#[command("tenancy", aggregate = "tenant")]`, optionally
/// with `personal(fields = [...], subject = path)` (repeatable), `secret = [password]` and
/// `type = "OtherName"`.
#[proc_macro_attribute]
pub fn command(args: TokenStream, item: TokenStream) -> TokenStream {
    expand(Kind::Command, args, item)
}

/// A query of a domain: `#[query("tenancy", answer = TenantRow)]` (default answer: any JSON),
/// optionally with `type = "OtherName"`.
#[proc_macro_attribute]
pub fn query(args: TokenStream, item: TokenStream) -> TokenStream {
    expand(Kind::Query, args, item)
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Event,
    Command,
    Query,
}

struct Personal {
    fields: Vec<Ident>,
    subject: Path,
}

#[derive(Default)]
struct Args {
    domain: Option<LitStr>,
    type_name: Option<LitStr>,
    version: Option<LitInt>,
    upcasts: Option<Path>,
    erases: Option<Path>,
    aggregate: Option<LitStr>,
    answer: Option<Type>,
    secret: Vec<Ident>,
    personal: Vec<Personal>,
}

fn idents(input: ParseStream) -> syn::Result<Vec<Ident>> {
    let content;
    bracketed!(content in input);
    Ok(Punctuated::<Ident, Token![,]>::parse_terminated(&content)?
        .into_iter()
        .collect())
}

impl Parse for Args {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut args = Args {
            domain: Some(input.parse()?),
            ..Args::default()
        };
        while input.parse::<Option<Token![,]>>()?.is_some() && !input.is_empty() {
            let key: Ident = if input.peek(Token![type]) {
                input.parse::<Token![type]>()?;
                Ident::new("type", Span::call_site())
            } else {
                input.parse()?
            };
            if key == "personal" {
                let content;
                parenthesized!(content in input);
                let (mut fields, mut subject) = (None, None);
                while !content.is_empty() {
                    let k: Ident = content.parse()?;
                    content.parse::<Token![=]>()?;
                    match k.to_string().as_str() {
                        "fields" => fields = Some(idents(&content)?),
                        "subject" => subject = Some(content.parse()?),
                        _ => return Err(Error::new(k.span(), "personal takes fields and subject")),
                    }
                    content.parse::<Option<Token![,]>>()?;
                }
                let (Some(fields), Some(subject)) = (fields, subject) else {
                    return Err(Error::new(
                        key.span(),
                        "personal(fields = [...], subject = path)",
                    ));
                };
                args.personal.push(Personal { fields, subject });
                continue;
            }
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "type" => args.type_name = Some(input.parse()?),
                "version" => args.version = Some(input.parse()?),
                "upcasts" => args.upcasts = Some(input.parse()?),
                "erases" => args.erases = Some(input.parse()?),
                "aggregate" => args.aggregate = Some(input.parse()?),
                "answer" => args.answer = Some(input.parse()?),
                "secret" => args.secret = idents(input)?,
                other => return Err(Error::new(key.span(), format!("unknown argument {other}"))),
            }
        }
        Ok(args)
    }
}

/// `account_id` → `accountId`, as serde's `rename_all = "camelCase"` does.
fn camel(name: &str) -> String {
    let name = name.strip_prefix("r#").unwrap_or(name);
    let mut out = String::new();
    let mut upper = false;
    for c in name.chars() {
        if c == '_' {
            upper = true;
        } else if upper {
            out.extend(c.to_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    out
}

fn is_option(ty: &Type) -> bool {
    matches!(ty, Type::Path(p) if p.qself.is_none()
        && p.path.segments.last().is_some_and(|s| s.ident == "Option"))
}

fn allowed(kind: Kind, args: &Args) -> Result<(), Error> {
    let span = Span::call_site();
    let only = |present: bool, what: &str, kinds: &str| {
        if present {
            Err(Error::new(span, format!("{what} is for {kinds} only")))
        } else {
            Ok(())
        }
    };
    only(
        kind != Kind::Event
            && (args.version.is_some() || args.upcasts.is_some() || args.erases.is_some()),
        "version, upcasts and erases",
        "events",
    )?;
    only(
        kind != Kind::Command && (args.aggregate.is_some() || !args.secret.is_empty()),
        "aggregate and secret",
        "commands",
    )?;
    only(
        kind == Kind::Query && !args.personal.is_empty(),
        "personal",
        "events and commands",
    )?;
    only(
        kind != Kind::Query && args.answer.is_some(),
        "answer",
        "queries",
    )?;
    if kind == Kind::Command && args.aggregate.is_none() {
        return Err(Error::new(
            span,
            "a command names its aggregate: aggregate = \"tenant\"",
        ));
    }
    Ok(())
}

fn expand(kind: Kind, args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as Args);
    let mut item = parse_macro_input!(item as ItemStruct);
    match generate(kind, &args, &mut item) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn generate(kind: Kind, args: &Args, item: &mut ItemStruct) -> syn::Result<Tokens> {
    allowed(kind, args)?;
    if !item.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &item.generics,
            "a message has no generic parameters",
        ));
    }
    let Fields::Named(fields) = &mut item.fields else {
        return Err(Error::new_spanned(
            &item.ident,
            "a message is a braced struct (`struct Done {}` when it has no fields): its name is also the constant that stands for its type",
        ));
    };
    let vault: Vec<&Ident> = args
        .secret
        .iter()
        .chain(args.personal.iter().flat_map(|p| &p.fields))
        .collect();
    for name in &vault {
        if !fields.named.iter().any(|f| f.ident.as_ref() == Some(name)) {
            return Err(Error::new(
                name.span(),
                format!("{} has no field {name}", item.ident),
            ));
        }
    }
    for field in &mut fields.named {
        let name = field.ident.as_ref().expect("named");
        if field.attrs.iter().any(|a| a.path().is_ident("serde")) {
            // The field says itself how it is read and written.
            continue;
        }
        if is_option(&field.ty) {
            // In an event, `None` is part of the record (`"ip": null`); in a command or query,
            // an unset option is left out. Either reads a missing field as `None`.
            if kind == Kind::Event {
                field.attrs.push(syn::parse_quote!(#[serde(default)]));
            } else {
                field.attrs.push(
                    syn::parse_quote!(#[serde(default, skip_serializing_if = "Option::is_none")]),
                );
            }
        } else if vault.contains(&name) {
            // An erased vault value comes back as null: the field's default then.
            field.attrs.push(syn::parse_quote!(#[serde(default, deserialize_with = "::kernable::__private::null_as_default")]));
        }
    }
    let ident = &item.ident;
    let vis = &item.vis;
    let domain = args.domain.as_ref().expect("parsed first");
    let type_name = args
        .type_name
        .clone()
        .unwrap_or_else(|| LitStr::new(&ident.to_string(), ident.span()));
    let personal = args.personal.iter().map(|p| {
        let names: Vec<String> = p.fields.iter().map(|f| camel(&f.to_string())).collect();
        let subject = &p.subject;
        quote! { let schema = schema.personal(&[#(#names),*], #subject); }
    });
    let declare = match kind {
        Kind::Event => {
            let version = args
                .version
                .clone()
                .unwrap_or_else(|| LitInt::new("1", Span::call_site()));
            let erases = args
                .erases
                .iter()
                .map(|f| quote! { let schema = schema.erases(#f); });
            let upcasts = args
                .upcasts
                .iter()
                .map(|f| quote! { let schema = #f(schema); });
            quote! {
                let schema = domain.event(<Self as ::kernable::Message>::TYPE, #version);
                #(#personal)*
                #(#erases)*
                #(#upcasts)*
                let _ = schema;
            }
        }
        Kind::Command => {
            let secret: Vec<String> = args.secret.iter().map(|f| camel(&f.to_string())).collect();
            let secret = (!secret.is_empty())
                .then(|| quote! { let schema = schema.secret(&[#(#secret),*]); });
            quote! {
                let schema = domain.command(<Self as ::kernable::Message>::TYPE);
                #(#personal)*
                #secret
                let _ = schema;
            }
        }
        Kind::Query => quote! { let _ = domain; },
    };
    let kind_impl = match kind {
        Kind::Event => quote! { impl ::kernable::EventType for #ident {} },
        Kind::Command => {
            let aggregate = &args.aggregate;
            quote! { impl ::kernable::CommandType for #ident { const AGGREGATE: &'static str = #aggregate; } }
        }
        Kind::Query => {
            let answer = args
                .answer
                .clone()
                .unwrap_or_else(|| syn::parse_quote!(::kernable::__private::serde_json::Value));
            quote! { impl ::kernable::QueryType for #ident { type Answer = #answer; } }
        }
    };
    let constant = format_ident!("{}", ident);
    Ok(quote! {
        #[derive(::kernable::__private::serde::Serialize, ::kernable::__private::serde::Deserialize)]
        #[serde(crate = "::kernable::__private::serde", rename_all = "camelCase")]
        #item

        /// Stands for the type where the SDK takes one, e.g. `aggregate.apply(ThisType, ...)`.
        #[allow(non_upper_case_globals)]
        #vis const #constant: ::kernable::Of<#ident> = ::kernable::Of::new();

        impl ::kernable::Message for #ident {
            const DOMAIN: &'static str = #domain;
            const TYPE: &'static str = #type_name;
            fn declare(domain: &::kernable::Domain) {
                #declare
            }
        }

        #kind_impl
    })
}
