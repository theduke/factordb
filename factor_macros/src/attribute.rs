use inflector::Inflector;
use proc_macro::TokenStream;
use quote::quote;

struct StructAttrs {
    namespace: syn::LitStr,
    // value_type: Option<syn::Path>,
    name: Option<String>,
    title: Option<String>,
    unique: bool,
    index: bool,
    strict: bool,
}

const PROPERTY_USAGE: &'static str =
    "Invalid macro attribute. Expected #[factor(namespace = \"my.namespace\", value = TYPE [, name = \"theName\"])]";

impl syn::parse::Parse for StructAttrs {
    fn parse(outer: syn::parse::ParseStream) -> syn::Result<Self> {
        let input;
        syn::parenthesized!(input in outer);

        // let mut value_type = None;
        let mut namespace = None;
        let mut name: Option<String> = None;
        let mut title: Option<String> = None;
        let mut unique = false;
        let mut index = false;
        let mut strict = false;

        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;

            match key.to_string().as_str() {
                // "value" => {
                //     let _eq: syn::token::Eq = input.parse()?;
                //     let v = input.parse::<syn::Path>()?;
                //     value_type = Some(v);
                // }
                "namespace" => {
                    let _eq: syn::token::Eq = input.parse()?;
                    let v = input.parse::<syn::LitStr>()?;
                    namespace = Some(v);
                }
                "name" => {
                    let _eq: syn::token::Eq = input.parse()?;
                    let s = input.parse::<syn::LitStr>()?;
                    name = Some(s.value());
                }
                "title" => {
                    let _eq: syn::token::Eq = input.parse()?;
                    let s = input.parse::<syn::LitStr>()?;
                    title = Some(s.value());
                }
                "unique" => {
                    unique = true;
                }
                "index" => {
                    index = true;
                }
                "strict" => {
                    strict = true;
                }
                _other => Err(input.error(PROPERTY_USAGE))?,
            }

            if !input.is_empty() {
                input.parse::<syn::token::Comma>()?;
            }
        }

        Ok(Self {
            // value_type,
            namespace: namespace.expect(PROPERTY_USAGE),
            name,
            title,
            unique,
            index,
            strict,
        })
    }
}

pub fn derive_attribute(tokens: TokenStream) -> TokenStream {
    let input: syn::DeriveInput = syn::parse(tokens).unwrap();

    let attr_raw =
        super::find_factor_attr(&input.attrs).expect("Could not find #[factor(...)] attribute");
    let attr: StructAttrs = syn::parse(attr_raw.tokens.clone().into()).expect(PROPERTY_USAGE);

    // Build output.

    let ident = &input.ident;
    let type_ = match &input.data {
        syn::Data::Struct(syn::DataStruct { fields, .. }) => match fields {
            syn::Fields::Unnamed(syn::FieldsUnnamed { unnamed, .. }) if unnamed.len() == 1 => {
                let ty = unnamed.first().unwrap();
                quote! {
                    #ty
                }
            }
            _ => {
                panic!("#[derive(Attribute)] must be used on tuple structs with a single inner type. eg: struct MyAttr(String);");
            }
        },
        _ => {
            panic!("#[derive(Attribute)] must be used on tuple structs with a single inner type. eg: struct MyAttr(String);");
        }
    };
    let namespace = attr.namespace;
    let name = attr.name.unwrap_or_else(|| {
        let raw = input.ident.to_string();
        let snake = raw.to_snake_case();
        let name = if snake.starts_with("attr_") {
            snake.strip_prefix("attr_").unwrap().to_string()
        } else {
            snake
        };
        name
    });
    let title = match attr.title {
        Some(x) => quote!( Some(#x.to_string()) ),
        None => quote!(None),
    };
    let unique = attr.unique;
    let index = attr.index;
    let strict = attr.strict;

    let full_name = format!("{}/{}", namespace.value(), name);

    let out = quote! {
        impl factordb::schema::AttributeDescriptor for #ident {
            const NAME: &'static str = #full_name;
            const IDENT: factordb::data::Ident = factordb::data::Ident::new_static(Self::NAME);
            type Type = #type_;

            fn schema() -> factordb::schema::AttributeSchema {
                factordb::schema::AttributeSchema {
                    id: factordb::data::Id::nil(),
                    name: #full_name.into(),
                    title: #title,
                    description: None,
                    value_type: <Self::Type as factordb::data::value::ValueTypeDescriptor>::value_type(),
                    index: #index,
                    unique: #unique,
                    strict: #strict,
                }
            }
        }
    };
    TokenStream::from(out)
}
