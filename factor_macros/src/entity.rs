// use inflector::Inflector;
use proc_macro::TokenStream;
use quote::quote;

struct StructAttrs {
    namespace: syn::LitStr,
    name: Option<String>,
    // title: Option<String>,
}

const STRUCT_USAGE: &'static str =
    "Invalid #[factor(...)) macro key: expected #[factor(namespace= \"my.namespace\")]";

impl syn::parse::Parse for StructAttrs {
    fn parse(outer: syn::parse::ParseStream) -> syn::Result<Self> {
        let input;
        syn::parenthesized!(input in outer);

        let mut namespace = None;
        let mut name = None;
        // let mut title = None;

        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;
            let _eq: syn::token::Eq = input.parse()?;

            match key.to_string().as_str() {
                "namespace" => {
                    let v = input.parse()?;
                    namespace = Some(v);
                }
                "name" => {
                    let s = input.parse::<syn::LitStr>()?;
                    name = Some(s.value());
                }
                // "title" => {
                //     let s = input.parse::<syn::LitStr>()?;
                //     title = Some(s.value());
                // }
                _other => Err(input.error(STRUCT_USAGE))?,
            }

            if !input.is_empty() {
                input.parse::<syn::token::Comma>()?;
            }
        }

        Ok(StructAttrs {
            namespace: namespace.expect(STRUCT_USAGE),
            name,
            // title,
        })
    }
}

struct FieldAttrs {
    attribute: Option<syn::Path>,
    extend: bool,
    is_relation: bool,
    // relation: Option<syn::Path>,
}

const FIELD_USAGE: &'static str =
    "Invalid #[factor()] macro key: expected #[factor(attr = Attribute)]";

impl syn::parse::Parse for FieldAttrs {
    fn parse(outer: syn::parse::ParseStream) -> syn::Result<Self> {
        let input;
        syn::parenthesized!(input in outer);

        let mut attribute = None;
        let mut extend = false;
        let mut relation = false;
        // let mut relation = None;

        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;

            match key.to_string().as_str() {
                "attr" => {
                    let _eq: syn::token::Eq = input.parse()?;

                    let v = input.parse()?;
                    attribute = Some(v);
                }
                "relation" => {
                    relation = true;

                    // if input.peek(syn::token::Eq) {
                    //     let _eq: syn::token::Eq = input.parse()?;

                    //     let v = input.parse()?;
                    //     relation = Some(v);
                    // }
                }
                "extend" => {
                    extend = true;
                }
                _other => Err(input.error(FIELD_USAGE))?,
            }

            if !input.is_empty() {
                input.parse::<syn::token::Comma>()?;
            }
        }

        if attribute.is_none() && !(extend || relation) {
            return Err(
                input.error("Must either specify or #[factor(attr = AttrType)] #[factor(extend)]")
            );
        }

        Ok(FieldAttrs {
            attribute,
            extend,
            is_relation: relation,
            // relation,
        })
    }
}

fn is_option(ty: &syn::Type) -> bool {
    match ty {
        syn::Type::Path(path) => path
            .path
            .segments
            .first()
            .map(|seg| seg.ident == "Option")
            .unwrap_or(false),
        _ => false,
    }
}

fn option_inner(ty: &syn::Type) -> Option<&syn::Type> {
    let path = match ty {
        syn::Type::Path(path) => path,
        _ => return None,
    };

    let segment = path.path.segments.first()?;
    if segment.ident != "Option" {
        return None;
    }

    let arg = match &segment.arguments {
        syn::PathArguments::AngleBracketed(syn::AngleBracketedGenericArguments {
            args, ..
        }) => args.first(),
        _ => None,
    }?;

    match arg {
        syn::GenericArgument::Type(t) => Some(t),
        _ => None,
    }
}

fn vec_inner(ty: &syn::Type) -> Option<&syn::Type> {
    let path = match ty {
        syn::Type::Path(path) => path,
        _ => return None,
    };

    let segment = path.path.segments.first()?;
    if segment.ident != "Vec" {
        return None;
    }

    let arg = match &segment.arguments {
        syn::PathArguments::AngleBracketed(syn::AngleBracketedGenericArguments {
            args, ..
        }) => args.first(),
        _ => None,
    }?;

    match arg {
        syn::GenericArgument::Type(t) => Some(t),
        _ => None,
    }
}

pub fn derive_entity(tokens: TokenStream) -> TokenStream {
    let input: syn::DeriveInput = syn::parse(tokens).unwrap();

    let struct_body = match &input.data {
        syn::Data::Struct(s) => s,
        _other => {
            panic!("#[derive(Entity)] can only be used on structs");
        }
    };
    let fields = match &struct_body.fields {
        syn::Fields::Named(named) => named,
        _other => {
            panic!("#[derive(Entity)] can only be used on structs with named fields");
        }
    };

    let attr_raw =
        super::find_factor_attr(&input.attrs).expect("Could not find #[factor(...)] attribute");
    let struct_attrs: StructAttrs = syn::parse(attr_raw.tokens.clone().into()).unwrap();

    let namespace = struct_attrs.namespace;
    let name = struct_attrs.name.unwrap_or_else(|| input.ident.to_string());
    // let title = struct_attrs.title.unwrap_or_else(|| name.to_title_case());

    let struct_name = &input.ident;

    let field_count = fields.named.len();
    let mut schema_attributes = Vec::with_capacity(field_count);
    let mut schema_extends = vec![quote!()];

    // let mut fields_to_relations = Vec::new();

    // State to determine which field to use for id accessor.
    let mut have_id = false;
    let mut extends_field: Option<syn::Ident> = None;

    for field in &fields.named {
        let field_attrs_raw = super::find_factor_attr(&field.attrs)
            .expect("Could not find #[factor(...) attribute on field");

        let field_attrs: FieldAttrs = syn::parse(field_attrs_raw.tokens.clone().into()).unwrap();
        let field_name = &field.ident.as_ref().expect("Only named fields are allowed");
        let field_ty = &field.ty;
        // let field_name_str = field_name.to_string();

        if field_attrs.extend {
            extends_field = Some((*field_name).clone());
            schema_extends.push(quote! {
                <#field_ty as factor_core::schema::EntityDescriptor>::TYPE.into(),
            });
        } else if field_attrs.is_relation {
            if let Some(_inner_ty) = option_inner(&field.ty) {
                // Option signifies a to-one relation.
            } else if let Some(_inner_ty) = vec_inner(&field.ty) {
                // Vec signifies a to-many relation.
            } else {
                panic!(
                    "Invalid field {}: relation fields must be Option<T> or Vec<T>",
                    field_name
                );
            }
        } else {
            let prop = &field_attrs.attribute.unwrap();

            let _is_option = is_option(&field.ty);

            if field_name.to_string() != "id" {
                schema_attributes.push(quote! {
                    <#prop as factor_core::schema::AttributeDescriptor>::IDENT,
                });
            } else {
                have_id = true;
            }
        }
    }

    let id_accessor = if have_id {
        quote! { &self.id }
    } else if let Some(_field) = extends_field {
        todo!()
    } else {
        panic!("#[derive(Entity)] requires an id field with type factor::Id");
    };

    let full_name = format!("{}/{}", namespace.value(), name);

    TokenStream::from(quote! {
        impl factor_core::schema::EntityDescriptor for #struct_name {
            const NAME: &'static str = #full_name;
            const IDENT: factor_core::data::Ident = factor_core::data::Ident::new_static(Self::NAME);

            fn schema() -> factor_core::schema::EntitySchema {
                factor_core::schema::EntitySchema{
                    id: factor_core::data::Id::nil(),
                    name: Self::NAME.into(),
                    description: None,
                    attributes: vec![
                        #( #schema_attributes )*
                    ],
                    extend: None,
                    strict: false,
                    is_relation: false,
                    from: None,
                    to: None,
                }
            }
        }

        impl factor_core::schema::EntityContainer for #struct_name {
            fn id(&self) -> factor_core::data::Id {
                *#id_accessor
            }

            fn entity_type(&self) -> factor_core::data::Ident {
                <Self as factor_core::schema::EntityDescriptor>::IDENT
            }
        }

    })
}
