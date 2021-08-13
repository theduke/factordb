use inflector::Inflector;
use proc_macro::TokenStream;
use quote::quote;

struct StructAttrs {
    namespace: String,
    name: Option<String>,
    title: Option<String>,
}

const STRUCT_USAGE: &'static str =
    "Invalid #[factor(...)) macro key: expected #[factor(namespace= \"my.namespace\")]";

impl syn::parse::Parse for StructAttrs {
    fn parse(outer: syn::parse::ParseStream) -> syn::Result<Self> {
        let input;
        syn::parenthesized!(input in outer);

        let mut namespace = None;
        let mut name: Option<String> = None;
        let mut title: Option<String> = None;

        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;
            let _eq: syn::token::Eq = input.parse()?;

            match key.to_string().as_str() {
                "namespace" => {
                    let v = input.parse::<syn::LitStr>()?;
                    namespace = Some(v.value());
                }
                "name" => {
                    let s = input.parse::<syn::LitStr>()?;
                    name = Some(s.value());
                }
                "title" => {
                    let s = input.parse::<syn::LitStr>()?;
                    title = Some(s.value());
                }
                _other => Err(input.error(STRUCT_USAGE))?,
            }

            if !input.is_empty() {
                input.parse::<syn::token::Comma>()?;
            }
        }

        Ok(StructAttrs {
            namespace: namespace.expect(STRUCT_USAGE),
            name,
            title,
        })
    }
}

struct FieldAttrs {
    attribute: Option<syn::Path>,
    extend: bool,
    is_relation: bool,
    ignored: bool,
    // relation: Option<syn::Path>,
}

const FIELD_USAGE: &'static str =
    "Invalid #[factor()] macro key: expected #[factor(attr = Attribute)]";

impl syn::parse::Parse for FieldAttrs {
    fn parse(outer: syn::parse::ParseStream) -> syn::Result<Self> {
        let input;
        syn::parenthesized!(input in outer);

        let mut attrs = FieldAttrs {
            attribute: None,
            extend: false,
            is_relation: false,
            ignored: false,
        };
        // let mut relation = None;

        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;

            match key.to_string().as_str() {
                "attr" => {
                    let _eq: syn::token::Eq = input.parse()?;

                    let v = input.parse()?;
                    attrs.attribute = Some(v);
                }
                "relation" => {
                    attrs.is_relation = true;

                    // if input.peek(syn::token::Eq) {
                    //     let _eq: syn::token::Eq = input.parse()?;

                    //     let v = input.parse()?;
                    //     relation = Some(v);
                    // }
                }
                "extend" => {
                    attrs.extend = true;
                }
                "ignore" => {
                    attrs.ignored = true;
                }
                _other => Err(input.error(FIELD_USAGE))?,
            }

            if !input.is_empty() {
                input.parse::<syn::token::Comma>()?;
            }
        }

        if !attrs.ignored {
            if attrs.attribute.is_none() && !(attrs.extend || attrs.is_relation) {
                return Err(input
                    .error("Must either specify or #[factor(attr = AttrType)] #[factor(extend)]"));
            }
        }

        Ok(attrs)
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

// fn option_inner(ty: &syn::Type) -> Option<&syn::Type> {
//     let path = match ty {
//         syn::Type::Path(path) => path,
//         _ => return None,
//     };

//     let segment = path.path.segments.first()?;
//     if segment.ident != "Option" {
//         return None;
//     }

//     let arg = match &segment.arguments {
//         syn::PathArguments::AngleBracketed(syn::AngleBracketedGenericArguments {
//             args, ..
//         }) => args.first(),
//         _ => None,
//     }?;

//     match arg {
//         syn::GenericArgument::Type(t) => Some(t),
//         _ => None,
//     }
// }

fn is_vec(ty: &syn::Type) -> bool {
    match ty {
        syn::Type::Path(path) => path
            .path
            .segments
            .first()
            .map(|x| x.ident == "Vec")
            .unwrap_or(false),
        _ => false,
    }
}

// fn vec_inner(ty: &syn::Type) -> Option<&syn::Type> {
//     let path = match ty {
//         syn::Type::Path(path) => path,
//         _ => return None,
//     };

//     let segment = path.path.segments.first()?;
//     if segment.ident != "Vec" {
//         return None;
//     }

//     let arg = match &segment.arguments {
//         syn::PathArguments::AngleBracketed(syn::AngleBracketedGenericArguments {
//             args, ..
//         }) => args.first(),
//         _ => None,
//     }?;

//     match arg {
//         syn::GenericArgument::Type(t) => Some(t),
//         _ => None,
//     }
// }

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
    let entity_name = struct_attrs.name.unwrap_or_else(|| input.ident.to_string());
    let title = struct_attrs
        .title
        .unwrap_or_else(|| entity_name.to_title_case());

    let struct_ident = &input.ident;

    let field_count = fields.named.len();
    let mut schema_attributes = Vec::with_capacity(field_count);
    let mut schema_extends: Vec<proc_macro2::TokenStream> = Vec::new();

    let mut serialize_fields = Vec::<proc_macro2::TokenStream>::new();
    // let mut deserialize_fields = Vec::<proc_macro2::TokenStream>::new();

    // let mut fields_to_relations = Vec::new();

    // State to determine which field to use for id accessor.
    let mut have_id = false;
    // The field name of the extended parent entity.
    let mut extends_field: Option<syn::Ident> = None;

    for field in &fields.named {
        let field_attrs_raw = super::find_factor_attr(&field.attrs)
            .expect("Could not find #[factor(...) attribute on field");

        let field_attrs: FieldAttrs = syn::parse(field_attrs_raw.tokens.clone().into()).unwrap();

        if field_attrs.ignored {
            continue;
        }

        let field_name = &field.ident.as_ref().expect("Only named fields are allowed");
        let field_ty = &field.ty;
        // let field_name_str = field_name.to_string();

        if field_attrs.extend {
            if extends_field.is_some() {
                panic!("#[derive(Entity)] does not support multiple extended parents");
            }
            extends_field = Some((*field_name).clone());
            schema_extends.push(quote! {
                <#field_ty as factordb::schema::EntityDescriptor>::IDENT.into(),
            });
        } else if field_attrs.is_relation {
            todo!()
            // if let Some(_inner_ty) = option_inner(&field.ty) {
            //     // Option signifies a to-one relation.
            // } else if let Some(_inner_ty) = vec_inner(&field.ty) {
            //     // Vec signifies a to-many relation.
            // } else {
            //     panic!(
            //         "Invalid field {}: relation fields must be Option<T> or Vec<T>",
            //         field_name
            //     );
            // }
        } else {
            let prop = &field_attrs.attribute.unwrap();

            let cardinality = match &field.ty {
                x if is_option(x) => quote!(Optional),
                x if is_vec(x) => quote!(Many),
                _ => quote!(Required),
            };

            if field_name.to_string() == "id" {
                have_id = true;
            } else {
                schema_attributes.push(quote! {
                    factordb::schema::EntityAttribute {
                        attribute: <#prop as factordb::schema::AttributeDescriptor>::IDENT,
                        cardinality: factordb::schema::Cardinality::#cardinality,
                    },
                });

                serialize_fields.push(quote! {
                    map.serialize_entry(
                        <#prop as factordb::schema::AttributeDescriptor>::QUALIFIED_NAME,
                        &self.#field_name,
                    )?;
                });
            }
        }
    }

    let id_accessor = if have_id {
        quote! { &self.id }
    } else if let Some(field) = extends_field {
        quote! {
            &self.#field.id
        }
    } else {
        panic!("#[derive(Entity)] requires an id field with type factor::Id");
    };

    let full_name = format!("{}/{}", namespace, entity_name);

    TokenStream::from(quote! {
        impl factordb::schema::EntityDescriptor for #struct_ident {
            const NAMESPACE: &'static str = #namespace;
            const PLAIN_NAME: &'static str = #entity_name;
            const QUALIFIED_NAME: &'static str = #full_name;
            const IDENT: factordb::data::Ident = factordb::data::Ident::new_static(Self::QUALIFIED_NAME);

            fn schema() -> factordb::schema::EntitySchema {
                factordb::schema::EntitySchema{
                    id: factordb::data::Id::nil(),
                    ident: #full_name.to_string(),
                    title: Some(#title.to_string()),
                    description: None,
                    attributes: vec![
                        #( #schema_attributes )*
                    ],
                    extends: vec![
                        #( #schema_extends )*
                    ],
                    strict: false,
                }
            }
        }

        impl factordb::schema::EntityContainer for #struct_ident {
            fn id(&self) -> factordb::data::Id {
                *#id_accessor
            }

            fn entity_type(&self) -> factordb::data::Ident {
                <Self as factordb::schema::EntityDescriptor>::IDENT
            }
        }

        // impl serde::Serialize for #struct_ident {
        //     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        //     where
        //         S: serde::ser::Serializer,
        //     {
        //         // TODO: use serialize_struct if no parents extended.
        //         use serde::ser::SerializeMap;
        //         let mut map = serializer.serialize_map(Some(#field_count))?;
        //         #( #serialize_fields )*
        //         map.end()
        //     }
        // }

    })
}
