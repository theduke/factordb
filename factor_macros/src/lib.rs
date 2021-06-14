extern crate proc_macro;

use proc_macro::TokenStream;

mod attribute;
mod entity;

/// Find an attribute with the format `#[factor(...)]`.
fn find_factor_attr(attrs: &[syn::Attribute]) -> Option<&syn::Attribute> {
    attrs.iter().find(|attr| attr.path.is_ident("factor"))
}

#[proc_macro_derive(Attribute, attributes(factor))]
pub fn derive_attribute(tokens: TokenStream) -> TokenStream {
    attribute::derive_attribute(tokens)
}

#[proc_macro_derive(Entity, attributes(factor))]
pub fn derive_entity(tokens: TokenStream) -> TokenStream {
    entity::derive_entity(tokens)
}

// #[proc_macro_derive(Object, attributes(factor))]
// pub fn derive_object(tokens: TokenStream) -> TokenStream {
//     object::derive_object(tokens)
// }
