extern crate proc_macro;

use proc_macro::TokenStream;

mod attribute;
mod class;

/// Find an attribute with the format `#[factor(...)]`.
fn find_factor_attr(attrs: &[syn::Attribute]) -> Option<&syn::Attribute> {
    attrs.iter().find(|attr| attr.path.is_ident("factor"))
}

#[proc_macro_derive(Attribute, attributes(factor))]
pub fn derive_attribute(tokens: TokenStream) -> TokenStream {
    attribute::derive_attribute(tokens)
}

#[proc_macro_derive(Class, attributes(factor))]
pub fn derive_class(tokens: TokenStream) -> TokenStream {
    class::derive_class(tokens)
}

// TODO: write an Object derive.

// #[proc_macro_derive(Object, attributes(factor))]
// pub fn derive_object(tokens: TokenStream) -> TokenStream {
//     object::derive_object(tokens)
// }
