mod id;
pub use id::{Id, IdOrIdent, NilIdError};

mod reference;
pub use self::reference::Ref;

mod ident;
pub use ident::{Ident, InvalidIdentError};

mod map;
pub mod patch;
pub mod value;
pub mod value_type;

pub use self::{
    map::ValueMap,
    value::{from_value, from_value_map, to_value, to_value_map, Value},
    value_type::ValueType,
};

mod time;
pub use time::Timestamp;

pub type DataMap = ValueMap<String>;
pub type IdMap = fnv::FnvHashMap<Id, Value>;

#[macro_export]
macro_rules! map {


    { __map $m:expr, } => {};

    {
        __map $m:expr,
        $key:literal : $value:expr ,
    } => {
        let id = $key.to_string();
        $m.insert(id, $value.into());
    };

    {
        __map $m:expr,
        $key:literal : $value:expr
    } => {
        $m.insert($key.to_string(), $crate::prelude::Value::from($value));
    };

    {
        __map $m:expr,
        $key:literal : $value:expr , $( $rest:tt )*
    } => {
        $m.insert($key.to_string(), $crate::prelude::Value::from($value));
        map!( __map $m, $( $rest )* );
    };

    // With ident.

    {
        __map $m:expr,
        $key:ident : $value:expr ,
    } => {
        $m.insert($key.to_string(), $crate::prelude::Value::from($value));
    };

    {
        __map $m:expr,
        $key:ident : $value:expr
    } => {
        $m.insert($key.to_string(), $crate::prelude::Value::from($value));
    };


    {
        __map $m:expr,
        $key:ident : $value:expr , $( $rest:tt )*
    } => {
        $m.insert($key.to_string(), $crate::prelude::Value::from($value));
        map!( __map $m, $( $rest )* );
    };

    {
        $( $rest:tt )*
    } => {
        {
            #[allow(unused_mut)]
            let mut m = $crate::data::DataMap::new();
            map!( __map m, $( $rest )* );

            m
        }

    };
}

#[macro_export]
macro_rules! tymap {
    (
      $ty:ty,
      {
        $( $key:literal : $value:expr  ),* $(,)?
      }
    ) => {
        {
            let mut map = $ty::new();
            $(
                {
                    let id = $key.to_string();
                    map.insert(id, $value.into());
                }

            )*

            map
        }

    };
}
