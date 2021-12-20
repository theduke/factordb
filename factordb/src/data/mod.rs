mod id;

pub use id::{Id, Ident};

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
    {
        $( $key:literal : $value:expr  ),* $(,)?
    } => {
        {
            #[allow(unused_mut)]
            let mut map = $crate::data::DataMap::new();
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
