mod id;

pub use id::{Id, Ident};

pub mod value;
pub use value::{Value, ValueType};

mod time;
pub use time::Timestamp;

pub use self::value::ValueMap;

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
