use serde::ser::{Error, Impossible};

use super::{Value, ValueMap};

macro_rules! tri {
    ($e:expr) => {
        match $e {
            Ok(val) => val,
            Err(err) => return Err(err),
        }
    };
    ($e:expr,) => {
        tri!($e)
    };
}

#[derive(Debug)]
pub struct ValueSerializeError {
    message: String,
}

impl std::fmt::Display for ValueSerializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ValueSerializeError {}

impl serde::de::Error for ValueSerializeError {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Self {
            message: msg.to_string(),
        }
    }
}

impl serde::ser::Error for ValueSerializeError {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Self {
            message: msg.to_string(),
        }
    }
}

impl serde::Serialize for Value {
    #[inline]
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match *self {
            Value::Unit => s.serialize_unit(),
            Value::Bool(v) => s.serialize_bool(v),
            Value::UInt(v) => s.serialize_u64(v),
            Value::Int(v) => s.serialize_i64(v),
            Value::Float(v) => s.serialize_f64(v.into_inner()),
            Value::String(ref v) => s.serialize_str(&v),
            Value::Bytes(ref v) => s.serialize_bytes(v.as_slice()),
            Value::List(ref v) => v.serialize(s),
            Value::Map(ref v) => v.serialize(s),
            Value::Id(v) => v.serialize(s),
        }
    }
}

pub fn to_value<T: serde::Serialize>(value: T) -> Result<Value, ValueSerializeError> {
    value.serialize(ValueSerializer)
}

/// Serializer that converts and T: Serialize into a Value.
struct ValueSerializer;

impl serde::Serializer for ValueSerializer {
    type Ok = Value;
    type Error = ValueSerializeError;
    type SerializeSeq = SerializeSeq;
    type SerializeTuple = SerializeTuple;
    type SerializeTupleStruct = SerializeTupleStruct;
    type SerializeTupleVariant = SerializeTupleVariant;
    type SerializeMap = SerializeMap<Value>;
    type SerializeStruct = SerializeStruct;
    type SerializeStructVariant = SerializeStructVariant;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Bool(v))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Int(v.into()))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Int(v.into()))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Int(v.into()))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Int(v))
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(Value::UInt(v.into()))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(Value::UInt(v.into()))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::UInt(v.into()))
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::UInt(v.into()))
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Float((v as f64).into()))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Float(v.into()))
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(Value::String(v.into()))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(Value::String(v.to_string()))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Bytes(v.to_vec()))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Unit)
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(ValueSerializer)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Unit)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Unit)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(Value::String(variant.to_string()))
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(ValueSerializer)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(ValueSerializer).map(|v| {
            let mut map = ValueMap::new();
            map.insert(Value::String(variant.to_string()), v);
            Value::Map(map)
        })
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(SerializeSeq(Vec::with_capacity(len.unwrap_or_default())))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(SerializeTuple(Vec::with_capacity(len)))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(SerializeTupleStruct(Vec::with_capacity(len)))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(SerializeTupleVariant(
            Value::String(variant.to_string()),
            Vec::with_capacity(len),
        ))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(SerializeMap {
            map: ValueMap::new(),
            key: None,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(SerializeStruct(ValueMap::new()))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(SerializeStructVariant(
            Value::String(variant.to_string()),
            ValueMap::new(),
        ))
    }
}

struct SerializeSeq(Vec<Value>);

impl serde::ser::SerializeSeq for SerializeSeq {
    type Ok = Value;
    type Error = ValueSerializeError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let value = tri!(value.serialize(ValueSerializer));
        self.0.push(value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::List(self.0))
    }
}

struct SerializeTuple(Vec<Value>);

impl serde::ser::SerializeTuple for SerializeTuple {
    type Ok = Value;
    type Error = ValueSerializeError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let value = tri!(value.serialize(ValueSerializer));
        self.0.push(value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::List(self.0))
    }
}

struct SerializeTupleStruct(Vec<Value>);

impl serde::ser::SerializeTupleStruct for SerializeTupleStruct {
    type Ok = Value;
    type Error = ValueSerializeError;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let value = tri!(value.serialize(ValueSerializer));
        self.0.push(value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::List(self.0))
    }
}

struct SerializeTupleVariant(Value, Vec<Value>);

impl serde::ser::SerializeTupleVariant for SerializeTupleVariant {
    type Ok = Value;
    type Error = ValueSerializeError;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let value = tri!(value.serialize(ValueSerializer));
        self.1.push(value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let mut map = ValueMap::new();
        map.insert(self.0, Value::List(self.1));
        Ok(Value::Map(map))
    }
}

struct SerializeMap<K> {
    map: ValueMap<K>,
    key: Option<K>,
}

impl serde::ser::SerializeMap for SerializeMap<Value> {
    type Ok = Value;
    type Error = ValueSerializeError;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let key = tri!(key.serialize(ValueSerializer));
        self.key = Some(key);
        Ok(())
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let value = tri!(value.serialize(ValueSerializer));
        self.map.insert(self.key.take().unwrap(), value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Map(self.map))
    }
}

struct SerializeStruct(ValueMap<Value>);

impl serde::ser::SerializeStruct for SerializeStruct {
    type Ok = Value;
    type Error = ValueSerializeError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let key = Value::String(key.to_string());
        let value = tri!(value.serialize(ValueSerializer));
        self.0.insert(key, value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Map(self.0))
    }
}

struct SerializeStructVariant(Value, ValueMap<Value>);

impl serde::ser::SerializeStructVariant for SerializeStructVariant {
    type Ok = Value;
    type Error = ValueSerializeError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let key = Value::String(key.to_string());
        let value = value.serialize(ValueSerializer)?;
        self.1.insert(key, value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let mut map = ValueMap::new();
        map.insert(self.0, Value::Map(self.1));
        Ok(Value::Map(map))
    }
}

struct MapSerializer<K> {
    _key: std::marker::PhantomData<K>,
}

impl<K> MapSerializer<K> {
    fn new() -> Self {
        Self {
            _key: std::marker::PhantomData,
        }
    }
}

struct MapSerializeStruct<K> {
    map: ValueMap<K>,
}

impl<K: Ord + From<String>> serde::ser::SerializeStruct for MapSerializeStruct<K> {
    type Ok = ValueMap<K>;
    type Error = ValueSerializeError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let value = tri!(value.serialize(ValueSerializer));
        self.map.insert(key.to_string().into(), value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.map)
    }
}

impl<K: Ord + From<String>> serde::Serializer for MapSerializer<K> {
    type Ok = ValueMap<K>;
    type Error = ValueSerializeError;
    type SerializeSeq = Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = MapSerializeStruct<K>;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_some<T: ?Sized>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(MapSerializeStruct {
            map: ValueMap::new(),
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(ValueSerializeError::custom(
            "expected a key => value structure",
        ))
    }
}

pub fn to_value_map<K, T>(value: T) -> Result<ValueMap<K>, ValueSerializeError>
where
    K: Ord + From<String>,
    T: serde::Serialize,
{
    value.serialize(MapSerializer::<K>::new())
}
