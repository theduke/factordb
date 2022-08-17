use std::collections::BTreeMap;

use super::Value;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueMap<K>(pub BTreeMap<K, Value>);

#[cfg(feature = "typescript-schema")]
impl<K: 'static> ts_rs::TS for ValueMap<K> {
    fn name() -> String {
        "Record<string, any>".to_string()
    }

    fn name_with_type_args(args: Vec<String>) -> String {
        assert!(args.is_empty(), "called name_with_type_args on primitive");
        "Record<string, any>".to_string()
    }

    fn inline() -> String {
        "Record<string, any>".to_string()
    }

    fn dependencies() -> Vec<ts_rs::Dependency> {
        vec![]
    }

    fn transparent() -> bool {
        false
    }
}

#[cfg(feature = "jsonschema")]
impl<K> schemars::JsonSchema for ValueMap<K> {
    fn is_referenceable() -> bool {
        false
    }

    fn schema_name() -> String {
        "float".to_owned()
    }

    fn json_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::Object.into()),
            format: Some("object".to_owned()),
            ..Default::default()
        }
        .into()
    }
}

impl<K: Ord> Default for ValueMap<K> {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

impl<K> ValueMap<K>
where
    K: Ord,
{
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn with_insert(mut self, key: impl Into<K>, value: impl Into<Value>) -> Self {
        self.insert(key.into(), value.into());
        self
    }
}

impl<K> ValueMap<K> {
    pub fn into_inner(self) -> BTreeMap<K, Value> {
        self.0
    }
}

impl<K> std::ops::Deref for ValueMap<K> {
    type Target = BTreeMap<K, Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K> std::ops::DerefMut for ValueMap<K> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K: Ord> std::iter::FromIterator<(K, Value)> for ValueMap<K> {
    fn from_iter<T: IntoIterator<Item = (K, Value)>>(iter: T) -> Self {
        ValueMap(BTreeMap::from_iter(iter))
    }
}

impl<K> From<BTreeMap<K, Value>> for ValueMap<K> {
    fn from(m: BTreeMap<K, Value>) -> Self {
        Self(m)
    }
}

impl<K: serde::Serialize + Ord> serde::Serialize for ValueMap<K> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de, K: serde::Deserialize<'de> + Ord> serde::Deserialize<'de> for ValueMap<K> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let inner = BTreeMap::deserialize(deserializer)?;
        Ok(Self(inner))
    }
}
