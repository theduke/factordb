use std::collections::btree_map;

use anyhow::bail;

use crate::{data::DataMap, AnyError};

use super::Value;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Patch(pub Vec<PatchOp>);

impl Patch {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn add(mut self, path: impl Into<PatchPath>, value: impl Into<Value>) -> Self {
        self.0.push(PatchOp::Add {
            path: path.into(),
            value: value.into(),
        });
        self
    }

    pub fn replace(mut self, path: impl Into<PatchPath>, new_value: impl Into<Value>) -> Self {
        self.0.push(PatchOp::Replace {
            path: path.into(),
            new_value: new_value.into(),
            current_value: None,
        });
        self
    }

    pub fn replace_with_old(
        mut self,
        path: impl Into<PatchPath>,
        new_value: impl Into<Value>,
        old_value: impl Into<Value>,
    ) -> Self {
        self.0.push(PatchOp::Replace {
            path: path.into(),
            new_value: new_value.into(),
            current_value: Some(old_value.into()),
        });
        self
    }

    pub fn remove(mut self, path: impl Into<PatchPath>) -> Self {
        self.0.push(PatchOp::Remove {
            path: path.into(),
            value: None,
        });
        self
    }

    pub fn remove_with_old(
        mut self,
        path: impl Into<PatchPath>,
        old_value: impl Into<Value>,
    ) -> Self {
        self.0.push(PatchOp::Remove {
            path: path.into(),
            value: Some(old_value.into()),
        });
        self
    }

    pub fn op(mut self, op: PatchOp) -> Self {
        self.0.push(op);
        self
    }

    pub fn apply_map(self, mut target: DataMap) -> Result<DataMap, AnyError> {
        for op in self.0 {
            op.apply_map(&mut target)?;
        }
        Ok(target)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum PatchOp {
    Add {
        path: PatchPath,
        value: Value,
    },
    Replace {
        path: PatchPath,
        new_value: Value,
        current_value: Option<Value>,
    },
    Remove {
        path: PatchPath,
        value: Option<Value>,
    },
    // Move { new_path: PatchPath },
    // Copy { new_path: PatchPath },
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PatchPath(pub Vec<PatchPathElem>);

impl From<String> for PatchPath {
    fn from(v: String) -> Self {
        Self(vec![PatchPathElem::Key(v)])
    }
}

impl<'a> From<&'a String> for PatchPath {
    fn from(v: &'a String) -> Self {
        Self(vec![PatchPathElem::Key(v.clone())])
    }
}

impl<'a> From<&'a str> for PatchPath {
    fn from(v: &'a str) -> Self {
        Self(vec![PatchPathElem::Key(v.to_string())])
    }
}

impl From<String> for PatchPathElem {
    fn from(v: String) -> Self {
        PatchPathElem::Key(v)
    }
}

impl<'a> From<&'a String> for PatchPathElem {
    fn from(v: &'a String) -> Self {
        PatchPathElem::Key(v.clone())
    }
}

impl<'a> From<&'a str> for PatchPathElem {
    fn from(v: &'a str) -> Self {
        PatchPathElem::Key(v.to_string())
    }
}

impl<E> From<Vec<E>> for PatchPath
where
    E: Into<PatchPathElem>,
{
    fn from(v: Vec<E>) -> Self {
        Self(v.into_iter().map(|e| e.into()).collect())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum PatchPathElem {
    Key(String),
    ListIndex(usize),
}

impl PatchPath {
    fn render(&self) -> String {
        let mut s = String::new();
        for elem in &self.0 {
            s.push('/');
            match elem {
                PatchPathElem::Key(key) => {
                    s.push_str(key);
                }
                PatchPathElem::ListIndex(index) => {
                    s.push_str(&index.to_string());
                }
            }
        }
        s
    }
}

impl std::fmt::Display for PatchPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.render())
    }
}

impl PatchOp {
    fn apply_map(self, target: &mut DataMap) -> Result<(), AnyError> {
        match self {
            PatchOp::Add { path, value } => match path.0.as_slice() {
                [] => {
                    bail!("Invalid empty path");
                }
                [PatchPathElem::ListIndex(_), ..] => {
                    bail!("Invalid list index into map");
                }
                [PatchPathElem::Key(key)] => match target.get_mut(key) {
                    None => {
                        target.insert(key.to_string(), value);
                        Ok(())
                    }
                    Some(u @ Value::Unit) => {
                        *u = value;
                        Ok(())
                    }
                    Some(Value::List(items)) => {
                        if !items.contains(&value) {
                            items.push(value);
                        }
                        Ok(())
                    }
                    Some(Value::Map(_)) => {
                        bail!("Tried to add value to a map");
                    }
                    Some(literal) => {
                        *literal = Value::List(vec![literal.clone(), value]);
                        Ok(())
                    }
                },
                [PatchPathElem::Key(_key), _rest @ ..] => {
                    // TODO: implement nesting.
                    todo!("Nested patch not implemented");
                }
            },
            PatchOp::Remove {
                path,
                value: old_value,
            } => match path.0.as_slice() {
                [] => {
                    bail!("Invalid empty path");
                }
                [PatchPathElem::ListIndex(_), ..] => {
                    bail!("Invalid list index into map");
                }
                [PatchPathElem::Key(key)] => {
                    if let Some(old_value) = old_value {
                        match target.entry(key.to_string()) {
                            btree_map::Entry::Vacant(_) => Ok(()),
                            btree_map::Entry::Occupied(mut current_value) => {
                                match current_value.get_mut() {
                                    Value::List(items) => {
                                        items.retain(|v| v != &old_value);
                                        Ok(())
                                    }
                                    other if other == &old_value => {
                                        // Value matches the given old_value, so
                                        // remove the key.
                                        std::mem::drop(other);
                                        current_value.remove();
                                        Ok(())
                                    }
                                    _ => {
                                        // Value does not match the given old_value, so don't remove.
                                        bail!("Could not remove key: specified old value does not match");
                                    }
                                }
                            }
                        }
                    } else {
                        target.remove(key);
                        Ok(())
                    }
                }
                [PatchPathElem::Key(_key), _rest @ ..] => {
                    // TODO: implement nesting.
                    todo!("Nested patch not implemented");
                }
            },
            PatchOp::Replace {
                path,
                new_value,
                current_value: old_value,
            } => match path.0.as_slice() {
                [] => {
                    bail!("Invalid empty path");
                }
                [PatchPathElem::ListIndex(_), ..] => {
                    bail!("Invalid list index into map");
                }
                [PatchPathElem::Key(key)] => {
                    if let Some(old_value) = old_value {
                        match target.entry(key.to_string()) {
                            btree_map::Entry::Vacant(entry) => {
                                entry.insert(new_value);
                                Ok(())
                            }
                            btree_map::Entry::Occupied(mut current_value) => {
                                match current_value.get_mut() {
                                    current if current == &old_value => {
                                        // Value matches the given old_value, so
                                        // replace it.
                                        *current = new_value;
                                        Ok(())
                                    }
                                    _ => {
                                        bail!("Could not replace key: expected value does not match current value");
                                    }
                                }
                            }
                        }
                    } else {
                        target.insert(key.clone(), new_value);
                        Ok(())
                    }
                }
                [PatchPathElem::Key(_key), _rest @ ..] => {
                    // TODO: implement nesting.
                    todo!("Nested patch not implemented");
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::map;

    use super::*;

    #[test]
    fn test_patch() {
        let m = map! {
            "a": 1,
            "b": true,
            "c": vec![1, 2],
            "d": vec![42, 69],
        };
        let out = Patch::new()
            .remove("a")
            .replace("b", false)
            .add("c", 9)
            .add("x", 22)
            .remove_with_old("d", 42)
            .apply_map(m)
            .unwrap();

        assert_eq!(
            out,
            map! {
                "b": false,
                "c": vec![1, 2, 9],
                "d": vec![69],
                "x": 22,
            }
        );
    }
}
