// use anyhow::anyhow;

// use crate::{data::DataMap, AnyError};

// use super::Value;

// #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
// pub struct Patch {
//     pub ops: Vec<PatchItem>,
// }

// #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
// pub enum PatchPathElem {
//     Key(String),
//     ListIndex(usize),
// }

// #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
// pub struct PatchPath {
//     pub elems: Vec<PatchPathElem>,
// }

// impl PatchPath {
//     fn render_elems(elems: &[PatchPathElem]) -> String {
//         let path = elems
//             .iter()
//             .map(|elem| match elem {
//                 PatchPathElem::Key(key) => key.to_string(),
//                 PatchPathElem::ListIndex(index) => index.to_string(),
//             })
//             .collect::<Vec<_>>()
//             .join("/");

//         path.insert(0, '/');
//         path
//     }
// }

// #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
// pub struct PatchItem {
//     pub path: PatchPath,
//     pub op: PatchOp,
// }

// impl PatchItem {
//     fn apply_map(self, map: &mut DataMap) -> Result<(), AnyError> {
//         let mut parents = Vec::new();
//         Self::apply_map_elems(&mut parents, &self.path.elems, self.op, map)
//     }

//     fn apply_map_elems<'a>(
//         parent_path: &mut Vec<PatchPathElem>,
//         elems: &'a [PatchPathElem],
//         op: PatchOp,
//         map: &mut DataMap,
//     ) -> Result<(), AnyError> {
//         match elems {
//             [] => Err(anyhow!("Invalid empty path")),
//             [PatchPathElem::Key(key)] => match op {
//                 PatchOp::Add { value } => {
//                     map.insert(key.clone(), value);
//                     Ok(())
//                 }
//                 PatchOp::Remove => {  
//                     map.remove(key).map(|_x| ()); Ok(()) 
//                 },
//                 PatchOp::Replace { new_value } => {
//                     // TODO: should we error out if old value does not exist?
//                     map.insert(key.clone(), new_value);
//                     Ok(())
//                 }
//                 // PatchOp::Move { new_path } => {
//                 //     let value = map.remove(key).ok_or_else(|| {
//                 //         anyhow::anyhow!(
//                 //             "Can't move key at path '{}': key '{}' not found",
//                 //             PatchPath::render_elems(&parent_path),
//                 //             key
//                 //         )
//                 //     })?;
//                 // }
//                 // PatchOp::Copy { new_path } => todo!(),
//             },
//             [PatchPathElem::ListIndex(_), ..] => {
//                 Err(anyhow!("Invalid path: trying to get list index of a map"))
//             }
//             [PatchPathElem::Key(key), rest @ ..] => {
//                 let nested = map
//                     .get_mut(key)
//                     .ok_or_else(|| anyhow!("Invalid path: key '{}' not found", key))?;
//                 parent_path.push(PatchPathElem::Key(key.clone()));
//                 Self::apply_elems(parent_path, rest, op, nested)
//             }
//         }
//     }

//     fn apply_elems(
//         parent_path: &mut Vec<PatchPathElem>,
//         elems: &[PatchPathElem],
//         op: PatchOp,
//         value: &mut Value,
//     ) -> Result<(), AnyError> {
//         match elems {
//             [] => Err(anyhow!("Invalid empty path")),
//             [PatchPathElem::Key(key)] => {
//                 let map = value.as_map_mut().ok_or_else(|| anyhow!("Can't access key '{}': not a map", key))?;
//                 let value_key: Value = key.to_string().into();

//                 match op {
//                     PatchOp::Add { value } => {
//                         map.insert(value_key, value);
//                         Ok(())
//                     }
//                     PatchOp::Remove => {
//                         map.remove(&value_key);
//                         Ok(())
//                     },
//                     PatchOp::Replace { new_value } => {
//                         map.insert(value_key, new_value);
//                         Ok(())
//                     },
//                 }

//             },
//             [PatchPathElem::Key(key), rest @ ..] => {

//                 let map = value.as_map_mut().ok_or_else(|| anyhow!("Can't access key '{}': not a map", key))?;
//                 let value_key: Value = key.to_string().into();

//                 let nested = map.get_mut(&value_key).ok_or_else(|| anyhow!("Key not found: '{}'", key))?;
//                 parent_path.push(PatchPathElem::Key(key.clone()));
//                 Self::apply_elems(parent_path, rest, op, nested)
//             }
//             [PatchPathElem::ListIndex(_), ..] => {
//                 Err(anyhow!("Invalid path: trying to get list index of a map"))
//             }
//         }
//     }
// }

// #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
// pub enum PatchOp {
//     Add { value: Value },
//     Remove,
//     Replace { new_value: Value },
//     // Move { new_path: PatchPath },
//     // Copy { new_path: PatchPath },
// }
