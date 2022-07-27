#[derive(Clone)]
pub struct Ident {
    value: String,
    namespace_len: usize,
}

impl Ident {
    fn parse_parts(value: &str) -> Result<(&str, &str), InvalidIdentError> {
        let s = value;
        let (ns, name) = s.split_once('/').ok_or_else(|| {
            InvalidIdentError::new(s, "missing namespace separator '/'".to_string())
        })?;

        if ns.is_empty() {
            return Err(InvalidIdentError::new(s, "namespace is empty"));
        }
        if !is_valid_name(ns) {
            return Err(InvalidIdentError::new(
                s,
                "namespace contains invalid characters (allowed: [a-zA-Z0-9._])",
            ));
        }
        if name.is_empty() {
            return Err(InvalidIdentError::new(s, "namespace is empty"));
        }
        if !is_valid_name(name) {
            return Err(InvalidIdentError::new(
                s,
                "name contains invalid characters (allowed: [a-zA-Z0-9._])",
            ));
        }

        Ok((ns, name))
    }

    pub fn validate(value: &str) -> Result<(), InvalidIdentError> {
        Self::parse_parts(value)?;
        Ok(())
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidIdentError> {
        let value = value.into();
        let (ns, _name) = Self::parse_parts(&value)?;
        Ok(Self {
            namespace_len: ns.len(),
            value,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.value[0..self.namespace_len]
    }

    pub fn name(&self) -> &str {
        &self.value[self.namespace_len + 1..]
    }
}

impl std::hash::Hash for Ident {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl std::fmt::Debug for Ident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Ident").field(&self.value).finish()
    }
}

impl std::fmt::Display for Ident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl PartialEq for Ident {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for Ident {}

impl PartialOrd for Ident {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ident {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value.cmp(&other.value)
    }
}

impl AsRef<str> for Ident {
    fn as_ref(&self) -> &str {
        &self.value
    }
}

#[derive(Debug)]
pub struct InvalidIdentError {
    value: String,
    message: String,
}

impl InvalidIdentError {
    fn new(value: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for InvalidIdentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Invalid ident '{}': {}", self.value, self.message)
    }
}

impl std::error::Error for InvalidIdentError {}

fn is_valid_name(s: &str) -> bool {
    s.chars()
        .all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_'))
}

impl std::str::FromStr for Ident {
    type Err = InvalidIdentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl serde::Serialize for Ident {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.value)
    }
}

impl<'de> serde::de::Deserialize<'de> for Ident {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Ident;

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ident::parse(v).map_err(|e| E::custom(e.to_string()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ident::parse(v).map_err(|e| E::custom(e.to_string()))
            }

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(
                    formatter,
                    "expted a valid ident string ([NAMESPACE]/[NAME])"
                )
            }
        }

        deserializer.deserialize_string(Visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ident_parse() {
        let a: Ident = "a/b".parse().unwrap();
        assert_eq!(a.namespace(), "a");
        assert_eq!(a.name(), "b");

        let a: Ident = "a_b9.x/alpha09_.".parse().unwrap();
        assert_eq!(a.namespace(), "a_b9.x");
        assert_eq!(a.name(), "alpha09_.");
    }
}
