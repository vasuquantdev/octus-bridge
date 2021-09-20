pub mod serde_url {
    use std::str::FromStr;

    use http::uri::PathAndQuery;
    use serde::de::Error;
    use serde::Deserialize;

    pub fn serialize<S>(data: &PathAndQuery, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(data.as_str())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<PathAndQuery, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = String::deserialize(deserializer)?;
        let data = match data.as_bytes().first() {
            None => "/".to_owned(),
            Some(b'/') => data,
            Some(_) => format!("/{}", data),
        };
        PathAndQuery::from_str(&data).map_err(D::Error::custom)
    }
}
