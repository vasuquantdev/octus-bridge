pub mod exporter;

use std::str::FromStr;
use std::time::Duration;

pub mod optional_serde_time {
    use super::*;

    use serde::{Deserialize, Serialize};

    pub fn serialize<S>(data: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        #[serde(transparent)]
        struct Wrapper<'a>(#[serde(with = "serde_time")] &'a Duration);

        match data {
            Some(duration) => serializer.serialize_some(&Wrapper(duration)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(transparent)]
        struct Wrapper(#[serde(with = "serde_time")] Duration);

        Option::<Wrapper>::deserialize(deserializer).map(|wrapper| wrapper.map(|data| data.0))
    }
}

pub mod serde_time {
    use super::*;

    use serde::de::Error;
    use serde::Deserialize;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum DurationValue {
        Number(u64),
        String(String),
    }

    pub fn serialize<S>(data: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u64(data.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value: DurationValue = serde::Deserialize::deserialize(deserializer)?;
        match value {
            DurationValue::Number(seconds) => Ok(Duration::from_secs(seconds)),
            DurationValue::String(string) => {
                let string = string.trim();

                let seconds = if string.chars().all(|c| c.is_digit(10)) {
                    u64::from_str(string).map_err(D::Error::custom)?
                } else {
                    humantime::Duration::from_str(string)
                        .map_err(D::Error::custom)?
                        .as_secs()
                };

                Ok(Duration::from_secs(seconds))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct TestStruct {
        #[serde(with = "serde_time")]
        interval: Duration,
    }

    #[test]
    fn test_deserialize() {
        let string = r#"interval: 5s"#;
        let object: TestStruct = serde_yaml::from_str(&string).unwrap();
        assert_eq!(object.interval.as_secs(), 5);

        let string = r#"interval: 1m 30s"#;
        let object: TestStruct = serde_yaml::from_str(&string).unwrap();
        assert_eq!(object.interval.as_secs(), 90);

        let string = r#"interval: 123"#;
        let object: TestStruct = serde_yaml::from_str(&string).unwrap();
        assert_eq!(object.interval.as_secs(), 123);
    }

    #[derive(Deserialize)]
    struct OptionalTestStruct {
        test: u32,
        #[serde(default, with = "optional_serde_time")]
        interval: Option<Duration>,
    }

    #[test]
    fn test_deserialize_optional() {
        let string = r#"---
test: 123"#;
        let object: OptionalTestStruct = serde_yaml::from_str(&string).unwrap();
        assert!(object.interval.is_none());

        let string = r#"---
test: 123 
interval:"#;
        let object: OptionalTestStruct = serde_yaml::from_str(&string).unwrap();
        assert!(object.interval.is_none());

        let string = r#"---
test: 123
interval: 1m 30s"#;
        let object: OptionalTestStruct = serde_yaml::from_str(&string).unwrap();
        assert_eq!(object.interval, Some(Duration::from_secs(90)));
    }
}
