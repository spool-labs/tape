//! Column family trait for defining typed columns

use wincode::{SchemaRead, SchemaWrite};

/// Trait for defining a typed column family
///
/// Implement this trait to define a column with typed keys and values.
/// Each column has a unique name (column family) and associated key/value types.
///
/// Key and Value types must implement wincode's `SchemaRead` and `SchemaWrite` traits.
/// Primitive types (u64, String, etc.) implement these traits automatically.
/// For custom structs, implement these traits manually or use derive macros if available.
///
/// # Example
///
/// ```
/// use store::Column;
///
/// // Column with primitive types
/// struct Users;
/// impl Column for Users {
///     const CF_NAME: &'static str = "users";
///     type Key = u64;     // user ID
///     type Value = String; // user data as string
/// }
/// ```
pub trait Column {
    /// Column family name - must be unique across all columns
    const CF_NAME: &'static str;

    /// Key type - must be serializable with wincode
    type Key: for<'de> SchemaRead<'de, Dst = Self::Key> + SchemaWrite<Src = Self::Key>;

    /// Value type - must be serializable with wincode
    type Value: for<'de> SchemaRead<'de, Dst = Self::Value> + SchemaWrite<Src = Self::Value>;
}
