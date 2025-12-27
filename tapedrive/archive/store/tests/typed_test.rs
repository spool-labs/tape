//! Integration tests for typed column layer

use store::{Column, TypedStore};
use store_memory::MemoryStore;

// Define column families with primitive types
// For custom structs, implement wincode's SchemaRead and SchemaWrite traits manually

struct Users;
impl Column for Users {
    const CF_NAME: &'static str = "users";
    type Key = u64;
    type Value = String;
}

struct Posts;
impl Column for Posts {
    const CF_NAME: &'static str = "posts";
    type Key = u64;
    type Value = String;
}

struct Comments;
impl Column for Comments {
    const CF_NAME: &'static str = "comments";
    type Key = u64;
    type Value = String;
}

#[test]
fn basic_ops() {
    let store = TypedStore::new(MemoryStore::new());

    // Create a user as encoded string
    let user = "Alice|30|alice@example.com".to_string();

    // Put
    store.put::<Users>(&1, &user).unwrap();

    // Get
    let retrieved = store.get::<Users>(&1).unwrap();
    assert_eq!(retrieved, Some(user.clone()));

    // Contains
    assert!(store.contains::<Users>(&1).unwrap());
    assert!(!store.contains::<Users>(&999).unwrap());

    // Delete
    store.delete::<Users>(&1).unwrap();
    assert_eq!(store.get::<Users>(&1).unwrap(), None);
}

#[test]
fn multi_cf() {
    let store = TypedStore::new(MemoryStore::new());

    // Insert data into different column families
    let user = "Bob|25|bob@example.com".to_string();
    let post = "Hello World|This is my first post|1".to_string();
    let comment = "1|1|Great post!".to_string();

    store.put::<Users>(&1, &user).unwrap();
    store.put::<Posts>(&1, &post).unwrap();
    store.put::<Comments>(&1, &comment).unwrap();

    // Verify all exist in their respective column families
    assert_eq!(store.get::<Users>(&1).unwrap(), Some(user));
    assert_eq!(store.get::<Posts>(&1).unwrap(), Some(post));
    assert_eq!(store.get::<Comments>(&1).unwrap(), Some(comment));
}

#[test]
fn iteration() {
    let store = TypedStore::new(MemoryStore::new());

    // Create multiple users
    let users = vec![
        "Alice|30|alice@example.com".to_string(),
        "Bob|25|bob@example.com".to_string(),
        "Charlie|35|charlie@example.com".to_string(),
    ];

    // Insert users
    for (i, user) in users.iter().enumerate() {
        store.put::<Users>(&((i + 1) as u64), user).unwrap();
    }

    // Iterate and verify
    let results = store.iter::<Users>().unwrap();
    assert_eq!(results.len(), 3);

    // Results should be sorted by key
    for (i, (key, value)) in results.iter().enumerate() {
        assert_eq!(*key, (i + 1) as u64);
        assert_eq!(*value, users[i]);
    }
}

#[test]
fn update_ops() {
    let store = TypedStore::new(MemoryStore::new());

    // Insert initial user
    let user = "Frank|40|frank@example.com".to_string();
    store.put::<Users>(&1, &user).unwrap();

    // Update user
    let updated_user = "Frank|41|frank.new@example.com".to_string();
    store.put::<Users>(&1, &updated_user).unwrap();

    // Verify update
    let retrieved = store.get::<Users>(&1).unwrap();
    assert_eq!(retrieved, Some(updated_user));
}

#[test]
fn empty_iter() {
    let store = TypedStore::new(MemoryStore::new());

    // Iterate over empty column
    let results = store.iter::<Users>().unwrap();
    assert_eq!(results.len(), 0);

    // Add data to one column
    store
        .put::<Posts>(&1, &"Test|Test content|1".to_string())
        .unwrap();

    // Users column should still be empty
    let results = store.iter::<Users>().unwrap();
    assert_eq!(results.len(), 0);

    // Posts column should have one entry
    let results = store.iter::<Posts>().unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn string_keys() {
    // Column with String keys
    struct Settings;
    impl Column for Settings {
        const CF_NAME: &'static str = "settings";
        type Key = String;
        type Value = String;
    }

    let store = TypedStore::new(MemoryStore::new());

    // Store some settings
    store
        .put::<Settings>(&"theme".to_string(), &"dark".to_string())
        .unwrap();
    store
        .put::<Settings>(&"language".to_string(), &"en".to_string())
        .unwrap();

    // Retrieve settings
    assert_eq!(
        store.get::<Settings>(&"theme".to_string()).unwrap(),
        Some("dark".to_string())
    );
    assert_eq!(
        store.get::<Settings>(&"language".to_string()).unwrap(),
        Some("en".to_string())
    );

    // Iterate
    let results = store.iter::<Settings>().unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn tuple_keys() {
    // Column with tuple keys
    struct UserPosts;
    impl Column for UserPosts {
        const CF_NAME: &'static str = "user_posts";
        type Key = (u64, u64); // (user_id, post_id)
        type Value = String;   // post title
    }

    let store = TypedStore::new(MemoryStore::new());

    // Store posts for multiple users
    store.put::<UserPosts>(&(1, 1), &"User 1, Post 1".to_string()).unwrap();
    store.put::<UserPosts>(&(1, 2), &"User 1, Post 2".to_string()).unwrap();
    store.put::<UserPosts>(&(2, 1), &"User 2, Post 1".to_string()).unwrap();

    // Retrieve specific post
    assert_eq!(
        store.get::<UserPosts>(&(1, 1)).unwrap(),
        Some("User 1, Post 1".to_string())
    );

    // Iterate all
    let results = store.iter::<UserPosts>().unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn serialization() {
    // This test verifies that wincode serialization is working correctly
    let store = TypedStore::new(MemoryStore::new());

    // Create a user with encoded data
    let user = "Grace|45|grace@example.com".to_string();

    // Store and retrieve
    store.put::<Users>(&123, &user).unwrap();
    let retrieved = store.get::<Users>(&123).unwrap();

    // Verify exact match
    assert_eq!(retrieved, Some(user));

    // Verify that a different key doesn't retrieve the same value
    assert_eq!(store.get::<Users>(&124).unwrap(), None);
}

#[test]
fn numeric_values() {
    // Column with numeric values
    struct Counters;
    impl Column for Counters {
        const CF_NAME: &'static str = "counters";
        type Key = String;
        type Value = u64;
    }

    let store = TypedStore::new(MemoryStore::new());

    store.put::<Counters>(&"page_views".to_string(), &1000).unwrap();
    store.put::<Counters>(&"api_calls".to_string(), &5000).unwrap();

    assert_eq!(
        store.get::<Counters>(&"page_views".to_string()).unwrap(),
        Some(1000)
    );
    assert_eq!(
        store.get::<Counters>(&"api_calls".to_string()).unwrap(),
        Some(5000)
    );
}
