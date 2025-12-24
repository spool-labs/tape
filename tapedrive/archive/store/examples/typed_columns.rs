//! Example demonstrating typed column usage in store
//!
//! Run with: cargo run --example typed_columns

use store::{Column, MemoryStore, TypedStore};

// Define column families with primitive types
// For custom structs, implement wincode's SchemaRead and SchemaWrite traits

struct Users;
impl Column for Users {
    const CF_NAME: &'static str = "users";
    type Key = u64; // user ID
    type Value = String; // serialized as "name|email|age"
}

struct Posts;
impl Column for Posts {
    const CF_NAME: &'static str = "posts";
    type Key = u64; // post ID
    type Value = String; // serialized as "title|content|author_id|published"
}

struct Comments;
impl Column for Comments {
    const CF_NAME: &'static str = "comments";
    type Key = u64; // comment ID
    type Value = String; // serialized as "post_id|user_id|text"
}

fn main() {
    println!("=== Typed Column Store Example ===\n");

    // Create a typed store wrapping an in-memory backend
    let store = TypedStore::new(MemoryStore::new());

    // 1. Create some users (as encoded strings)
    println!("1. Creating users...");
    let users = vec![
        "Alice|alice@example.com|30",
        "Bob|bob@example.com|25",
        "Charlie|charlie@example.com|35",
    ];

    for (id, user) in users.iter().enumerate() {
        let user_id = (id + 1) as u64;
        store.put::<Users>(&user_id, &user.to_string()).unwrap();
        let name = user.split('|').next().unwrap();
        println!("  - Stored user {}: {}", user_id, name);
    }

    // 2. Create some posts
    println!("\n2. Creating posts...");
    let posts = vec![
        "Hello World|This is my first post!|1|true",
        "Rust is Amazing|Let me tell you about Rust...|1|true",
        "Draft Post|Work in progress...|2|false",
    ];

    for (id, post) in posts.iter().enumerate() {
        let post_id = (id + 1) as u64;
        store.put::<Posts>(&post_id, &post.to_string()).unwrap();
        let title = post.split('|').next().unwrap();
        println!("  - Stored post {}: {}", post_id, title);
    }

    // 3. Create some comments
    println!("\n3. Creating comments...");
    let comments = vec![
        "1|2|Great post!",
        "1|3|Very helpful, thanks!",
        "2|3|I agree, Rust is fantastic!",
    ];

    for (id, comment) in comments.iter().enumerate() {
        let comment_id = (id + 1) as u64;
        store.put::<Comments>(&comment_id, &comment.to_string()).unwrap();
        let parts: Vec<&str> = comment.split('|').collect();
        println!(
            "  - Stored comment {} on post {}",
            comment_id, parts[0]
        );
    }

    // 4. Query individual records
    println!("\n4. Querying individual records...");
    if let Some(user) = store.get::<Users>(&1).unwrap() {
        let parts: Vec<&str> = user.split('|').collect();
        println!("  - User 1: {} ({})", parts[0], parts[1]);
    }

    if let Some(post) = store.get::<Posts>(&2).unwrap() {
        let title = post.split('|').next().unwrap();
        println!("  - Post 2: {}", title);
    }

    // 5. Check existence
    println!("\n5. Checking existence...");
    println!(
        "  - User 1 exists: {}",
        store.contains::<Users>(&1).unwrap()
    );
    println!(
        "  - User 999 exists: {}",
        store.contains::<Users>(&999).unwrap()
    );

    // 6. Iterate over all users
    println!("\n6. All users:");
    let all_users = store.iter::<Users>().unwrap();
    for (id, user) in all_users {
        let parts: Vec<&str> = user.split('|').collect();
        println!("  - [{}] {} (age: {})", id, parts[0], parts[2]);
    }

    // 7. Iterate over all posts
    println!("\n7. All posts:");
    let all_posts = store.iter::<Posts>().unwrap();
    for (id, post) in all_posts {
        let parts: Vec<&str> = post.split('|').collect();
        let status = if parts[3] == "true" { "published" } else { "draft" };
        println!("  - [{}] {} ({})", id, parts[0], status);
    }

    // 8. Iterate over all comments
    println!("\n8. All comments:");
    let all_comments = store.iter::<Comments>().unwrap();
    for (id, comment) in all_comments {
        let parts: Vec<&str> = comment.split('|').collect();
        println!(
            "  - [{}] Comment on post {} by user {}: {}",
            id, parts[0], parts[1], parts[2]
        );
    }

    // 9. Update a record
    println!("\n9. Updating a record...");
    if let Some(user) = store.get::<Users>(&1).unwrap() {
        let parts: Vec<&str> = user.split('|').collect();
        println!("  - Original: {} (age {})", parts[0], parts[2]);
        let updated = format!("{}|{}|31", parts[0], parts[1]);
        store.put::<Users>(&1, &updated).unwrap();
        let updated = store.get::<Users>(&1).unwrap().unwrap();
        let parts: Vec<&str> = updated.split('|').collect();
        println!("  - Updated: {} (age {})", parts[0], parts[2]);
    }

    // 10. Delete a record
    println!("\n10. Deleting a record...");
    println!("  - User 3 exists before delete: {}", store.contains::<Users>(&3).unwrap());
    store.delete::<Users>(&3).unwrap();
    println!("  - User 3 exists after delete: {}", store.contains::<Users>(&3).unwrap());

    println!("\n=== Example Complete ===");
}
