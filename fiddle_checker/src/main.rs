#![feature(const_fn_trait_bound)]

use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
};

use futures::future::join_all;
use loader::NUM_FILES;

async fn check_part(num: usize) -> HashMap<u64, String> {
    let span = loader::load_file(num, Some("./cache")).await.unwrap();
    let mut map = HashMap::<u64, String>::new();

    for pixel in span {
        let mut hasher = &mut DefaultHasher::new();
        pixel.user_hash.hash(&mut hasher);
        let hash = hasher.finish();
        if let Some(old) = map.insert(hash, pixel.user_hash.clone()) {
            if pixel.user_hash != old {
                println!("{} collides with {}", pixel.user_hash, old);
            }
        }
    }

    map
}

#[tokio::main]
async fn main() {
    let parts: Vec<_> = (0..NUM_FILES).map(|num| check_part(num)).collect();

    let maps = join_all(parts).await;

    let all_users = maps
        .into_iter()
        .reduce(|mut a, b| {
            for (k, v) in b {
                if let Some(old) = a.insert(k, v.clone()) {
                    if v != old {
                        println!("{} collides with {}", v, old);
                    }
                }
            }
            a
        })
        // There will
        .unwrap();

    println!("{} users participated", all_users.len());
}
