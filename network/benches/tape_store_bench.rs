use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{distributions::Alphanumeric, Rng};
use solana_sdk::pubkey::Pubkey;
use tape_api::prelude::*;
use tape_network::store::*;
use tempdir::TempDir;

const SEGMENTS_PER_TAPE: u64 = 1000;
const NUM_TAPES: usize = 10;

fn generate_random_data(size: usize) -> Vec<u8> {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(size)
        .collect()
}

fn bench_put_segment(c: &mut Criterion) {
    let temp_dir = TempDir::new("bench_put_segment").unwrap();
    let store = TapeStore::new(temp_dir.path()).unwrap();

    let mut group = c.benchmark_group("put_segment");
    group.bench_function("put_segment", |b| {
        let tape_address = Pubkey::new_unique();
        let global_seg_idx = 0;
        let data = generate_random_data(PACKED_SEGMENT_SIZE);

        b.iter(|| {
            store
                .put_segment(
                    black_box(&tape_address),
                    black_box(global_seg_idx),
                    black_box(data.clone()),
                )
                .unwrap();
        })
    });
    group.finish();
}

fn bench_put_tape(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_tape");

    group.bench_function("put_tape_with_segments", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new("bench_put_tape").unwrap();
            let store = TapeStore::new(temp_dir.path()).unwrap();
            let tape_address = Pubkey::new_unique();
            let tape_number = 1;

            for global_seg_idx in 0..SEGMENTS_PER_TAPE {
                let data = generate_random_data(PACKED_SEGMENT_SIZE);
                store
                    .put_segment(&tape_address, global_seg_idx, data)
                    .unwrap();
            }

            store
                .put_tape_address(black_box(tape_number), black_box(&tape_address))
                .unwrap();
        })
    });
    group.finish();
}

fn bench_put_many_tapes(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_many_tapes");

    group.bench_function("put_many_tapes", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new("bench_put_many").unwrap();
            let store = TapeStore::new(temp_dir.path()).unwrap();

            for tape_idx in 0..NUM_TAPES {
                let tape_address = Pubkey::new_unique();
                let tape_number = (tape_idx + 1) as u64;

                for global_seg_idx in 0..SEGMENTS_PER_TAPE {
                    let data = generate_random_data(PACKED_SEGMENT_SIZE);
                    store
                        .put_segment(&tape_address, global_seg_idx, data)
                        .unwrap();
                }

                store
                    .put_tape_address(black_box(tape_number), black_box(&tape_address))
                    .unwrap();
            }
        })
    });
    group.finish();
}

fn bench_get_segment(c: &mut Criterion) {
    let temp_dir = TempDir::new("bench_get_segment").unwrap();
    let store = TapeStore::new(temp_dir.path()).unwrap();

    let mut tape_addresses = Vec::with_capacity(NUM_TAPES);
    for tape_idx in 0..NUM_TAPES {
        let tape_address = Pubkey::new_unique();
        let tape_number = (tape_idx + 1) as u64;
        tape_addresses.push(tape_address);

        for global_seg_idx in 0..SEGMENTS_PER_TAPE {
            let data = generate_random_data(PACKED_SEGMENT_SIZE);
            store
                .put_segment(&tape_address, global_seg_idx, data)
                .unwrap();
        }
        store.put_tape_address(tape_number, &tape_address).unwrap();
    }

    let mut group = c.benchmark_group("get_segment");
    group.bench_function("get_segment_many_tapes", |b| {
        let tape_address = tape_addresses[NUM_TAPES / 2];
        let global_seg_idx = SEGMENTS_PER_TAPE / 2;

        b.iter(|| {
            store
                .get_segment(black_box(&tape_address), black_box(global_seg_idx))
                .unwrap();
        })
    });
    group.finish();
}


fn customized_criterion() -> Criterion {
    Criterion::default().sample_size(20)
}

criterion_group! {
    name = benches;
    config = customized_criterion();
    targets = 
        bench_put_segment,
        bench_put_tape,
        bench_put_many_tapes,
        bench_get_segment,
}

criterion_main!(benches);
