use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{distributions::Alphanumeric, Rng};
use solana_sdk::pubkey::Pubkey;
use tape_api::prelude::*;
use tape_network::store::TapeStore;
use tempdir::TempDir;

const SEGMENTS_PER_TAPE: u64 = 1000;
const NUM_TAPES: usize = 1000;

fn generate_random_data(size: usize) -> Vec<u8> {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(size)
        .collect()
}

fn bench_add_segments(c: &mut Criterion) {
    let temp_dir = TempDir::new("bench_add_segments").unwrap();
    let store = TapeStore::new(temp_dir.path()).unwrap();

    let mut group = c.benchmark_group("add_segments");
    group.bench_function("add_segment", |b| {
        let tape_address = Pubkey::new_unique();
        let segment_number = 0;
        let data = generate_random_data(SEGMENT_SIZE);

        b.iter(|| {
            store
                .add_segment(
                    black_box(&tape_address),
                    black_box(segment_number),
                    black_box(data.clone()),
                )
                .unwrap();
        })
    });
    group.finish();
}

fn bench_add_segments_batch(c: &mut Criterion) {
    let temp_dir = TempDir::new("bench_add_segments_batch").unwrap();
    let store = TapeStore::new(temp_dir.path()).unwrap();

    let mut group = c.benchmark_group("add_segments_batch");
    group.bench_function("batch", |b| {
        let tape_address = Pubkey::new_unique();
        let segment_addresses = vec![tape_address; SEGMENTS_PER_TAPE as usize];
        let segment_numbers = (0..SEGMENTS_PER_TAPE).collect::<Vec<_>>();
        let segment_data = (0..SEGMENTS_PER_TAPE)
            .map(|_| generate_random_data(SEGMENT_SIZE))
            .collect::<Vec<_>>();

        b.iter(|| {
            store
                .add_segments_batch(
                    black_box(&segment_addresses),
                    black_box(&segment_numbers),
                    black_box(segment_data.clone()),
                )
                .unwrap();
        });
    });
    group.finish();
}


fn bench_add_slots(c: &mut Criterion) {
    let temp_dir = TempDir::new("bench_add_slots").unwrap();
    let store = TapeStore::new(temp_dir.path()).unwrap();

    let mut group = c.benchmark_group("add_slots");
    group.bench_function("add_slot", |b| {
        let tape_address = Pubkey::new_unique();
        let segment_number = 0;
        let slot = 12345;

        b.iter(|| {
            store
                .add_slot(
                    black_box(&tape_address),
                    black_box(segment_number),
                    black_box(slot),
                )
                .unwrap();
        })
    });
    group.finish();
}

fn bench_add_slots_batch(c: &mut Criterion) {
    let temp_dir = TempDir::new("bench_add_slots_batch").unwrap();
    let store = TapeStore::new(temp_dir.path()).unwrap();

    let mut group = c.benchmark_group("add_slots_batch");
    group.bench_function("batch", |b| {
        let tape_address = Pubkey::new_unique();
        let segment_addresses = vec![tape_address; SEGMENTS_PER_TAPE as usize];
        let segment_numbers = (0..SEGMENTS_PER_TAPE).collect::<Vec<_>>();
        let slot_values = (0..SEGMENTS_PER_TAPE as u64).collect::<Vec<_>>();

        b.iter(|| {
            store
                .add_slots_batch(
                    black_box(&segment_addresses),
                    black_box(&segment_numbers),
                    black_box(&slot_values),
                )
                .unwrap();
        });
    });
    group.finish();
}


fn bench_add_tape(c: &mut Criterion) {
    let mut group = c.benchmark_group("add_tape");

    group.bench_function("add_tape_with_segments", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new("bench_add_tape").unwrap();
            let store = TapeStore::new(temp_dir.path()).unwrap();
            let tape_address = Pubkey::new_unique();
            let tape_number = 1;

            for segment_number in 0..SEGMENTS_PER_TAPE {
                let data = generate_random_data(SEGMENT_SIZE);
                store
                    .add_segment(&tape_address, segment_number, data)
                    .unwrap();
                store
                    .add_slot(&tape_address, segment_number, segment_number)
                    .unwrap();
            }

            store
                .add_tape(black_box(tape_number), black_box(&tape_address))
                .unwrap();
        })
    });
    group.finish();
}

fn bench_add_many_tapes(c: &mut Criterion) {
    let mut group = c.benchmark_group("add_many_tapes");

    group.bench_function("add_many_tapes", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new("bench_add_many").unwrap();
            let store = TapeStore::new(temp_dir.path()).unwrap();

            for tape_idx in 0..NUM_TAPES {
                let tape_address = Pubkey::new_unique();
                let tape_number = (tape_idx + 1) as u64;

                for segment_number in 0..SEGMENTS_PER_TAPE {
                    let data = generate_random_data(SEGMENT_SIZE);
                    store
                        .add_segment(&tape_address, segment_number, data)
                        .unwrap();
                    store
                        .add_slot(&tape_address, segment_number, segment_number)
                        .unwrap();
                }

                store
                    .add_tape(black_box(tape_number), black_box(&tape_address))
                    .unwrap();
            }
        })
    });
    group.finish();
}

fn bench_add_tapes_batch(c: &mut Criterion) {
    let temp_dir = TempDir::new("bench_add_tapes_batch").unwrap();
    let store = TapeStore::new(temp_dir.path()).unwrap();

    let mut group = c.benchmark_group("add_tapes_batch");
    group.bench_function("batch", |b| {
        let tape_addresses = (0..NUM_TAPES).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
        let tape_numbers = (1..=NUM_TAPES as u64).collect::<Vec<_>>();

        b.iter(|| {
            store
                .add_tapes_batch(black_box(&tape_numbers), black_box(&tape_addresses))
                .unwrap();
        });
    });
    group.finish();
}


fn bench_get_segment(c: &mut Criterion) {
    let temp_dir = TempDir::new("bench_get_segment").unwrap();
    let store = TapeStore::new(temp_dir.path()).unwrap();

    let mut tape_numbers = Vec::with_capacity(NUM_TAPES);
    for tape_idx in 0..NUM_TAPES {
        let tape_address = Pubkey::new_unique();
        let tape_number = (tape_idx + 1) as u64;
        tape_numbers.push(tape_number);

        for segment_number in 0..SEGMENTS_PER_TAPE {
            let data = generate_random_data(SEGMENT_SIZE);
            store
                .add_segment(&tape_address, segment_number, data)
                .unwrap();
            store
                .add_slot(&tape_address, segment_number, segment_number)
                .unwrap();
        }
        store.add_tape(tape_number, &tape_address).unwrap();
    }

    let mut group = c.benchmark_group("get_segment");
    group.bench_function("get_segment_many_tapes", |b| {
        let tape_number = tape_numbers[NUM_TAPES / 2];
        let segment_number = SEGMENTS_PER_TAPE / 2;

        b.iter(|| {
            store
                .get_segment(black_box(tape_number), black_box(segment_number))
                .unwrap();
        })
    });
    group.finish();
}

fn bench_get_segment_by_address(c: &mut Criterion) {
    let temp_dir = TempDir::new("bench_get_segment_by_address").unwrap();
    let store = TapeStore::new(temp_dir.path()).unwrap();

    let mut tape_addresses = Vec::with_capacity(NUM_TAPES);
    for _tape_idx in 0..NUM_TAPES {
        let tape_address = Pubkey::new_unique();
        tape_addresses.push(tape_address);

        for segment_number in 0..SEGMENTS_PER_TAPE {
            let data = generate_random_data(SEGMENT_SIZE);
            store
                .add_segment(&tape_address, segment_number, data)
                .unwrap();
            store
                .add_slot(&tape_address, segment_number, segment_number)
                .unwrap();
        }
    }

    let mut group = c.benchmark_group("get_segment_by_address");
    group.bench_function("get_segment_by_address_many_tapes", |b| {
        let tape_address = tape_addresses[NUM_TAPES / 2];
        let segment_number = SEGMENTS_PER_TAPE / 2;

        b.iter(|| {
            store
                .get_segment_by_address(black_box(&tape_address), black_box(segment_number))
                .unwrap();
        })
    });
    group.finish();
}

fn bench_get_tape_segments(c: &mut Criterion) {
    let temp_dir = TempDir::new("bench_get_tape_segments").unwrap();
    let store = TapeStore::new(temp_dir.path()).unwrap();

    let mut tape_addresses = Vec::with_capacity(NUM_TAPES);
    for tape_idx in 0..NUM_TAPES {
        let tape_address = Pubkey::new_unique();
        let tape_number = (tape_idx + 1) as u64;
        tape_addresses.push(tape_address);

        for segment_number in 0..SEGMENTS_PER_TAPE {
            let data = generate_random_data(SEGMENT_SIZE);
            store
                .add_segment(&tape_address, segment_number, data)
                .unwrap();
            store
                .add_slot(&tape_address, segment_number, segment_number)
                .unwrap();
        }
        store.add_tape(tape_number, &tape_address).unwrap();
    }

    let mut group = c.benchmark_group("get_tape_segments");
    group.bench_function("get_tape_segments_many_tapes", |b| {
        let tape_address = &tape_addresses[NUM_TAPES / 2];

        b.iter(|| {
            store.get_tape_segments(black_box(tape_address)).unwrap();
        })
    });
    group.finish();
}

fn bench_get_slot(c: &mut Criterion) {
    let temp_dir = TempDir::new("bench_get_slot").unwrap();
    let store = TapeStore::new(temp_dir.path()).unwrap();

    let mut tape_numbers = Vec::with_capacity(NUM_TAPES);
    for tape_idx in 0..NUM_TAPES {
        let tape_address = Pubkey::new_unique();
        let tape_number = (tape_idx + 1) as u64;
        tape_numbers.push(tape_number);

        for segment_number in 0..SEGMENTS_PER_TAPE {
            let data = generate_random_data(SEGMENT_SIZE);
            store
                .add_segment(&tape_address, segment_number, data)
                .unwrap();
            store
                .add_slot(&tape_address, segment_number, segment_number)
                .unwrap();
        }
        store.add_tape(tape_number, &tape_address).unwrap();
    }

    let mut group = c.benchmark_group("get_slot");
    group.bench_function("get_slot_many_tapes", |b| {
        let tape_number = tape_numbers[NUM_TAPES / 2];
        let segment_number = SEGMENTS_PER_TAPE / 2;

        b.iter(|| {
            store
                .get_slot(black_box(tape_number), black_box(segment_number))
                .unwrap();
        })
    });
    group.finish();
}

fn bench_get_slot_by_address(c: &mut Criterion) {
    let temp_dir = TempDir::new("bench_get_slot_by_address").unwrap();
    let store = TapeStore::new(temp_dir.path()).unwrap();

    let mut tape_addresses = Vec::with_capacity(NUM_TAPES);
    for _tape_idx in 0..NUM_TAPES {
        let tape_address = Pubkey::new_unique();
        tape_addresses.push(tape_address);

        for segment_number in 0..SEGMENTS_PER_TAPE {
            let data = generate_random_data(SEGMENT_SIZE);
            store
                .add_segment(&tape_address, segment_number, data)
                .unwrap();
            store
                .add_slot(&tape_address, segment_number, segment_number)
                .unwrap();
        }
    }

    let mut group = c.benchmark_group("get_slot_by_address");
    group.bench_function("get_slot_by_address_many_tapes", |b| {
        let tape_address = tape_addresses[NUM_TAPES / 2];
        let segment_number = SEGMENTS_PER_TAPE / 2;

        b.iter(|| {
            store
                .get_slot_by_address(black_box(&tape_address), black_box(segment_number))
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
        bench_add_segments,
        bench_add_segments_batch,
        bench_add_slots,
        bench_add_slots_batch,
        bench_add_tape,
        bench_add_many_tapes,
        bench_add_tapes_batch,
        bench_get_segment,
        bench_get_tape_segments,
        bench_get_slot,
        bench_get_segment_by_address,
        bench_get_slot_by_address
}

criterion_main!(benches);
