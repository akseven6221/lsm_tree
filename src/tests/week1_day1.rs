// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{sync::Arc, thread};

use tempfile::tempdir;

use crate::{
    lsm_storage::{LsmStorageInner, LsmStorageOptions},
    mem_table::MemTable,
};

#[test]
fn test_task1_memtable_get() {
    let memtable = MemTable::create(0);
    memtable.for_testing_put_slice(b"key1", b"value1").unwrap();
    memtable.for_testing_put_slice(b"key2", b"value2").unwrap();
    memtable.for_testing_put_slice(b"key3", b"value3").unwrap();
    assert_eq!(
        &memtable.for_testing_get_slice(b"key1").unwrap()[..],
        b"value1"
    );
    assert_eq!(
        &memtable.for_testing_get_slice(b"key2").unwrap()[..],
        b"value2"
    );
    assert_eq!(
        &memtable.for_testing_get_slice(b"key3").unwrap()[..],
        b"value3"
    );
}

#[test]
fn test_task1_memtable_overwrite() {
    let memtable = MemTable::create(0);
    memtable.for_testing_put_slice(b"key1", b"value1").unwrap();
    memtable.for_testing_put_slice(b"key2", b"value2").unwrap();
    memtable.for_testing_put_slice(b"key3", b"value3").unwrap();
    memtable.for_testing_put_slice(b"key1", b"value11").unwrap();
    memtable.for_testing_put_slice(b"key2", b"value22").unwrap();
    memtable.for_testing_put_slice(b"key3", b"value33").unwrap();
    assert_eq!(
        &memtable.for_testing_get_slice(b"key1").unwrap()[..],
        b"value11"
    );
    assert_eq!(
        &memtable.for_testing_get_slice(b"key2").unwrap()[..],
        b"value22"
    );
    assert_eq!(
        &memtable.for_testing_get_slice(b"key3").unwrap()[..],
        b"value33"
    );
}

#[test]
fn test_task2_storage_integration() {
    let dir = tempdir().unwrap();
    let storage = Arc::new(
        LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_week1_test()).unwrap(),
    );
    assert_eq!(&storage.get(b"0").unwrap(), &None);
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233");
    assert_eq!(&storage.get(b"2").unwrap().unwrap()[..], b"2333");
    assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"23333");
    storage.delete(b"2").unwrap();
    assert!(storage.get(b"2").unwrap().is_none());
    storage.delete(b"0").unwrap(); // should NOT report any error
}

#[test]
fn test_task3_storage_integration() {
    let dir = tempdir().unwrap();
    let storage = Arc::new(
        LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_week1_test()).unwrap(),
    );
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    assert_eq!(storage.state.read().imm_memtables.len(), 1);
    let previous_approximate_size = storage.state.read().imm_memtables[0].approximate_size();
    assert_eq!(previous_approximate_size, 15);
    assert!(previous_approximate_size >= 15);
    storage.put(b"1", b"2333").unwrap();
    storage.put(b"2", b"23333").unwrap();
    storage.put(b"3", b"233333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    assert_eq!(storage.state.read().imm_memtables.len(), 2);
    assert!(
        storage.state.read().imm_memtables[1].approximate_size() == previous_approximate_size,
        "wrong order of memtables?"
    );
    assert!(storage.state.read().imm_memtables[0].approximate_size() > previous_approximate_size);
}

#[test]
fn test_task3_freeze_on_capacity() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_week1_test();
    options.target_sst_size = 1024;
    options.num_memtable_limit = 1000;
    let storage = Arc::new(LsmStorageInner::open(dir.path(), options).unwrap());
    for _ in 0..1000 {
        storage.put(b"1", b"2333").unwrap();
    }
    let num_imm_memtables = storage.state.read().imm_memtables.len();
    assert!(num_imm_memtables >= 1, "no memtable frozen?");
    for _ in 0..1000 {
        storage.delete(b"1").unwrap();
    }
    assert!(
        storage.state.read().imm_memtables.len() > num_imm_memtables,
        "no more memtable frozen?"
    );
}

#[test]
// #[ignore] // 依赖时序，可能不稳定
fn test_concurrent_put_without_explicit_drop_guard() {
    const NUM_THREADS: usize = 4;
    const PUTS_PER_THREAD: usize = 5;
    // 设置小阈值，增加冻结频率
    const TARGET_SST_SIZE: usize = 20;

    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_week1_test();
    options.target_sst_size = TARGET_SST_SIZE;
    let storage = Arc::new(LsmStorageInner::open(dir.path(), options).unwrap());

    let mut handles = vec![];

    println!(
        "[Main] Spawning {} threads, each doing {} puts.",
        NUM_THREADS, PUTS_PER_THREAD
    );

    for i in 0..NUM_THREADS {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            let thread_id = i;
            for j in 0..PUTS_PER_THREAD {
                let key = format!("t{}-k{}", thread_id, j);
                let value = format!("v{}", j);
                match storage_clone.put(key.as_bytes(), value.as_bytes()) {
                    Ok(()) => {
                        // println!("[Thread {}] Put {} successful", thread_id, j);
                    }
                    Err(e) => {
                        eprintln!("[Thread {}] Put {} failed: {}", thread_id, j, e);
                        // 在测试中断言失败可能更好，但这里先打印
                        return Err(e);
                    }
                }
                // 短暂 sleep 增加交错执行的可能性
                // std::thread::sleep(Duration::from_millis(1));
            }
            Ok(())
        });
        handles.push(handle);
    }

    // 等待所有线程完成
    let mut errors = 0;
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(Ok(())) => println!("[Main] Thread {} finished successfully.", i),
            Ok(Err(e)) => {
                eprintln!("[Main] Thread {} finished with error: {}", i, e);
                errors += 1;
            }
            Err(_) => {
                eprintln!("[Main] Thread {} panicked!", i);
                errors += 1;
            }
        }
    }

    // 检查最终状态
    let state = storage.state.read();
    let imm_memtables_count = state.imm_memtables.len();
    let active_memtable_size = state.memtable.approximate_size();

    println!(
        "[Main] Final state: {} immutable memtables, active memtable size: {}",
        imm_memtables_count, active_memtable_size
    );
    for (i, imm_table) in state.imm_memtables.iter().enumerate() {
        let size = imm_table.approximate_size();
        println!("[Main] Immutable memtable {} size: {}", i, size);
        // 在这种并发场景下，由于竞争，不应该产生空的不可变表
        assert!(
            size > 0,
            "Found an empty immutable memtable at index {}, which should not happen even with concurrency.",
            i
        );
    }

    // 如果有任何线程出错，则测试失败
    assert_eq!(errors, 0, "Some threads failed during concurrent puts.");

    // 我们可以根据总写入量和阈值估算一个大致的不可变表数量范围
    // 总写入量 = NUM_THREADS * PUTS_PER_THREAD * (avg_key_len + avg_value_len)
    // 预期表数量大约是 总写入量 / TARGET_SST_SIZE
    // 这里不做精确断言，因为并发冻结的时机不确定
    println!("[Main] Test finished checking state.");
    // 如果代码能运行到这里没有 panic 或 assert 失败，说明在这种并发下没观察到明显错误。
}

#[test]
fn test_task4_storage_integration() {
    let dir = tempdir().unwrap();
    let storage = Arc::new(
        LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_week1_test()).unwrap(),
    );
    assert_eq!(&storage.get(b"0").unwrap(), &None);
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    storage.delete(b"1").unwrap();
    storage.delete(b"2").unwrap();
    storage.put(b"3", b"2333").unwrap();
    storage.put(b"4", b"23333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    storage.put(b"1", b"233333").unwrap();
    storage.put(b"3", b"233333").unwrap();
    assert_eq!(storage.state.read().imm_memtables.len(), 2);
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233333");
    assert_eq!(&storage.get(b"2").unwrap(), &None);
    assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"233333");
    assert_eq!(&storage.get(b"4").unwrap().unwrap()[..], b"23333");
}
