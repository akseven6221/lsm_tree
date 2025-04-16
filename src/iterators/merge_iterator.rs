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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();
        // 使用 enumerate 来同时获取索引和迭代器
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                // 将有效的迭代器包装后放入堆中
                heap.push(HeapWrapper(idx, iter));
            }
        }

        // 从堆顶弹出第一个元素（key 最小，如果 key 相同则 index 最小）作为初始 current
        let current = heap.pop();

        MergeIterator {
            iters: heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current
            .as_ref()
            .map(|x| x.1.key())
            .unwrap_or_else(|| KeySlice::from_slice(&[]))
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .map(|x| x.1.value())
            .unwrap_or_else(|| &[])
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        // 1. 取出当前的 HeapWrapper，将 self.current 置为 None
        let mut current_wrapper = match self.current.take() {
            Some(wrapper) => wrapper,
            None => return Ok(()), // 如果已经结束，直接返回
        };

        // 2. 保存当前暴露的 key，用于后续跳过相同 key
        let current_key = current_wrapper.1.key().to_key_vec(); // 需要复制一份 key

        // 3. 将刚处理完的迭代器向前移动一步
        current_wrapper.1.next()?;

        // 4. 如果移动后仍然有效，将其重新加入堆中
        if current_wrapper.1.is_valid() {
            self.iters.push(current_wrapper);
        }

        // 5. 循环处理堆顶元素，直到找到一个与 current_key 不同的 key，或者堆为空
        while let Some(top_wrapper) = self.iters.peek() {
            // 检查堆顶元素的 key 是否与我们刚刚处理的 key 相同
            if top_wrapper.1.key() == current_key.as_key_slice() {
                // 如果 key 相同，说明这个元素是旧版本，需要跳过
                // 从堆中弹出这个元素
                let mut skipped_wrapper = self.iters.pop().unwrap(); // unwrap 安全，因为 peek() 刚检查过

                // 将这个被跳过的迭代器也向前移动一步
                skipped_wrapper.1.next()?;

                // 如果移动后仍然有效，重新加入堆中
                if skipped_wrapper.1.is_valid() {
                    self.iters.push(skipped_wrapper);
                }
                // 继续循环，检查新的堆顶元素
            } else {
                // 如果堆顶元素的 key 不同，说明找到了下一个应该暴露的 key，跳出循环
                break;
            }
        }

        // 6. 循环结束后，堆顶的元素（如果有的话）就是下一个 current
        self.current = self.iters.pop();

        Ok(())
    }
}
