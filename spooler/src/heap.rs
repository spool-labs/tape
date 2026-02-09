/// Lightweight, fast binary max-heap implementation. Uses less CU and memory than
/// `std::collections::BinaryHeap`.
pub struct MaxHeap<T: Ord> {
    data: Vec<T>,
}

impl<T: Ord> MaxHeap<T> {
    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        MaxHeap { data: Vec::with_capacity(cap) }
    }

    #[inline]
    pub fn push(&mut self, v: T) {
        self.data.push(v);
        self.sift_up(self.data.len() - 1);
    }

    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        let len = self.data.len();
        if len == 0 {
            return None;
        }
        self.data.swap(0, len - 1);
        let max = self.data.pop();
        if !self.data.is_empty() {
            self.sift_down(0);
        }
        max
    }

    #[inline]
    fn sift_up(&mut self, mut idx: usize) {
        while idx > 0 {
            let parent = (idx - 1) >> 1;
            if self.data[idx] > self.data[parent] {
                self.data.swap(idx, parent);
                idx = parent;
            } else {
                break;
            }
        }
    }

    #[inline]
    fn sift_down(&mut self, mut idx: usize) {
        let len = self.data.len();
        loop {
            let left = (idx << 1) + 1;
            let right = left + 1;

            if left >= len {
                break;
            }

            let mut largest = left;
            if right < len && self.data[right] > self.data[left] {
                largest = right;
            }

            if self.data[largest] > self.data[idx] {
                self.data.swap(idx, largest);
                idx = largest;
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MaxHeap;
    use std::cmp::Ordering;

    #[test]
    fn pop_from_empty_returns_none() {
        let mut heap: MaxHeap<i32> = MaxHeap::with_capacity(0);
        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn single_push_pop() {
        let mut heap = MaxHeap::with_capacity(1);
        heap.push(42);
        assert_eq!(heap.pop(), Some(42));
        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn max_order_integers() {
        let mut heap = MaxHeap::with_capacity(8);
        let items = [3, 1, 4, 1, 5, 9, 2, 6];
        for &x in &items {
            heap.push(x);
        }

        let mut popped = Vec::new();
        while let Some(x) = heap.pop() {
            popped.push(x);
        }

        assert_eq!(popped, vec![9, 6, 5, 4, 3, 2, 1, 1]);
    }

    #[test]
    fn interleaved_push_pop() {
        let mut heap = MaxHeap::with_capacity(4);

        heap.push(10);
        heap.push(20);
        assert_eq!(heap.pop(), Some(20));

        heap.push(5);
        heap.push(15);
        assert_eq!(heap.pop(), Some(15));
        assert_eq!(heap.pop(), Some(10));

        heap.push(25);
        assert_eq!(heap.pop(), Some(25));
        assert_eq!(heap.pop(), Some(5));
        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn handles_duplicates() {
        let mut heap = MaxHeap::with_capacity(10);
        let data = [7, 7, 7, 3, 3, 10, 10, 1, 1, 1];
        for &x in &data {
            heap.push(x);
        }

        let mut popped = Vec::new();
        while let Some(x) = heap.pop() {
            popped.push(x);
        }

        assert_eq!(popped, vec![10, 10, 7, 7, 7, 3, 3, 1, 1, 1]);
    }

    #[derive(Clone, Eq, PartialEq, Debug)]
    struct Pair {
        primary: i32,
        secondary: i32,
    }

    impl Ord for Pair {
        fn cmp(&self, other: &Self) -> Ordering {
            self.primary.cmp(&other.primary).then(self.secondary.cmp(&other.secondary))
        }
    }

    impl PartialOrd for Pair {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    #[test]
    fn works_with_custom_ord() {
        let mut heap = MaxHeap::with_capacity(5);
        let items = [
            Pair { primary: 5, secondary: 1 },
            Pair { primary: 5, secondary: 3 },
            Pair { primary: 5, secondary: 2 },
            Pair { primary: 6, secondary: 0 },
            Pair { primary: 4, secondary: 100 },
        ];
        for x in items {
            heap.push(x);
        }

        let mut popped = Vec::new();
        while let Some(x) = heap.pop() {
            popped.push(x);
        }

        assert_eq!(
            popped,
            vec![
                Pair { primary: 6, secondary: 0 },
                Pair { primary: 5, secondary: 3 },
                Pair { primary: 5, secondary: 2 },
                Pair { primary: 5, secondary: 1 },
                Pair { primary: 4, secondary: 100 },
            ]
        );
    }

    fn lcg_sequence(n: usize, mut x: u64) -> Vec<u64> {
        let a: u64 = 6364136223846793005;
        let c: u64 = 1;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            x = x.wrapping_mul(a).wrapping_add(c);
            out.push(x);
        }
        out
    }

    #[test]
    fn stress_against_sort() {
        let data = lcg_sequence(2000, 0x1234567890111111);

        let mut heap = MaxHeap::with_capacity(data.len());
        for &v in &data {
            heap.push(v);
        }

        let mut popped = Vec::with_capacity(data.len());
        while let Some(x) = heap.pop() {
            popped.push(x);
        }

        let mut sorted = data.clone();
        sorted.sort_unstable_by(|a, b| b.cmp(a));

        assert_eq!(popped, sorted);
    }
}
