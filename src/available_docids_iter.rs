use std::iter::Chain;
use std::ops::RangeInclusive;

use roaring::treemap::IntoIter;
use roaring::RoaringTreemap;

pub struct AvailableDocIds {
    iter: Chain<IntoIter, RangeInclusive<u64>>,
}

impl AvailableDocIds {
    pub fn new(docids: &RoaringTreemap) -> AvailableDocIds {
        match docids.max() {
            Some(last_id) => {
                let mut available = RoaringTreemap::from_iter(0..last_id);
                available -= docids;

                let iter = match last_id.checked_add(1) {
                    Some(id) => id..=u64::max_value(),
                    #[allow(clippy::reversed_empty_ranges)]
                    None => 1..=0, // empty range iterator
                };

                AvailableDocIds { iter: available.into_iter().chain(iter) }
            }
            None => {
                let empty = RoaringTreemap::new().into_iter();
                AvailableDocIds { iter: empty.chain(0..=u64::max_value()) }
            }
        }
    }
}

impl Iterator for AvailableDocIds {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let base = RoaringTreemap::new();
        let left = AvailableDocIds::new(&base);
        let right = 0..=u64::max_value();
        left.zip(right).take(500).for_each(|(l, r)| assert_eq!(l, r));
    }

    #[test]
    fn scattered() {
        let mut base = RoaringTreemap::new();
        base.insert(0);
        base.insert(10);
        base.insert(100);
        base.insert(405);

        let left = AvailableDocIds::new(&base);
        let right = (0..=u64::max_value()).filter(|&n| n != 0 && n != 10 && n != 100 && n != 405);
        left.zip(right).take(500).for_each(|(l, r)| assert_eq!(l, r));
    }
}
