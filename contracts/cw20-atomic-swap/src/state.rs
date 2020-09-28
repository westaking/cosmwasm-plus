use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::balance::Balance;
use cosmwasm_std::{Binary, BlockInfo, CanonicalAddr, Order, ReadonlyStorage, StdError, StdResult, Storage };
use cosmwasm_storage::{bucket, bucket_read, prefixed_read, Bucket, ReadonlyBucket};
use cw20::Expiration;

#[derive(Serialize, Deserialize, Clone, PartialEq, JsonSchema, Debug, Default)]
pub struct AtomicSwap {
    /// This is the sha-256 hash of the preimage
    pub hash: Binary,
    pub recipient: CanonicalAddr,
    pub source: CanonicalAddr,
    pub expires: Expiration,
    /// Balance in native tokens, or cw20 token
    pub balance: Balance,
}

impl AtomicSwap {
    pub fn is_expired(&self, block: &BlockInfo) -> bool {
        self.expires.is_expired(&block)
    }
}

pub const PREFIX_SWAP: &[u8] = b"atomic_swap";
pub const RECIPIENT_INDEX: &[u8] = b"asri";
const MARKER_VALUE: u64 = 0u64;

/// Returns a bucket with all swaps (query by id)
pub fn create_atomic_swap<S: Storage>(storage: &mut S, key: &[u8], a: &AtomicSwap) -> StdResult<()> {
    atomic_swaps(storage).save(&key, a)?;
    atomic_swaps_recipient_index(storage, a.recipient.as_slice())
        .save(key, &MARKER_VALUE)
}

// (Secondary index, primary id) -> u64
pub fn atomic_swaps_recipient_index<'a, S: Storage>(storage: &'a mut S, rec: &[u8]) -> Bucket<'a, S, u64> {
    Bucket::multilevel(&[RECIPIENT_INDEX, rec], storage)
}

/// Returns a bucket with all swaps (query by id)
pub fn atomic_swaps<S: Storage>(storage: &mut S) -> Bucket<S, AtomicSwap> {
    bucket(PREFIX_SWAP, storage)
}

/// Returns a bucket with all swaps (query by id)
/// (read-only version for queries)
pub fn atomic_swaps_read<S: ReadonlyStorage>(storage: &S) -> ReadonlyBucket<S, AtomicSwap> {
    bucket_read(PREFIX_SWAP, storage)
}

/// This returns the list of ids for all active swaps
pub fn all_swap_ids<S: ReadonlyStorage>(
    storage: &S,
    start: Option<Vec<u8>>,
    limit: usize,
) -> StdResult<Vec<String>> {
    prefixed_read(PREFIX_SWAP, storage)
        .range(start.as_deref(), None, Order::Ascending)
        .take(limit)
        .map(|(k, _)| String::from_utf8(k).map_err(|_| StdError::invalid_utf8("Parsing swap id")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use cosmwasm_std::testing::MockStorage;
    use cosmwasm_std::Binary;

    #[test]
    fn test_no_swap_ids() {
        let storage = MockStorage::new();
        let ids = all_swap_ids(&storage, None, 10).unwrap();
        assert_eq!(0, ids.len());
    }

    fn dummy_swap() -> AtomicSwap {
        AtomicSwap {
            recipient: CanonicalAddr(Binary(b"recip".to_vec())),
            source: CanonicalAddr(Binary(b"source".to_vec())),
            hash: Binary("hash".into()),
            ..AtomicSwap::default()
        }
    }

    #[test]
    fn test_all_swap_ids() {
        let mut storage = MockStorage::new();
        atomic_swaps(&mut storage)
            .save("lazy".as_bytes(), &dummy_swap())
            .unwrap();
        atomic_swaps(&mut storage)
            .save("assign".as_bytes(), &dummy_swap())
            .unwrap();
        atomic_swaps(&mut storage)
            .save("zen".as_bytes(), &dummy_swap())
            .unwrap();

        let ids = all_swap_ids(&storage, None, 10).unwrap();
        assert_eq!(3, ids.len());
        assert_eq!(
            vec!["assign".to_string(), "lazy".to_string(), "zen".to_string()],
            ids
        )
    }

    #[test]
    fn test_atomic_swap_recipient_index() {
        let mut storage = MockStorage::new();
        let recipient1 = b"0";
        let key11= 00u8;
        let recipient2 = b"1";
        let key21 = 01u8;
        let key22 = 02u8;

        let aswap1 = AtomicSwap {
            recipient: CanonicalAddr(Binary(recipient1.to_vec())),
            ..AtomicSwap::default()
        };
        create_atomic_swap(&mut storage, &vec![key11], &aswap1).unwrap();

        let aswap2 = AtomicSwap {
            recipient: CanonicalAddr(Binary(recipient2.to_vec())),
            ..AtomicSwap::default()
        };
        create_atomic_swap(&mut storage, &vec![key21], &aswap2).unwrap();
        create_atomic_swap(&mut storage, &vec![key22], &aswap2).unwrap();

        // first recipient
        let res: StdResult<Vec<Vec<u8>>> = atomic_swaps_recipient_index(&mut storage, recipient1)
            .range(None, None, Order::Ascending)
            .map(|item| {
                let (k, _) = item?;
                Ok(k)
            }).collect();

        assert_eq!(vec![key11], res.unwrap().concat());

        // second recipient
        let res: StdResult<Vec<Vec<u8>>> = atomic_swaps_recipient_index(&mut storage, recipient2)
            .range(None, None, Order::Ascending)
            .map(|item| {
                let (k, _) = item?;
                return Ok(k)
            }).collect();

        assert_eq!(vec![key21, key22], res.unwrap().concat());
    }
}
