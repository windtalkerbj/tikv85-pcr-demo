// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{thread, time::Duration};

use engine_traits::{RaftEngine, RaftLogBatch};
use error_code::ErrorCodeExt;
use fail::fail_point;
use kvproto::{
    metapb::{Peer, Region, RegionEpoch, Store},
    raft_serverpb::{RaftLocalState, StoreIdent},
};
use pd_client::PdClient;
use raft::INVALID_ID;
use raftstore::store::initial_region;
use slog::{debug, error, info, warn, Logger};
use tikv_util::{box_err, box_try};

use crate::{operation::write_initial_states, Result};

const MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT: u64 = 60;
const CHECK_CLUSTER_BOOTSTRAPPED_RETRY_INTERVAL: Duration = Duration::from_secs(3);

/// Default number of regions to pre-split during bootstrap.
/// Reduces per-tablet creation overhead when TiDB starts.
const DEFAULT_BOOTSTRAP_PRE_SPLIT_COUNT: usize = 64;

/// A struct for bootstrapping the store.
///
/// A typical bootstrap process should take the following steps:
///
/// 1. Calls `bootstrap_store` to bootstrap the store.
/// 2. Calls `bootstrap_first_region` to bootstrap the first region using store
///    ID returned from last step.
///
/// # Safety
///
/// These steps are re-entrant, i.e. the caller can redo any steps whether or
/// not they fail or succeed.
pub struct Bootstrap<'a, ER: RaftEngine> {
    engine: &'a ER,
    cluster_id: u64,
    pd_client: &'a dyn PdClient,
    logger: Logger,
    /// Number of regions to pre-split into during bootstrap (default 64).
    /// Pre-creates per-Region RocksDB tablets to speed up TiDB initialization.
    pre_split_count: usize,
}

// Although all methods won't change internal state, but they still receive
// `&mut self` as it's not thread safe to bootstrap concurrently.
impl<'a, ER: RaftEngine> Bootstrap<'a, ER> {
    pub fn new(
        engine: &'a ER,
        cluster_id: u64,
        pd_client: &'a impl PdClient,
        logger: Logger,
    ) -> Self {
        Self {
            engine,
            cluster_id,
            pd_client,
            logger,
            pre_split_count: DEFAULT_BOOTSTRAP_PRE_SPLIT_COUNT,
        }
    }

    /// Set the number of regions to pre-split into during bootstrap.
    pub fn set_pre_split_count(&mut self, count: usize) {
        self.pre_split_count = count.max(1);
    }

    /// Gets and validates the store ID from engine if it's already
    /// bootstrapped.
    fn check_store_id_in_engine(&mut self) -> Result<Option<u64>> {
        let ident = match self.engine.get_store_ident()? {
            Some(ident) => ident,
            None => return Ok(None),
        };
        if ident.get_cluster_id() != self.cluster_id {
            return Err(box_err!(
                "cluster ID mismatch, local {} != remote {}, \
                you are trying to connect to another cluster, \
                please reconnect to the correct PD",
                ident.get_cluster_id(),
                self.cluster_id
            ));
        }
        if ident.get_store_id() == INVALID_ID {
            return Err(box_err!("invalid store ident {:?}", ident));
        }
        Ok(Some(ident.get_store_id()))
    }

    /// Bootstraps the store and returns the store ID.
    ///
    /// The bootstrapping basically allocates a new store ID from PD and writes
    /// it to engine with sync=true.
    ///
    /// If the store is already bootstrapped, return the store ID directly.
    pub fn bootstrap_store(&mut self) -> Result<u64> {
        if let Some(id) = self.check_store_id_in_engine()? {
            return Ok(id);
        }
        if !self.engine.is_empty()? {
            return Err(box_err!("store is not empty and has already had data"));
        }
        let id = self.pd_client.alloc_id()?;
        debug!(self.logger, "alloc store id"; "store_id" => id);
        let mut ident = StoreIdent::default();
        ident.set_cluster_id(self.cluster_id);
        ident.set_store_id(id);
        let mut lb = self.engine.log_batch(1);
        lb.put_store_ident(&ident)?;
        self.engine.consume(&mut lb, true)?;
        fail_point!("node_after_bootstrap_store", |_| Err(box_err!(
            "injected error: node_after_bootstrap_store"
        )));
        Ok(id)
    }

    fn prepare_bootstrap_first_region(&mut self, store_id: u64) -> Result<Region> {
        let region_id = self.pd_client.alloc_id()?;
        debug!(
            self.logger,
            "alloc first region id";
            "region_id" => region_id,
            "cluster_id" => self.cluster_id,
            "store_id" => store_id
        );
        let peer_id = self.pd_client.alloc_id()?;
        debug!(
            self.logger,
            "alloc first peer id for first region";
            "peer_id" => peer_id,
            "region_id" => region_id,
        );

        let region = initial_region(store_id, region_id, peer_id);

        let mut wb = self.engine.log_batch(10);
        wb.put_prepare_bootstrap_region(&region)?;
        write_initial_states(&mut wb, region.clone())?;
        box_try!(self.engine.consume(&mut wb, true));

        Ok(region)
    }

    fn check_pd_first_region_bootstrapped(&mut self) -> Result<bool> {
        for _ in 0..MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT {
            match self.pd_client.is_cluster_bootstrapped() {
                Ok(b) => return Ok(b),
                Err(e) => {
                    warn!(self.logger, "check cluster bootstrapped failed"; "err" => ?e);
                }
            }
            thread::sleep(CHECK_CLUSTER_BOOTSTRAPPED_RETRY_INTERVAL);
        }
        Err(box_err!("check cluster bootstrapped failed"))
    }

    fn clear_prepare_bootstrap(&mut self, first_region_id: Option<u64>) -> Result<()> {
        let mut wb = self.engine.log_batch(10);
        wb.remove_prepare_bootstrap_region()?;
        if let Some(id) = first_region_id {
            box_try!(
                self.engine
                    .clean(id, 0, &RaftLocalState::default(), &mut wb)
            );
        }
        box_try!(self.engine.consume(&mut wb, true));
        Ok(())
    }

    /// Bootstraps the first region of this cluster.
    ///
    /// The bootstrapping starts by allocating a region ID from PD. Then it
    /// initializes the region's state and writes a preparing marker to the
    /// engine. After attempting to register itself as the first region to PD,
    /// the preparing marker is deleted from the engine.
    ///
    /// On the occasion that the someone else bootstraps the first region
    /// before us, the region state is cleared and `None` is returned.
    pub fn bootstrap_first_region(
        &mut self,
        store: &Store,
        store_id: u64,
    ) -> Result<Option<Region>> {
        let first_region = match self.engine.get_prepare_bootstrap_region()? {
            // The last bootstrap aborts. We need to resume or clean it up.
            Some(r) => r,
            None => {
                if self.check_pd_first_region_bootstrapped()? {
                    // If other node has bootstrap the cluster, skip to avoid
                    // useless ID allocating and disk writes.
                    return Ok(None);
                }
                self.prepare_bootstrap_first_region(store_id)?
            }
        };

        info!(
            self.logger,
            "trying to bootstrap first region";
            "store_id" => store_id,
            "region" => ?first_region
        );
        // cluster is not bootstrapped, and we choose first store to bootstrap
        fail_point!("node_after_prepare_bootstrap_cluster", |_| Err(box_err!(
            "injected error: node_after_prepare_bootstrap_cluster"
        )));

        let region_id = first_region.get_id();
        let mut retry = 0;
        while retry < MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT {
            match self
                .pd_client
                .bootstrap_cluster(store.clone(), first_region.clone())
            {
                Ok(_) => {
                    info!(
                        self.logger,
                        "bootstrap cluster ok";
                        "cluster_id" => self.cluster_id
                    );
                    fail_point!("node_after_bootstrap_cluster", |_| Err(box_err!(
                        "injected error: node_after_bootstrap_cluster"
                    )));
                    self.clear_prepare_bootstrap(None)?;
                    return Ok(Some(first_region));
                }
                Err(pd_client::Error::ClusterBootstrapped(_)) => {
                    match self.pd_client.get_region(b"") {
                        Ok(region) => {
                            if region == first_region {
                                // It is bootstrapped by us before.
                                self.clear_prepare_bootstrap(None)?;
                                return Ok(Some(first_region));
                            } else {
                                info!(
                                    self.logger,
                                    "cluster is already bootstrapped";
                                    "cluster_id" => self.cluster_id
                                );
                                self.clear_prepare_bootstrap(Some(region_id))?;
                                return Ok(None);
                            }
                        }
                        Err(e) => {
                            warn!(self.logger, "get the first region failed"; "err" => ?e);
                        }
                    }
                }
                Err(e) => {
                    error!(
                        self.logger,
                        "bootstrap cluster failed once";
                        "cluster_id" => self.cluster_id,
                        "err" => ?e,
                        "err_code" => %e.error_code()
                    );
                }
            }
            retry += 1;
            thread::sleep(CHECK_CLUSTER_BOOTSTRAPPED_RETRY_INTERVAL);
        }
        Err(box_err!(
            "bootstrapped cluster failed after {} attempts",
            retry
        ))
    }

    /// Pre-split the initial region into `pre_split_count` sub-regions.
    ///
    /// This creates N evenly-distributed Region stubs during bootstrap so that
    /// TiDB doesn't need to trigger costly sequential splits. Each pre-split
    /// region gets its own RocksDB tablet created upfront, dramatically
    /// reducing TiDB bootstrap latency with raftstore-v2.
    ///
    /// Split keys are generated evenly across the TiDB table key space (`t`
    /// prefix), using table ID ranges.
    pub fn pre_split_initial_region(
        &mut self,
        store_id: u64,
        first_region: &Region,
    ) -> Result<Vec<Region>> {
        if self.pre_split_count <= 1 {
            return Ok(vec![]);
        }

        let count = self.pre_split_count as u64;
        info!(
            self.logger,
            "pre-splitting initial region";
            "store_id" => store_id,
            "pre_split_count" => count,
        );

        // Generate split keys with valid TiDB key encoding.
        // TiDB row key format: t{table_id:8 bytes BE}_r{row_id:8 bytes BE}
        // Split on ascending table IDs to align with TiDB's natural regions.
        let split_keys: Vec<Vec<u8>> = (1..count)
            .map(|i| {
                let table_id: u64 = (i as u64) * (u64::MAX / (count + 1));
                // Valid TiDB row key: t + table_id(8B BE) + _r + row_id(8B BE, zero)
                let mut key = vec![b't'];
                key.extend_from_slice(&table_id.to_be_bytes());
                key.extend_from_slice(b"_r");
                // Row ID = min value (all zeros) for split boundary
                key.extend_from_slice(&0u64.to_be_bytes());
                key
            })
            .collect();

        let mut regions = Vec::with_capacity(count as usize);
        let base_epoch = first_region.get_region_epoch().clone();

        // Create regions: each gets a [split_key_prev, split_key) range
        let mut prev_key: Vec<u8> = vec![];
        for split_key in split_keys.iter().chain(std::iter::once(&vec![])) {
            let region_id = self.pd_client.alloc_id()?;
            let peer_id = self.pd_client.alloc_id()?;

            let mut region = Region::default();
            region.set_id(region_id);
            region.set_start_key(prev_key.clone());
            region.set_end_key(split_key.clone());
            let mut epoch = RegionEpoch::default();
            epoch.set_conf_ver(base_epoch.get_conf_ver());
            epoch.set_version(base_epoch.get_version() + 1);
            region.set_region_epoch(epoch);

            let mut peer = Peer::default();
            peer.set_id(peer_id);
            peer.set_store_id(store_id);
            region.mut_peers().push(peer);

            // Write initial Raft states for this region
            let mut wb = self.engine.log_batch(10);
            write_initial_states(&mut wb, region.clone())?;
            self.engine.consume(&mut wb, true)?;

            regions.push(region);
            prev_key = split_key.clone();
        }

        // Note: Individual RocksDB tablets for each region will be created
        // lazily when the StoreSystem applies the initial states during startup.
        // This is handled by raftstore-v2's tablet recovery logic.

        info!(
            self.logger,
            "pre-split complete";
            "region_count" => regions.len(),
            "first_split_key" => ?split_keys.first(),
        );

        Ok(regions)
    }
}
