//! Micro-benchmark comparing two ways of answering "which replicas of this
//! tablet are in datacenter X?", measured under Valgrind via gungraun.
//!
//! The driver keeps, for every tablet, its replica list as `Vec<(Arc<Node>, Shard)>`
//! and answers per-datacenter queries from the load balancing policy. Two
//! representations are compared:
//!
//! - **`PerDcMap`**: additionally keep a `HashMap<String, Vec<(Arc<Node>, Shard)>>`
//!   grouping the replicas by datacenter, and answer a query with one map lookup.
//!   This is what the driver does today.
//! - **`Filter`**: keep only the full list and answer a query by filtering it on
//!   `node.datacenter == dc`, which for a replica list of a few entries is a few
//!   short string comparisons.
//! - **`DcIndex`**: keep the list plus a tiny index: for each datacenter among
//!   the replicas, its (interned) name and a bitmask of the replicas' positions.
//!   A query scans the index (a few name comparisons, no `Node` dereferences)
//!   and follows the mask. A scheme with topology-wide datacenter ids would cost
//!   the same on the query side, since the query arrives as a name.
//!
//! Every representation is exercised in three ways, each mirroring a real
//! operation on tablet metadata:
//!
//! - `build`: constructing it from a resolved replica list (learning a tablet),
//! - `clone`: cloning it (what cloning the cluster state does per tablet),
//! - `lookup`: the hot path - for a stream of (tablet, datacenter) queries,
//!   iterate the datacenter's replicas, take only the first of them (`first`),
//!   and separately `choose` one of them by index the way `ReplicaSet::choose`
//!   does (a `len()` followed by an `nth()`). For `Filter` a single-pass
//!   `choose` is measured as well.
//!
//! The data is realistic rather than minimal: datacenter names are of the
//! cloud-region kind (`us-east-1`), the replica list holds `RF` replicas per
//! datacenter, and the queried datacenter name is a separate allocation from
//! the one stored in the nodes, as it is in the driver (it comes from the
//! policy's configuration).
//!
//! Two replica orders are measured, because filtering is sensitive to it while
//! the map is not: `interleaved` (datacenters alternate, queries spread over all
//! datacenters) and `grouped` - the worst case for filtering - where each
//! datacenter's replicas are contiguous and every query is for the *last*
//! datacenter, as for a client whose local datacenter comes last in every list.
//!
//! This benchmark needs no cluster. Run it with
//! `cargo bench -p benchmarks --bench dc_replicas`.

#![allow(missing_docs)]

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use gungraun::Dhat;
use gungraun::prelude::*;
use smallvec::SmallVec;

type Shard = u32;

/// Stand-in for the driver's `Node`: only the fields the comparison touches.
/// Like in the driver, the datacenter is an owned `String` inside an `Arc`ed
/// node, so filtering has to dereference the `Arc` and compare strings.
struct Node {
    datacenter: Option<String>,
    #[expect(dead_code)] // Realistic size of the node; never read.
    rack: Option<String>,
}

// Of equal length on purpose: a mismatching comparison then has to look at the
// bytes, as it does for `dc1`/`dc2` or `us-east-1`/`us-west-2`, instead of
// failing on the length alone.
const DATACENTERS: [&str; 3] = ["us-east-1", "eu-west-1", "ap-east-1"];

/// Number of tablets in the fixture: enough that the lookups do not all hit the
/// same tablet.
const TABLETS: usize = 64;

/// Number of lookups per measured `lookup` run.
const LOOKUPS: usize = 4096;

/// Datacenter-grouped replicas, as the driver keeps them today.
#[derive(Clone)]
struct PerDcMap {
    // Only datacenter queries are benchmarked, but the full list is part of the
    // representation and of what `build` and `clone` cost, so it is kept.
    #[expect(dead_code)]
    all: Vec<(Arc<Node>, Shard)>,
    per_dc: HashMap<String, Vec<(Arc<Node>, Shard)>>,
}

impl PerDcMap {
    fn build(all: Vec<(Arc<Node>, Shard)>) -> Self {
        let mut per_dc: HashMap<String, Vec<(Arc<Node>, Shard)>> = HashMap::new();
        for (node, shard) in all.iter() {
            if let Some(dc) = node.datacenter.as_ref() {
                if let Some(replicas) = per_dc.get_mut(dc) {
                    replicas.push((Arc::clone(node), *shard));
                } else {
                    per_dc.insert(dc.to_string(), vec![(Arc::clone(node), *shard)]);
                }
            }
        }
        Self { all, per_dc }
    }

    fn dc_replicas(&self, dc: &str) -> &[(Arc<Node>, Shard)] {
        self.per_dc.get(dc).map(Vec::as_slice).unwrap_or(&[])
    }

    fn iter_dc(&self, dc: &str) -> impl Iterator<Item = (&Arc<Node>, Shard)> {
        self.dc_replicas(dc).iter().map(|(n, s)| (n, *s))
    }

    fn choose_dc(&self, dc: &str, index: usize) -> Option<(&Arc<Node>, Shard)> {
        let replicas = self.dc_replicas(dc);
        let len = replicas.len();
        if len == 0 {
            return None;
        }
        replicas.get(index % len).map(|(n, s)| (n, *s))
    }
}

/// Replica list only; datacenter queries filter it.
#[derive(Clone)]
struct Filter {
    all: Vec<(Arc<Node>, Shard)>,
}

impl Filter {
    fn build(all: Vec<(Arc<Node>, Shard)>) -> Self {
        Self { all }
    }

    fn iter_dc<'a, 'd>(
        &'a self,
        dc: &'d str,
    ) -> impl Iterator<Item = (&'a Arc<Node>, Shard)> + use<'a, 'd> {
        self.all
            .iter()
            .filter(move |(n, _)| n.datacenter.as_deref() == Some(dc))
            .map(|(n, s)| (n, *s))
    }

    /// Two passes, like `ReplicaSet::choose` does for datacenter-filtered
    /// vnode replicas today: count the matches, then take the n-th.
    fn choose_dc(&self, dc: &str, index: usize) -> Option<(&Arc<Node>, Shard)> {
        let len = self.iter_dc(dc).count();
        if len == 0 {
            return None;
        }
        self.iter_dc(dc).nth(index % len)
    }

    /// Single pass: collects the indices of the datacenter's replicas into a
    /// stack buffer, then picks one. Falls back to the two-pass version for
    /// lists too long for the buffer.
    fn choose_dc_single_pass(&self, dc: &str, index: usize) -> Option<(&Arc<Node>, Shard)> {
        let mut matching = [0u8; 16];
        let mut count = 0usize;
        if self.all.len() > usize::from(u8::MAX) {
            return self.choose_dc(dc, index);
        }
        for (i, (n, _)) in self.all.iter().enumerate() {
            if n.datacenter.as_deref() == Some(dc) {
                if count == matching.len() {
                    return self.choose_dc(dc, index);
                }
                matching[count] = i as u8;
                count += 1;
            }
        }
        if count == 0 {
            return None;
        }
        let (n, s) = &self.all[usize::from(matching[index % count])];
        Some((n, *s))
    }
}

/// Replica list plus a per-datacenter index. Interned datacenter names, so that
/// the index costs no string allocation per tablet. Lists are assumed to hold
/// at most 16 replicas (the mask width); the driver would need a fallback.
#[derive(Clone)]
struct DcIndex {
    all: Vec<(Arc<Node>, Shard)>,
    index: SmallVec<[(Arc<str>, u16); 3]>,
}

impl DcIndex {
    fn build(all: Vec<(Arc<Node>, Shard)>, interner: &mut Vec<Arc<str>>) -> Self {
        assert!(all.len() <= 16);
        let mut index: SmallVec<[(Arc<str>, u16); 3]> = SmallVec::new();
        for (i, (node, _)) in all.iter().enumerate() {
            let Some(dc) = node.datacenter.as_deref() else {
                continue;
            };
            if let Some((_, mask)) = index.iter_mut().find(|(name, _)| &**name == dc) {
                *mask |= 1 << i;
            } else {
                let name = match interner.iter().find(|name| &***name == dc) {
                    Some(name) => Arc::clone(name),
                    None => {
                        let name: Arc<str> = Arc::from(dc);
                        interner.push(Arc::clone(&name));
                        name
                    }
                };
                index.push((name, 1 << i));
            }
        }
        Self { all, index }
    }

    fn mask(&self, dc: &str) -> u16 {
        self.index
            .iter()
            .find(|(name, _)| &**name == dc)
            .map_or(0, |(_, mask)| *mask)
    }

    fn iter_dc<'a, 'd>(
        &'a self,
        dc: &'d str,
    ) -> impl Iterator<Item = (&'a Arc<Node>, Shard)> + use<'a, 'd> {
        let mut mask = self.mask(dc);
        std::iter::from_fn(move || {
            if mask == 0 {
                return None;
            }
            let i = mask.trailing_zeros() as usize;
            mask &= mask - 1;
            let (node, shard) = &self.all[i];
            Some((node, *shard))
        })
    }

    fn choose_dc(&self, dc: &str, index: usize) -> Option<(&Arc<Node>, Shard)> {
        let mut mask = self.mask(dc);
        let len = mask.count_ones() as usize;
        if len == 0 {
            return None;
        }
        for _ in 0..index % len {
            mask &= mask - 1;
        }
        let (node, shard) = &self.all[mask.trailing_zeros() as usize];
        Some((node, *shard))
    }
}

/// Shape of the replica lists: `dcs` datacenters with `rf` replicas in each.
#[derive(Clone, Copy)]
struct Shape {
    dcs: usize,
    rf: usize,
    /// Whether each datacenter's replicas are contiguous in the list (and
    /// every query is for the last datacenter), rather than interleaved.
    grouped: bool,
}

/// The nodes of a cluster with `shape.dcs` datacenters, `2 * shape.rf` nodes
/// each (so a tablet's replicas are a proper subset of the datacenter).
fn make_nodes(shape: Shape) -> Vec<Arc<Node>> {
    (0..shape.dcs)
        .flat_map(|dc| {
            (0..2 * shape.rf).map(move |i| {
                Arc::new(Node {
                    datacenter: Some(DATACENTERS[dc].to_string()),
                    rack: Some(format!("rack{}", i % 3)),
                })
            })
        })
        .collect()
}

/// Resolved replica lists of `TABLETS` tablets, `shape.rf` replicas in each
/// datacenter, interleaved across datacenters the way a real payload lists them.
/// The node choice is a fixed pseudo-random sequence, so it is the same in every
/// run.
fn make_replica_lists(shape: Shape, nodes: &[Arc<Node>]) -> Vec<Vec<(Arc<Node>, Shard)>> {
    let mut rng = Lcg(0x9E37_79B9_7F4A_7C15);
    let per_dc = 2 * shape.rf;
    (0..TABLETS)
        .map(|_| {
            let mut replicas = Vec::with_capacity(shape.dcs * shape.rf);
            let mut push = |dc: usize, r: usize| {
                // Distinct nodes within a datacenter: offset by `r`, stride
                // through the datacenter's nodes.
                let idx = dc * per_dc + (rng.next() as usize + r) % per_dc;
                replicas.push((Arc::clone(&nodes[idx]), rng.next() as Shard % 8));
            };
            if shape.grouped {
                for dc in 0..shape.dcs {
                    for r in 0..shape.rf {
                        push(dc, r);
                    }
                }
            } else {
                for r in 0..shape.rf {
                    for dc in 0..shape.dcs {
                        push(dc, r);
                    }
                }
            }
            replicas
        })
        .collect()
}

/// The (tablet index, datacenter, chosen index) of every lookup. Owned `String`s
/// for the datacenter, distinct from the ones inside the nodes, as in the driver.
fn make_queries(shape: Shape) -> Vec<(usize, String, usize)> {
    let mut rng = Lcg(0x2545_F491_4F6C_DD1D);
    (0..LOOKUPS)
        .map(|_| {
            let dc = if shape.grouped {
                shape.dcs - 1
            } else {
                rng.next() as usize % shape.dcs
            };
            (
                rng.next() as usize % TABLETS,
                DATACENTERS[dc].to_string(),
                rng.next() as usize,
            )
        })
        .collect()
}

/// Minimal deterministic generator (no `rand` dependency, no seeding subtleties).
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

// ---- build ----------------------------------------------------------------

type BuildState = Vec<Vec<(Arc<Node>, Shard)>>;

fn setup_build(dcs: usize, rf: usize) -> BuildState {
    let shape = Shape {
        dcs,
        rf,
        grouped: false,
    };
    make_replica_lists(shape, &make_nodes(shape))
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_build)]
fn build_per_dc_map(lists: BuildState) -> Vec<PerDcMap> {
    lists.into_iter().map(PerDcMap::build).collect()
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_build)]
fn build_filter(lists: BuildState) -> Vec<Filter> {
    lists.into_iter().map(Filter::build).collect()
}

// Includes interning the datacenter names (one allocation per datacenter, not
// per tablet).
#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_build)]
fn build_dc_index(lists: BuildState) -> (Vec<DcIndex>, Vec<Arc<str>>) {
    let mut interner = Vec::new();
    let tablets = lists
        .into_iter()
        .map(|list| DcIndex::build(list, &mut interner))
        .collect();
    (tablets, interner)
}

// ---- clone ----------------------------------------------------------------

fn setup_clone_per_dc_map(dcs: usize, rf: usize) -> Vec<PerDcMap> {
    setup_build(dcs, rf)
        .into_iter()
        .map(PerDcMap::build)
        .collect()
}

fn setup_clone_filter(dcs: usize, rf: usize) -> Vec<Filter> {
    setup_build(dcs, rf)
        .into_iter()
        .map(Filter::build)
        .collect()
}

fn setup_clone_dc_index(dcs: usize, rf: usize) -> Vec<DcIndex> {
    let mut interner = Vec::new();
    setup_build(dcs, rf)
        .into_iter()
        .map(|list| DcIndex::build(list, &mut interner))
        .collect()
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_clone_per_dc_map)]
fn clone_per_dc_map(tablets: Vec<PerDcMap>) -> (Vec<PerDcMap>, Vec<PerDcMap>) {
    let cloned = tablets.clone();
    (tablets, cloned)
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_clone_filter)]
fn clone_filter(tablets: Vec<Filter>) -> (Vec<Filter>, Vec<Filter>) {
    let cloned = tablets.clone();
    (tablets, cloned)
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_clone_dc_index)]
fn clone_dc_index(tablets: Vec<DcIndex>) -> (Vec<DcIndex>, Vec<DcIndex>) {
    let cloned = tablets.clone();
    (tablets, cloned)
}

// ---- lookup ---------------------------------------------------------------

type LookupState<T> = (Vec<T>, Vec<(usize, String, usize)>);

fn setup_lookup<T>(
    dcs: usize,
    rf: usize,
    grouped: bool,
    build: impl FnMut(Vec<(Arc<Node>, Shard)>) -> T,
) -> LookupState<T> {
    let shape = Shape { dcs, rf, grouped };
    let tablets = make_replica_lists(shape, &make_nodes(shape))
        .into_iter()
        .map(build)
        .collect();
    (tablets, make_queries(shape))
}

fn setup_lookup_per_dc_map(dcs: usize, rf: usize) -> LookupState<PerDcMap> {
    setup_lookup(dcs, rf, false, PerDcMap::build)
}

fn setup_lookup_filter(dcs: usize, rf: usize) -> LookupState<Filter> {
    setup_lookup(dcs, rf, false, Filter::build)
}

fn setup_lookup_filter_grouped(dcs: usize, rf: usize) -> LookupState<Filter> {
    setup_lookup(dcs, rf, true, Filter::build)
}

fn setup_lookup_dc_index(dcs: usize, rf: usize) -> LookupState<DcIndex> {
    let mut interner = Vec::new();
    setup_lookup(dcs, rf, false, |list| DcIndex::build(list, &mut interner))
}

// Iterates every replica of the queried datacenter, as the policy does when
// building a query plan.
#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_lookup_per_dc_map)]
fn iter_per_dc_map(state: LookupState<PerDcMap>) -> LookupState<PerDcMap> {
    let (tablets, queries) = &state;
    let mut visited = 0usize;
    for (tablet, dc, _) in queries {
        for (node, shard) in tablets[*tablet].iter_dc(dc) {
            black_box((node, shard));
            visited += 1;
        }
    }
    black_box(visited);
    state
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_lookup_filter)]
fn iter_filter(state: LookupState<Filter>) -> LookupState<Filter> {
    let (tablets, queries) = &state;
    let mut visited = 0usize;
    for (tablet, dc, _) in queries {
        for (node, shard) in tablets[*tablet].iter_dc(dc) {
            black_box((node, shard));
            visited += 1;
        }
    }
    black_box(visited);
    state
}

// Picks one replica of the queried datacenter by index (`len()` then `nth()`),
// as the policy does when picking the first replica to try.
#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_lookup_per_dc_map)]
fn choose_per_dc_map(state: LookupState<PerDcMap>) -> LookupState<PerDcMap> {
    let (tablets, queries) = &state;
    for (tablet, dc, index) in queries {
        black_box(tablets[*tablet].choose_dc(dc, *index));
    }
    state
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_lookup_filter)]
fn choose_filter(state: LookupState<Filter>) -> LookupState<Filter> {
    let (tablets, queries) = &state;
    for (tablet, dc, index) in queries {
        black_box(tablets[*tablet].choose_dc(dc, *index));
    }
    state
}

#[library_benchmark]
#[benches::shapes(args = [(2, 3), (3, 3)], setup = setup_lookup_filter_grouped)]
fn iter_filter_grouped(state: LookupState<Filter>) -> LookupState<Filter> {
    let (tablets, queries) = &state;
    let mut visited = 0usize;
    for (tablet, dc, _) in queries {
        for (node, shard) in tablets[*tablet].iter_dc(dc) {
            black_box((node, shard));
            visited += 1;
        }
    }
    black_box(visited);
    state
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_lookup_dc_index)]
fn iter_dc_index(state: LookupState<DcIndex>) -> LookupState<DcIndex> {
    let (tablets, queries) = &state;
    let mut visited = 0usize;
    for (tablet, dc, _) in queries {
        for (node, shard) in tablets[*tablet].iter_dc(dc) {
            black_box((node, shard));
            visited += 1;
        }
    }
    black_box(visited);
    state
}

// Takes only the first replica of the queried datacenter, as the policy does
// for deterministic (LWT) routing and when starting a fallback iteration. This
// is the operation whose cost depends on where the datacenter's replicas are.
#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_lookup_per_dc_map)]
fn first_per_dc_map(state: LookupState<PerDcMap>) -> LookupState<PerDcMap> {
    let (tablets, queries) = &state;
    for (tablet, dc, _) in queries {
        black_box(tablets[*tablet].iter_dc(dc).next());
    }
    state
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_lookup_filter)]
fn first_filter(state: LookupState<Filter>) -> LookupState<Filter> {
    let (tablets, queries) = &state;
    for (tablet, dc, _) in queries {
        black_box(tablets[*tablet].iter_dc(dc).next());
    }
    state
}

#[library_benchmark]
#[benches::shapes(args = [(2, 3), (3, 3)], setup = setup_lookup_filter_grouped)]
fn first_filter_grouped(state: LookupState<Filter>) -> LookupState<Filter> {
    let (tablets, queries) = &state;
    for (tablet, dc, _) in queries {
        black_box(tablets[*tablet].iter_dc(dc).next());
    }
    state
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_lookup_dc_index)]
fn first_dc_index(state: LookupState<DcIndex>) -> LookupState<DcIndex> {
    let (tablets, queries) = &state;
    for (tablet, dc, _) in queries {
        black_box(tablets[*tablet].iter_dc(dc).next());
    }
    state
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_lookup_dc_index)]
fn choose_dc_index(state: LookupState<DcIndex>) -> LookupState<DcIndex> {
    let (tablets, queries) = &state;
    for (tablet, dc, index) in queries {
        black_box(tablets[*tablet].choose_dc(dc, *index));
    }
    state
}

#[library_benchmark]
#[benches::shapes(args = [(1, 3), (2, 3), (3, 3)], setup = setup_lookup_filter)]
fn choose_filter_single_pass(state: LookupState<Filter>) -> LookupState<Filter> {
    let (tablets, queries) = &state;
    for (tablet, dc, index) in queries {
        black_box(tablets[*tablet].choose_dc_single_pass(dc, *index));
    }
    state
}

library_benchmark_group!(
    name = dc_replicas;
    benchmarks =
        build_per_dc_map,
        build_filter,
        build_dc_index,
        clone_per_dc_map,
        clone_filter,
        clone_dc_index,
        iter_per_dc_map,
        iter_filter,
        iter_filter_grouped,
        iter_dc_index,
        first_per_dc_map,
        first_filter,
        first_filter_grouped,
        first_dc_index,
        choose_per_dc_map,
        choose_filter,
        choose_filter_single_pass,
        choose_dc_index
);

main!(
    config = LibraryBenchmarkConfig::default()
        .tool(Dhat::default())
        .valgrind_args(["--trace-children=no", "--num-callers=500"]);
    library_benchmark_groups = dc_replicas
);
