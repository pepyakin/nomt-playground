use nomt::hasher::BinaryHasher;
use nomt::trie::KeyPath;
use nomt::{KeyReadWrite, Nomt, Options, Overlay, SessionParams, WitnessMode};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sha2::{Digest, digest};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tempfile::TempDir;

// Storage
#[derive(Clone)]
pub struct NomtSessionBuilder<H> {
    state_db: Arc<Nomt<BinaryHasher<H>>>,
    relevant_snapshot_refs: Vec<u64>,
    all_snapshots: Arc<RwLock<HashMap<u64, Overlay>>>,
}

impl<H> NomtSessionBuilder<H>
where
    H: digest::Digest<OutputSize = digest::typenum::U32> + Send + Sync,
{
    pub fn new(
        state_db: Arc<Nomt<BinaryHasher<H>>>,
        relevant_snapshot_refs: Vec<u64>,
        all_snapshots: Arc<RwLock<HashMap<u64, Overlay>>>,
    ) -> Self {
        Self {
            state_db,
            relevant_snapshot_refs,
            all_snapshots,
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn begin_session(&self) -> anyhow::Result<nomt::Session<BinaryHasher<H>>> {
        let start = std::time::Instant::now();
        let params = {
            let mut overlays = Vec::with_capacity(self.relevant_snapshot_refs.len());
            let snapshots = self.all_snapshots.read().expect("Snapshots lock poisoned");
            for overlay_ref in &self.relevant_snapshot_refs {
                let Some(state_overlay) = snapshots.get(overlay_ref) else {
                    tracing::debug!(
                        "Cannot find snapshot from reference, assuming it has been committed"
                    );
                    continue;
                };
                overlays.push(state_overlay);
            }
            SessionParams::default()
                .overlay(overlays)
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to construct session params for user session: {:?}",
                        e
                    )
                })?
                .witness_mode(WitnessMode::read_write())
        };
        let session = self.state_db.begin_session(params);
        let init_time = start.elapsed();
        let overlays = self.relevant_snapshot_refs.len();
        tracing::debug!(?init_time, overlays, "Session has been initialized");
        Ok(session)
    }
}

pub struct StorageManager<H> {
    _dir: TempDir,
    state_db: Arc<Nomt<BinaryHasher<H>>>,
    all_snapshots: Arc<RwLock<HashMap<u64, Overlay>>>,
    last_commited_key: u64,
    next_key: u64,
}

impl<H> StorageManager<H>
where
    H: digest::Digest<OutputSize = digest::typenum::U32> + Send + Sync,
{
    pub fn new(temp_in: Option<String>) -> Self {
        let dir = match temp_in {
            None => tempfile::tempdir().unwrap(),
            Some(parent) => tempfile::tempdir_in(&parent).unwrap(),
        };

        let mut opts = Options::new();
        opts.metrics(true);
        // Enable rollback, so we can handle errors with commits to 2 databases.
        opts.rollback(true);
        opts.max_rollback_log_len(1);
        opts.commit_concurrency(1);
        opts.hashtable_buckets(1_000_000);
        opts.preallocate_ht(false);
        opts.path(dir.path().join("kernel_nomt_db"));
        let nomt = Nomt::<BinaryHasher<H>>::open(opts).unwrap();
        Self {
            _dir: dir,
            state_db: Arc::new(nomt),
            all_snapshots: Arc::new(Default::default()),
            last_commited_key: 0,
            next_key: 1,
        }
    }

    pub fn crate_next_storage(&mut self) -> (u64, NomtSessionBuilder<H>) {
        tracing::debug!(
            self.last_commited_key,
            self.next_key,
            "Creating new storage"
        );

        let key = self.next_key;
        let storage = self.create_storage_for_existing_key(key);
        self.next_key += 1;
        (key, storage)
    }

    pub fn create_storage_for_existing_key(&self, key: u64) -> NomtSessionBuilder<H> {
        let refs = ((self.last_commited_key + 1)..key)
            .rev()
            .collect::<Vec<u64>>();
        tracing::debug!(?refs, "Creating storage");
        NomtSessionBuilder::new(self.state_db.clone(), refs, self.all_snapshots.clone())
    }

    #[tracing::instrument(skip(self, overlay))]
    pub fn save_change_set(&mut self, key: u64, overlay: Overlay) {
        let root = overlay.root();
        tracing::debug!(%root, "Saving change set");
        let mut snapshots = self.all_snapshots.write().unwrap();
        snapshots.insert(key, overlay);
        tracing::debug!(%root, "Saved change set");
    }

    #[tracing::instrument(skip(self))]
    pub fn finalize(&mut self, key: u64) {
        let start = std::time::Instant::now();
        tracing::debug!(self.last_commited_key, "Finalizing..");
        if key != (self.last_commited_key + 1) {
            panic!("Only sequential commit is allowed");
        }
        let mut snapshots = self.all_snapshots.write().unwrap();
        let overlay = snapshots
            .remove(&key)
            .expect("Attempt to commit non-existent key");
        let root = overlay.root();
        tracing::debug!(%root, "Commiting");
        overlay.commit(&self.state_db).expect("Failed to commit");

        self.last_commited_key = key;

        tracing::info!(self.last_commited_key, time = ?start.elapsed(), "Commited");
    }
}

pub struct RollupNode {
    storage_manager: StorageManager<sha2::Sha256>,
    storage_sender: tokio::sync::watch::Sender<NomtSessionBuilder<sha2::Sha256>>,
    // To keep the channel alive
    _storage_receiver: tokio::sync::watch::Receiver<NomtSessionBuilder<sha2::Sha256>>,
    data_receiver: std::sync::mpsc::Receiver<Vec<(KeyPath, KeyReadWrite)>>,
    finalization_probability: u8,
    rng: StdRng,
}

impl RollupNode {
    pub fn new(
        temp_in: Option<String>,
        fast_sequencers: usize,
        sleepy_sequencers: usize,
        finalization_probability: u8,
        seed: u64,
    ) -> Self {
        let mut storage_manager = StorageManager::new(temp_in);
        let (init_key, init_storage) = storage_manager.crate_next_storage();
        let (storage_sender, storage_receiver) = tokio::sync::watch::channel(init_storage.clone());
        let (data_sender, data_receiver) = std::sync::mpsc::channel();
        let mut rng_seed = seed;

        {
            let genesis_session = init_storage
                .begin_session()
                .expect("Failed to begin genesis session");
            let finished_session = genesis_session
                .finish(Vec::new())
                .expect("Failed to finish genesis session");
            let overlay = finished_session.into_overlay();
            storage_manager.save_change_set(init_key, overlay);
            storage_manager.finalize(init_key);
        }

        for _ in 0..fast_sequencers {
            let sequencer_rng = StdRng::seed_from_u64(rng_seed);
            rng_seed += 1;
            SequencerTask::spawn(
                storage_receiver.clone(),
                data_sender.clone(),
                false,
                sequencer_rng,
            );
        }
        for _ in 0..sleepy_sequencers {
            let sequencer_rng = StdRng::seed_from_u64(rng_seed);
            rng_seed += 1;
            SequencerTask::spawn(
                storage_receiver.clone(),
                data_sender.clone(),
                true,
                sequencer_rng,
            );
        }

        Self {
            storage_manager,
            storage_sender,
            _storage_receiver: storage_receiver,
            data_receiver,
            finalization_probability,
            rng: StdRng::seed_from_u64(rng_seed),
        }
    }

    /// Initial version.
    /// TODO:
    ///  - Extend reads/writes with different keys (existing, etc)
    ///     - Currently it tries to read some random key, but probability is low.
    pub fn run(mut self, blocks: usize) {
        for block_number in 0..blocks {
            tracing::info!(block_number, "Processing block");

            // Receive data from sequencers
            let data = match self.data_receiver.recv() {
                Ok(data) => data,
                Err(e) => {
                    tracing::error!("Failed to receive data from sequencers: {:?}", e);
                    break;
                }
            };

            let (key, storage) = self.storage_manager.crate_next_storage();
            let change_set = {
                let session = storage.begin_session().expect("Failed to start session");
                let prev_root = session.prev_root();
                tracing::debug!(num = data.len(), %prev_root, "Session has started with data from sequencer");
                tracing::info!("Finishing session");
                let finished_session = session.finish(data).unwrap();
                let next_root = finished_session.root();
                tracing::info!(block_number, %prev_root, %next_root, "Session is finished, converting into overlay");
                finished_session.into_overlay()
            };
            self.storage_manager.save_change_set(key, change_set);
            let new_storage = self
                .storage_manager
                .create_storage_for_existing_key(key + 1);
            self.storage_sender.send(new_storage).unwrap();
            let n = self.rng.random_range(0u8..=100);
            if n > self.finalization_probability {
                for k in self.storage_manager.last_commited_key + 1..=key {
                    self.storage_manager.finalize(k);
                }
            } else {
                tracing::debug!("Skipping finalization here");
            }
        }
        tracing::info!("Done");
    }
}

fn generate_random_writes(
    rng: &mut impl Rng,
    new: usize,
    probably_existing: usize,
) -> Vec<(KeyPath, Vec<u8>)> {
    let mut result = Vec::with_capacity(new);

    for _ in 0..new {
        let key: [u8; 32] = rng.random();
        let value: Vec<u8> = (0..32).map(|_| rng.random::<u8>()).collect();
        result.push((key.into(), value));
    }

    let mut existing_key = [0u8; 32];
    for _ in 0..probably_existing {
        existing_key[0] = rng.random::<u8>();
        existing_key[1] = rng.random::<u8>();
        let key: KeyPath = sha2::Sha256::digest(&existing_key).into();
        if result.iter().find(|(k, _)| k == &key).is_none() {
            let value: Vec<u8> = (0..32).map(|_| rng.random::<u8>()).collect();
            result.push((key, value));
        }
    }

    result
}

pub struct SequencerTask {
    storage_receiver: tokio::sync::watch::Receiver<NomtSessionBuilder<sha2::Sha256>>,
    data_sender: std::sync::mpsc::Sender<Vec<(KeyPath, KeyReadWrite)>>,
    with_strategic_sleeps: bool,
    rng: StdRng,
}

impl SequencerTask {
    pub fn spawn(
        storage_receiver: tokio::sync::watch::Receiver<NomtSessionBuilder<sha2::Sha256>>,
        data_sender: std::sync::mpsc::Sender<Vec<(KeyPath, KeyReadWrite)>>,
        with_strategic_sleeps: bool,
        rng: StdRng,
    ) {
        let task = Self {
            storage_receiver,
            data_sender,
            with_strategic_sleeps,
            rng,
        };

        std::thread::spawn(move || {
            task.run();
        });
    }

    fn run(mut self) {
        let seq_type = if self.with_strategic_sleeps {
            "sleepy"
        } else {
            "fast"
        };
        let _span = tracing::info_span!("seq-loop", seq_type = seq_type).entered();

        loop {
            if self.with_strategic_sleeps {
                let sleep_duration_1 = self.rng.random_range(0..=30);
                std::thread::sleep(std::time::Duration::from_millis(sleep_duration_1));
            }

            // Generate random keys
            let num_writes = self.rng.random_range(1..=2000);
            let num_reads = self.rng.random_range(1..=100);
            let raw_generated_data = generate_random_writes(&mut self.rng, num_writes, num_reads);

            // Get current storage from the receiver
            let storage = self.storage_receiver.borrow().clone();

            // Start session
            let session = match storage.begin_session() {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to begin session: {:?}", e);
                    continue;
                }
            };

            let mut data = Vec::with_capacity(raw_generated_data.len());

            for (key, raw_value) in raw_generated_data {
                let do_read = self.rng.random_bool(0.3);
                if do_read {
                    // In real sovereign rollup we never actually read from NOMT (all data is rocksdb)
                    let existing_data = session.read(key).unwrap();
                    let do_delete = self.rng.random_bool(0.1);
                    if do_delete {
                        // Deletion
                        data.push((key, KeyReadWrite::ReadThenWrite(existing_data, None)));
                    } else {
                        data.push((
                            key,
                            KeyReadWrite::ReadThenWrite(existing_data, Some(raw_value)),
                        ))
                    }
                } else {
                    data.push((key, KeyReadWrite::Write(Some(raw_value))))
                }
            }

            data.sort_by(|k1, k2| k1.0.cmp(&k2.0));

            if self.with_strategic_sleeps {
                let sleep_duration_2 = self.rng.random_range(0..=200);
                std::thread::sleep(std::time::Duration::from_millis(sleep_duration_2));
            }

            // Finish session
            let finished_session = match session.finish(data.clone()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to finish session: {:?}", e);
                    continue;
                }
            };

            // Print root
            let root = finished_session.root();
            tracing::info!(%root, "Sequencer session finished");

            // Submit data to sender channel
            if self.data_sender.send(data).is_err() {
                tracing::info!("Data channel closed, stopping sequencer task");
                break;
            }
        }
    }
}
