use crate::aggregator::Aggregator;
use crate::config::{Committee, Parameters, Stake};
use crate::error::{ConsensusError, ConsensusResult};
use crate::filter::FilterInput;
use crate::leader::LeaderElector;
use crate::mempool::MempoolDriver;
use crate::messages::{
    ABAOutput, ABAVal, Block, HVote, MDone, MHalt, MPreVote, MVote, MVoteTag, PrePare,
    PrePareProof, PreVoteTag, RandomnessShare, SPBProof, SPBValue, SPBVote, QC,
};
use crate::synchronizer::Synchronizer;
use async_recursion::async_recursion;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::{HashMap, HashSet, VecDeque};
use store::Store;
use threshold_crypto::PublicKeySet;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration};
#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

#[cfg(test)]
#[path = "tests/smvba_tests.rs"]
pub mod smvba_tests;

pub type SeqNumber = u64; // For both round and view

pub const OPT: u8 = 0;
pub const PES: u8 = 1;

pub const VAL_PHASE: u8 = 0;
pub const MUX_PHASE: u8 = 1;

pub const INIT_PHASE: u8 = 0;
pub const LOCK_PHASE: u8 = 1;
pub const FIN_PHASE: u8 = 2;

#[derive(Serialize, Deserialize, Debug)]
pub enum ConsensusMessage {
    HsPropose(Block),
    HSVote(HVote),
    HsLoopBack(Block),
    SyncRequest(Digest, PublicKey),
    SyncReply(Block),
    SPBPropose(SPBValue, SPBProof),
    SPBVote(SPBVote),
    SPBFinsh(SPBValue, SPBProof),
    SPBDone(MDone),
    SMVBAPreVote(MPreVote),
    SMVBAVote(MVote),
    SMVBACoinShare(RandomnessShare), //elect leader
    SMVBAHalt(MHalt),                //mvba halt
    ParPrePare(PrePare),
    ParABAVal(ABAVal),
    ParABACoinShare(RandomnessShare),
    ParABAOutput(ABAOutput),
    ParLoopBack(Block),
    DelayProPose(SeqNumber, SeqNumber, QC),
}

pub struct Core {
    name: PublicKey,
    committee: Committee,
    parameters: Parameters,
    store: Store,
    signature_service: SignatureService,
    pk_set: PublicKeySet,
    leader_elector: LeaderElector,
    mempool_driver: MempoolDriver,
    synchronizer: Synchronizer,
    core_channel: Receiver<ConsensusMessage>,
    tx_core: Sender<ConsensusMessage>,
    smvba_channel: Receiver<ConsensusMessage>,
    tx_smvba: Sender<ConsensusMessage>,
    network_filter: Sender<FilterInput>,
    network_filter_smvba: Sender<FilterInput>,
    commit_channel: Sender<Block>,
    height: SeqNumber, // current height
    epoch: SeqNumber,  // current epoch
    last_voted_height: SeqNumber,
    last_committed_height: SeqNumber,
    unhandle_message: VecDeque<(SeqNumber, ConsensusMessage)>,
    high_qc: QC,
    aggregator: Aggregator,
    opt_path: bool,
    pes_path: bool,
    smvba_y_flag: HashMap<(SeqNumber, SeqNumber), bool>,
    smvba_n_flag: HashMap<(SeqNumber, SeqNumber), bool>,
    smvba_d_flag: HashMap<(SeqNumber, SeqNumber), bool>, //2f+1 个finish？
    spb_proposes: HashMap<(SeqNumber, SeqNumber), SPBValue>,
    spb_finishs: HashMap<(SeqNumber, SeqNumber), HashMap<PublicKey, (SPBValue, SPBProof)>>,
    spb_locks: HashMap<(SeqNumber, SeqNumber), HashMap<PublicKey, (SPBValue, SPBProof)>>,
    spb_current_phase: HashMap<(SeqNumber, SeqNumber), u8>,
    spb_abandon_flag: HashMap<(SeqNumber, SeqNumber), bool>,
    smvba_halt_falg: HashMap<(SeqNumber, SeqNumber), bool>,
    smvba_dones: HashMap<(SeqNumber, SeqNumber), HashSet<PublicKey>>,
    smvba_current_round: HashMap<SeqNumber, SeqNumber>, // height->round
    // smvba_yes_votes: HashMap<(SeqNumber, SeqNumber), Vec<MVote>>,
    smvba_votes: HashMap<(SeqNumber, SeqNumber), HashSet<PublicKey>>,
    smvba_no_prevotes: HashMap<(SeqNumber, SeqNumber), HashSet<PublicKey>>,
    prepare_tag: HashSet<SeqNumber>, //标记 height高度的 tag是否已经发送
    par_prepare_opts: HashMap<SeqNumber, HashSet<PublicKey>>,
    par_prepare_pess: HashMap<SeqNumber, HashSet<PublicKey>>,
    par_values: HashMap<SeqNumber, [Option<Block>; 2]>,
    aba_vals_used: HashMap<(SeqNumber, SeqNumber, u8), HashSet<PublicKey>>,
    aba_temp_vals: HashMap<(SeqNumber, SeqNumber, u8), [Stake; 2]>,
    aba_bin_vals: HashMap<(SeqNumber, SeqNumber), [bool; 2]>,
    aba_mux_vals: HashMap<(SeqNumber, SeqNumber), [bool; 2]>,
    aba_current_round: HashMap<SeqNumber, SeqNumber>,
    aba_output: HashMap<SeqNumber, Option<usize>>,
    aba_output_messages: HashMap<SeqNumber, HashSet<PublicKey>>,
    par_value_wait: HashMap<SeqNumber, [bool; 2]>,
    smvba_is_invoke: HashMap<SeqNumber, bool>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        pk_set: PublicKeySet,
        store: Store,
        leader_elector: LeaderElector,
        mempool_driver: MempoolDriver,
        synchronizer: Synchronizer,
        core_channel: Receiver<ConsensusMessage>,
        tx_core: Sender<ConsensusMessage>,
        smvba_channel: Receiver<ConsensusMessage>,
        tx_smvba: Sender<ConsensusMessage>,
        network_filter: Sender<FilterInput>,
        network_filter_smvba: Sender<FilterInput>,
        commit_channel: Sender<Block>,
        opt_path: bool,
        pes_path: bool,
    ) -> Self {
        let aggregator = Aggregator::new(committee.clone());
        let mut core = Self {
            name,
            committee,
            parameters,
            signature_service,
            store,
            pk_set,
            leader_elector,
            mempool_driver,
            synchronizer,
            network_filter,
            network_filter_smvba,
            commit_channel,
            core_channel,
            tx_core,
            smvba_channel,
            tx_smvba,
            height: 1,
            epoch: 0,
            last_voted_height: 0,
            last_committed_height: 0,
            unhandle_message: VecDeque::new(),
            high_qc: QC::genesis(),
            aggregator,
            opt_path,
            pes_path,
            smvba_y_flag: HashMap::new(),
            smvba_n_flag: HashMap::new(),
            smvba_d_flag: HashMap::new(),
            spb_proposes: HashMap::new(),
            spb_finishs: HashMap::new(),
            spb_locks: HashMap::new(),
            spb_current_phase: HashMap::new(),
            spb_abandon_flag: HashMap::new(),
            smvba_halt_falg: HashMap::new(),
            smvba_current_round: HashMap::new(),
            smvba_dones: HashMap::new(),
            // smvba_yes_votes: HashMap::new(),
            smvba_votes: HashMap::new(),
            smvba_no_prevotes: HashMap::new(),
            prepare_tag: HashSet::new(),
            par_prepare_opts: HashMap::new(),
            par_prepare_pess: HashMap::new(),
            par_values: HashMap::new(),
            aba_vals_used: HashMap::new(),
            aba_temp_vals: HashMap::new(),
            aba_current_round: HashMap::new(),
            aba_output: HashMap::new(),
            aba_bin_vals: HashMap::new(),
            aba_mux_vals: HashMap::new(),
            aba_output_messages: HashMap::new(),
            par_value_wait: HashMap::new(),
            smvba_is_invoke: HashMap::new(),
        };
        core.update_smvba_state(1, 1);
        core.update_hs_state(1);
        return core;
    }

    //initlization epoch
    fn epoch_init(&mut self, epoch: u64) {
        //清除之前的消息
        self.leader_elector = LeaderElector::new(self.committee.clone());
        self.aggregator = Aggregator::new(self.committee.clone());
        self.height = 1;
        self.epoch = epoch;
        self.high_qc = QC::genesis();
        self.last_voted_height = 0;
        self.last_committed_height = 0;
        self.spb_proposes = HashMap::new();
        self.spb_finishs = HashMap::new();
        self.smvba_current_round = HashMap::new();
        // self.smvba_yes_votes = HashMap::new();
        self.smvba_votes = HashMap::new();
        self.smvba_no_prevotes = HashMap::new();
        self.prepare_tag = HashSet::new();
        self.smvba_y_flag = HashMap::new();
        self.smvba_n_flag = HashMap::new();
        self.smvba_d_flag = HashMap::new();
        self.spb_current_phase = HashMap::new();
        self.smvba_dones = HashMap::new();
        self.par_prepare_opts = HashMap::new();
        self.par_prepare_pess = HashMap::new();
        self.par_values = HashMap::new();
        self.aba_vals_used = HashMap::new();
        self.aba_temp_vals = HashMap::new();
        self.aba_current_round = HashMap::new();
        self.aba_output = HashMap::new();
        self.aba_bin_vals = HashMap::new();
        self.aba_mux_vals = HashMap::new();
        self.aba_output_messages = HashMap::new();
        self.par_value_wait = HashMap::new();
        self.spb_abandon_flag = HashMap::new();
        self.smvba_halt_falg = HashMap::new();
        self.smvba_is_invoke = HashMap::new();
        self.update_smvba_state(1, 1);
        self.update_hs_state(1);
    }

    fn update_smvba_state(&mut self, height: SeqNumber, round: SeqNumber) {
        //每人都从第一轮开始
        self.smvba_current_round.insert(height, round);
        self.smvba_d_flag.insert((height, round), false);
        self.smvba_y_flag.insert((height, round), false);
        self.smvba_n_flag.insert((height, round), false);
        self.spb_current_phase.insert((height, round), INIT_PHASE);
        self.smvba_dones.insert((height, round), HashSet::new());
        self.smvba_no_prevotes
            .insert((height, round), HashSet::new());
        self.smvba_votes.insert((height, round), HashSet::new());
    }

    fn update_hs_state(&mut self, height: SeqNumber) {
        self.par_prepare_opts.insert(height, HashSet::new());
        self.par_prepare_pess.insert(height, HashSet::new());
        self.aba_current_round.insert(height, 1);
        self.aba_output.insert(height, None);
        self.aba_bin_vals.insert((height, 1), [false, false]);
        self.aba_mux_vals.insert((height, 1), [false, false]);
        self.aba_current_round.insert(height, 1);
        self.aba_output.insert(height, None);
        self.aba_output_messages.insert(height, HashSet::new());
        self.par_value_wait.insert(height, [false, false]);
    }

    fn clean_smvba_state(&mut self, height: &SeqNumber) {
        self.smvba_current_round.retain(|h, _| h > height);
        self.spb_proposes.retain(|(h, _), _| h > height);
        self.spb_finishs.retain(|(h, _), _| h > height);
        self.spb_locks.retain(|(h, _), _| h > height);
        // self.smvba_yes_votes.retain(|(h, _), _| h >= height);
        self.smvba_votes.retain(|(h, _), _| h > height);
        self.smvba_no_prevotes.retain(|(h, _), _| h > height);
        self.aggregator.cleanup_mvba_random(height);
        self.aggregator.cleanup_spb_vote(height);

        //
        self.par_prepare_opts.retain(|h, _| h > height);
        self.par_prepare_pess.retain(|h, _| h > height);
        self.aba_current_round.retain(|h, _| h > height);
        self.aba_output.retain(|h, _| h > height);
        self.aba_bin_vals.retain(|(h, _), _| h > height);
        self.aba_mux_vals.retain(|(h, _), _| h > height);
        self.aba_current_round.retain(|h, _| h > height);
        self.aba_output.retain(|h, _| h > height);
        self.aba_output_messages.retain(|h, _| h > height);
        self.par_value_wait.retain(|h, _| h > height);
    }

    async fn store_block(&mut self, block: &Block) {
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store.write(key, value).await;
    }

    async fn parbft2_smvba_delay(
        time_out: SeqNumber,
        epoch: SeqNumber,
        height: SeqNumber,
        qc: QC,
    ) -> (SeqNumber, SeqNumber, QC) {
        sleep(Duration::from_millis(time_out)).await;
        (epoch, height, qc)
    }

    // -- Start Safety Module --
    fn increase_last_voted_round(&mut self, target: SeqNumber) {
        self.last_voted_height = max(self.last_voted_height, target);
    }

    async fn make_spb_vote(&mut self, value: &SPBValue) -> Option<SPBVote> {
        //有效性规则由其他过程完成
        if value.phase > LOCK_PHASE {
            return None;
        }
        Some(SPBVote::new(value.clone(), self.name, self.signature_service.clone()).await)
    }

    async fn make_opt_vote(&mut self, block: &Block) -> Option<HVote> {
        // Check if we can vote for this block.
        let safety_rule_1 = block.height > self.last_voted_height;
        let safety_rule_2 = block.qc.height + 1 == block.height;

        if !(safety_rule_1 && safety_rule_2) {
            return None;
        }

        // Ensure we won't vote for contradicting blocks.
        self.increase_last_voted_round(block.height);
        // TODO [issue #15]: Write to storage preferred_round and last_voted_round.
        Some(HVote::new(&block, self.name, self.signature_service.clone()).await)
    }

    #[async_recursion]
    async fn commit(&mut self, block: &Block) -> ConsensusResult<()> {
        let mut current_block = block.clone();
        while current_block.height > self.last_committed_height {
            if !current_block.payload.is_empty() {
                info!(
                    "Committed {} epoch {} tag {}",
                    current_block, current_block.epoch, current_block.tag
                );

                #[cfg(feature = "benchmark")]
                for x in &current_block.payload {
                    info!(
                        "Committed B{}({}) epoch {} tag {}",
                        current_block.height,
                        base64::encode(x),
                        current_block.epoch,
                        current_block.tag,
                    );
                }
                // Cleanup the mempool.
                self.mempool_driver.cleanup_par(&current_block).await;
            }
            debug!("Committed {}", current_block);
            let parent = match self.synchronizer.get_parent_block(&current_block).await? {
                Some(b) => b,
                None => {
                    debug!(
                        "Commit ancestors, processing of {} suspended: missing parent",
                        current_block.digest()
                    );
                    break;
                }
            };
            current_block = parent;
        }
        Ok(())
    }
    // -- End Safety Module --

    // -- Start Pacemaker --
    fn update_high_qc(&mut self, qc: &QC) {
        if qc.height > self.high_qc.height {
            self.high_qc = qc.clone();
        }
    }

    #[async_recursion]
    async fn handle_opt_vote(&mut self, vote: &HVote) -> ConsensusResult<()> {
        debug!("Processing OPT Vote {:?}", vote);

        if vote.height < self.height || self.epoch != vote.epoch {
            return Ok(());
        }

        // Ensure the vote is well formed.
        vote.verify(&self.committee)?;

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(qc) = self.aggregator.add_hs_vote(vote.clone())? {
            debug!("Assembled {:?}", qc);
            // println!("Assembled {:?}", qc);
            // Process the QC.
            self.process_qc(&qc).await;

            // Make a new block if we are the next leader.
            if self.name == self.leader_elector.get_leader(self.height) {
                let block = self
                    .generate_proposal(self.epoch, self.height, OPT, self.high_qc.clone())
                    .await;
                self.broadcast_opt_propose(block).await?;
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn advance_height(&mut self, height: SeqNumber) {
        if height < self.height {
            return;
        }

        // Cleanup the vote aggregator.
        self.aggregator.cleanup_hs_vote(&self.height);
        // Reset the timer and advance round.
        self.height = height + 1;
        debug!("Moved to round {}", self.height);
        self.update_hs_state(self.height);
    }
    // -- End Pacemaker --

    #[async_recursion]
    async fn generate_proposal(
        &mut self,
        epoch: SeqNumber,
        height: SeqNumber,
        tag: u8,
        qc: QC,
    ) -> Block {
        // Make a new block.
        let payload = self
            .mempool_driver
            .get(self.parameters.max_payload_size, tag)
            .await;
        let mut block = Block::new(
            qc,
            self.name,
            height,
            epoch,
            payload,
            self.signature_service.clone(),
        )
        .await;
        block.tag = tag;
        if !block.payload.is_empty() {
            info!("Created {} epoch {} tag {}", block, block.epoch, block.tag);

            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Created B{}({}) epoch {} tag {}",
                    block.height,
                    base64::encode(x),
                    block.epoch,
                    block.tag
                );
            }
        }
        debug!("Created {:?}", block);

        block
    }

    async fn broadcast_opt_propose(&mut self, block: Block) -> ConsensusResult<()> {
        // Process our new block and broadcast it.
        let message = ConsensusMessage::HsPropose(block.clone());
        Synchronizer::transmit(
            message,
            &self.name,
            None,
            &self.network_filter,
            &self.committee,
            OPT,
        )
        .await?;

        // if self.pes_path {
        //     let message =
        //         ConsensusMessage::DelayProPose(self.epoch, self.height, self.high_qc.clone());
        //     if let Err(e) = self.tx_smvba.send(message).await {
        //         warn!("Failed to send block through the smvba channel: {}", e);
        //     }
        // }
        self.process_opt_block(&block).await?;
        // Wait for the minimum block delay.
        sleep(Duration::from_millis(self.parameters.min_block_delay)).await;
        Ok(())
    }

    async fn broadcast_pes_propose(
        &mut self,
        block: Block,
        proof: SPBProof,
    ) -> ConsensusResult<()> {
        let value = SPBValue::new(block, proof.round, proof.phase).await;

        if proof.phase == INIT_PHASE {
            self.spb_proposes
                .insert((value.block.height, value.round), value.clone());
        }

        let message = ConsensusMessage::SPBPropose(value.clone(), proof.clone());
        Synchronizer::transmit(
            message,
            &self.name,
            None,
            &self.network_filter_smvba,
            &self.committee,
            PES,
        )
        .await?;
        self.process_spb_propose(&value, &proof).await?;
        // Wait for the minimum block delay.
        sleep(Duration::from_millis(self.parameters.min_block_delay)).await;
        Ok(())
    }

    async fn process_qc(&mut self, qc: &QC) {
        self.advance_height(qc.height).await;
        self.update_high_qc(qc);
    }

    #[async_recursion]
    async fn process_opt_block(&mut self, block: &Block) -> ConsensusResult<()> {
        debug!("Processing OPT Block {:?}", block);

        if self.pes_path {
            let message =
                ConsensusMessage::DelayProPose(block.epoch, block.height, block.qc.clone());
            if let Err(e) = self.tx_smvba.send(message).await {
                warn!("Failed to send block through the smvba channel: {}", e);
            }
        }

        // Let's see if we have the last three ancestors of the block, that is:
        //      b0 <- |qc0; b1| <- |qc1; block|
        // If we don't, the synchronizer asks for them to other nodes. It will
        // then ensure we process both ancestors in the correct order, and
        // finally make us resume processing this block.

        let (b0, b1) = match self.synchronizer.get_ancestors(block).await? {
            Some(ancestors) => ancestors,
            None => {
                debug!("Processing of {} suspended: missing parent", block.digest());
                return Ok(());
            }
        };

        //TODO:
        // 1. 对 height-1 的 block 发送 prepare-opt
        let flag = self
            .smvba_is_invoke
            .entry(block.height - 1)
            .or_insert(false);
        if self.pes_path && block.height > 1 && *flag {
            self.active_prepare_pahse(
                block.height - 1,
                &b1,                                 // Block h-1
                PrePareProof::OPT(block.qc.clone()), //qc h-1
                OPT,
            )
            .await?;
        }
        // 2. 终止 height-2 的 SMVBA
        if self.pes_path && self.height > 2 {
            self.terminate_smvba(self.height - 2).await?;
        }

        // Store the block only if we have already processed all its ancestors.
        self.store_block(block).await;

        // The chain should have consecutive round numbers by construction.
        let mut consecutive_rounds = b0.height + 1 == b1.height;
        consecutive_rounds &= b1.height + 1 == block.height;
        ensure!(
            consecutive_rounds || block.qc == QC::genesis(),
            ConsensusError::NonConsecutiveRounds {
                rd1: b0.height,
                rd2: b1.height,
                rd3: block.height
            }
        );

        if b0.height > self.last_committed_height {
            self.commit(&b0).await?;

            self.last_committed_height = b0.height;
            debug!("Committed {:?}", b0);
            if let Err(e) = self.commit_channel.send(b0.clone()).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }

        // Ensure the block's round is as expected.
        // This check is important: it prevents bad leaders from producing blocks
        // far in the future that may cause overflow on the round number.
        if block.height != self.height {
            return Ok(());
        }

        // See if we can vote for this block.
        if let Some(vote) = self.make_opt_vote(block).await {
            debug!("Created hs {:?}", vote);
            //3. broadcast vote
            let leader = self.leader_elector.get_leader(self.height + 1);
            if leader != self.name {
                let message = ConsensusMessage::HSVote(vote.clone());
                Synchronizer::transmit(
                    message,
                    &self.name,
                    Some(&leader),
                    &self.network_filter,
                    &self.committee,
                    OPT,
                )
                .await?;
            } else {
                self.handle_opt_vote(&vote).await?;
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn process_spb_propose(
        &mut self,
        value: &SPBValue,
        proof: &SPBProof,
    ) -> ConsensusResult<()> {
        debug!("Processing PES Block {:?}", value.block);

        //如果是lock 阶段 保存
        if value.phase == LOCK_PHASE {
            self.spb_locks
                .entry((value.block.height, value.round))
                .or_insert(HashMap::new())
                .insert(value.block.author, (value.clone(), proof.clone()));
        }

        //vote
        if let Some(spb_vote) = self.make_spb_vote(&value).await {
            //将vote 广播给value 的 propose

            if self.name != value.block.author {
                let message = ConsensusMessage::SPBVote(spb_vote);
                Synchronizer::transmit(
                    message,
                    &self.name,
                    Some(&value.block.author),
                    &self.network_filter_smvba,
                    &self.committee,
                    PES,
                )
                .await?;
            } else {
                self.handle_spb_vote(&spb_vote).await?;
            }
        }
        Ok(())
    }

    async fn handle_opt_proposal(&mut self, block: &Block) -> ConsensusResult<()> {
        let digest = block.digest();
        if block.epoch < self.epoch {
            return Ok(());
        } else if block.epoch > self.epoch {
            self.unhandle_message
                .push_back((block.epoch, ConsensusMessage::HsPropose(block.clone())));
            return Err(ConsensusError::EpochEnd(self.epoch));
        }
        // Ensure the block proposer is the right leader for the round.
        ensure!(
            block.author == self.leader_elector.get_leader(block.height),
            ConsensusError::WrongLeader {
                digest,
                leader: block.author,
                round: block.height
            }
        );

        // Check the block is correctly formed.
        block.verify(&self.committee)?;

        // Process the QC. This may allow us to advance round.
        self.process_qc(&block.qc).await;

        // Let's see if we have the block's data. If we don't, the mempool
        // will get it and then make us resume processing this block.
        if !self.mempool_driver.verify(block.clone(), OPT).await? {
            debug!("Processing of {} suspended: missing payload", digest);
            return Ok(());
        }

        // All check pass, we can process this block.
        self.process_opt_block(block).await
    }

    async fn active_prepare_pahse(
        &mut self,
        height: SeqNumber,
        block: &Block,
        proof: PrePareProof,
        path: u8,
    ) -> ConsensusResult<()> {
        if self.prepare_tag.contains(&height) {
            return Ok(());
        }
        self.prepare_tag.insert(height);

        let prepare = PrePare::new(
            self.name,
            block.clone(),
            proof,
            path,
            self.signature_service.clone(),
        )
        .await;

        let message = ConsensusMessage::ParPrePare(prepare.clone());
        Synchronizer::transmit(
            message,
            &self.name,
            None,
            &self.network_filter_smvba,
            &self.committee,
            PES,
        )
        .await?;

        self.handle_par_prepare(prepare.clone()).await?;
        Ok(())
    }

    async fn terminate_smvba(&mut self, height: SeqNumber) -> ConsensusResult<()> {
        self.clean_smvba_state(&height);
        Ok(())
    }

    async fn handle_sync_request(
        &mut self,
        digest: Digest,
        sender: PublicKey,
    ) -> ConsensusResult<()> {
        if let Some(bytes) = self.store.read(digest.to_vec()).await? {
            let block = bincode::deserialize(&bytes)?;
            let message = ConsensusMessage::SyncReply(block);
            Synchronizer::transmit(
                message,
                &self.name,
                Some(&sender),
                &self.network_filter,
                &self.committee,
                OPT,
            )
            .await?;
        }
        Ok(())
    }

    fn smvba_msg_filter(
        &mut self,
        epoch: SeqNumber,
        height: SeqNumber,
        _round: SeqNumber,
        _phase: u8,
    ) -> bool {
        if self.epoch > epoch {
            return false;
        }
        if self.height >= height + 2 {
            return false;
        }
        // let cur_round = self.smvba_current_round.entry(height).or_insert(1);
        // if *cur_round > round {
        //     return false;
        // }

        // // halt?
        // if *self.smvba_halt_falg.entry((height, round)).or_insert(false) {
        //     return false;
        // }

        true
    }

    //SMVBA only deal current round
    async fn handle_spb_proposal(
        &mut self,
        value: SPBValue,
        proof: SPBProof,
    ) -> ConsensusResult<()> {
        //check message is timeout?

        ensure!(
            self.smvba_msg_filter(value.block.epoch, proof.height, proof.round, proof.phase),
            ConsensusError::TimeOutMessage(proof.height, proof.round)
        );

        if value.block.epoch > self.epoch {
            self.unhandle_message.push_back((
                value.block.epoch,
                ConsensusMessage::SPBPropose(value, proof),
            ));
            return Err(ConsensusError::EpochEnd(self.epoch));
        }

        // if *self
        //     .spb_abandon_flag
        //     .entry((proof.height, proof.round))
        //     .or_insert(false)
        // {
        //     return Ok(());
        // }
        if self.parameters.exp == 1 {
            //验证Proof是否正确
            value.verify(&self.committee, &proof, &self.pk_set)?;
        }

        self.process_spb_propose(&value, &proof).await?;
        Ok(())
    }

    #[async_recursion]
    async fn handle_spb_vote(&mut self, spb_vote: &SPBVote) -> ConsensusResult<()> {
        debug!("Processing {:?}", spb_vote);
        //check message is timeout?
        ensure!(
            self.smvba_msg_filter(
                spb_vote.epoch,
                spb_vote.height,
                spb_vote.round,
                spb_vote.phase
            ),
            ConsensusError::TimeOutMessage(spb_vote.height, spb_vote.round)
        );

        // if *self
        //     .spb_abandon_flag
        //     .entry((spb_vote.height, spb_vote.round))
        //     .or_insert(false)
        // {
        //     return Ok(());
        // }
        if self.parameters.exp == 1 {
            spb_vote.verify(&self.committee, &self.pk_set)?;
        }
        if let Some(proof) = self.aggregator.add_spb_vote(spb_vote.clone())? {
            debug!("Create spb proof {:?}!", proof);
            // println!("Create spb proof {:?}!", proof);
            // self.spb_current_phase
            //     .insert((spb_vote.height, spb_vote.round), proof.phase); //phase + 1

            let value = self.spb_proposes.get(&(proof.height, proof.round)).unwrap();
            //进行下一阶段的发送
            if proof.phase == LOCK_PHASE {
                self.broadcast_pes_propose(value.block.clone(), proof)
                    .await?;
            } else if proof.phase == FIN_PHASE {
                let mut temp = value.clone();
                temp.phase = FIN_PHASE;

                let message = ConsensusMessage::SPBFinsh(temp.clone(), proof.clone());
                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter_smvba,
                    &self.committee,
                    PES,
                )
                .await?;
                self.handle_spb_finish(temp, proof).await?;
            }
        }
        Ok(())
    }

    async fn handle_spb_finish(&mut self, value: SPBValue, proof: SPBProof) -> ConsensusResult<()> {
        debug!("Processing finish {:?}", proof);

        // check message is timeout?
        ensure!(
            self.smvba_msg_filter(value.block.epoch, proof.height, proof.round, proof.phase),
            ConsensusError::TimeOutMessage(proof.height, proof.round)
        );

        if self.parameters.exp == 1 {
            value.verify(&self.committee, &proof, &self.pk_set)?;
        }

        self.spb_finishs
            .entry((proof.height, proof.round))
            .or_insert(HashMap::new())
            .insert(value.block.author, (value.clone(), proof.clone()));

        let d_flag = self
            .smvba_d_flag
            .entry((proof.height, proof.round))
            .or_insert(false);

        if *d_flag {
            return Ok(());
        }

        let weight = self
            .spb_finishs
            .get(&(proof.height, proof.round))
            .unwrap()
            .len() as Stake;

        if weight == self.committee.quorum_threshold() {
            *d_flag = true;
            self.invoke_done_and_share(proof.height, proof.round)
                .await?;
        }

        Ok(())
    }

    async fn handle_smvba_done(&mut self, mdone: MDone) -> ConsensusResult<()> {
        debug!("Processing  {:?}", mdone);

        ensure!(
            self.smvba_msg_filter(mdone.epoch, mdone.height, mdone.round, FIN_PHASE),
            ConsensusError::TimeOutMessage(mdone.height, mdone.round)
        );

        if self.parameters.exp == 1 {
            mdone.verify()?;
        }

        let d_flag = self
            .smvba_d_flag
            .entry((mdone.height, mdone.round))
            .or_insert(false);

        let set = self
            .smvba_dones
            .entry((mdone.height, mdone.round))
            .or_insert(HashSet::new());
        set.insert(mdone.author);
        let mut weight = set.len() as Stake;

        // d_flag= false and weight == f+1?
        if *d_flag == false && weight == self.committee.random_coin_threshold() {
            *d_flag = true;
            set.insert(self.name);
            weight += 1;
            self.invoke_done_and_share(mdone.height, mdone.round)
                .await?;
        }

        // 2f+1?
        if weight == self.committee.quorum_threshold() {
            //abandon spb message
            self.spb_abandon_flag
                .insert((mdone.height, mdone.round), true);
        }

        Ok(())
    }

    #[async_recursion]
    async fn invoke_done_and_share(
        &mut self,
        height: SeqNumber,
        round: SeqNumber,
    ) -> ConsensusResult<()> {
        let mdone = MDone::new(
            self.name,
            self.signature_service.clone(),
            self.epoch,
            height,
            round,
        )
        .await;

        let share = RandomnessShare::new(
            height,
            self.epoch,
            round,
            self.name,
            self.signature_service.clone(),
        )
        .await;

        let message = ConsensusMessage::SPBDone(mdone.clone());
        Synchronizer::transmit(
            message,
            &self.name,
            None,
            &self.network_filter_smvba,
            &self.committee,
            PES,
        )
        .await?;

        let message = ConsensusMessage::SMVBACoinShare(share.clone());
        Synchronizer::transmit(
            message,
            &self.name,
            None,
            &self.network_filter_smvba,
            &self.committee,
            PES,
        )
        .await?;
        self.handle_smvba_done(mdone).await?;
        self.handle_smvba_rs(share).await?;
        Ok(())
    }

    async fn handle_smvba_prevote(&mut self, prevote: MPreVote) -> ConsensusResult<()> {
        debug!("Processing  {:?}", prevote);
        // println!("Processing  {:?}", prevote);
        ensure!(
            self.smvba_msg_filter(prevote.epoch, prevote.height, prevote.round, FIN_PHASE),
            ConsensusError::TimeOutMessage(prevote.height, prevote.round)
        );

        if self.parameters.exp == 1 {
            prevote.verify(&self.committee, &self.pk_set)?;
        }

        let y_flag = self
            .smvba_y_flag
            .entry((prevote.height, prevote.round))
            .or_insert(false);
        let n_flag = self
            .smvba_n_flag
            .entry((prevote.height, prevote.round))
            .or_insert(false);

        let mut mvote: Option<MVote> = None;
        if !(*y_flag) && !(*n_flag) {
            match &prevote.tag {
                PreVoteTag::Yes(value, proof) => {
                    *y_flag = true;
                    if let Some(vote) = self.make_spb_vote(value).await {
                        mvote = Some(
                            MVote::new(
                                self.name,
                                prevote.leader,
                                self.signature_service.clone(),
                                prevote.round,
                                prevote.height,
                                prevote.epoch,
                                MVoteTag::Yes(value.clone(), proof.clone(), vote),
                            )
                            .await,
                        );
                    }
                }
                PreVoteTag::No() => {
                    let set = self
                        .smvba_no_prevotes
                        .entry((prevote.height, prevote.round))
                        .or_insert(HashSet::new());
                    set.insert(prevote.author);
                    let weight = set.len() as Stake;

                    if weight == self.committee.quorum_threshold() {
                        *n_flag = true;
                        mvote = Some(
                            MVote::new(
                                self.name,
                                prevote.leader,
                                self.signature_service.clone(),
                                prevote.round,
                                prevote.height,
                                prevote.epoch,
                                MVoteTag::No(),
                            )
                            .await,
                        );
                    }
                }
            }
        }

        if let Some(vote) = mvote {
            let message = ConsensusMessage::SMVBAVote(vote.clone());
            Synchronizer::transmit(
                message,
                &self.name,
                None,
                &self.network_filter_smvba,
                &self.committee,
                PES,
            )
            .await?;
            self.handle_smvba_mvote(vote).await?;
        }

        Ok(())
    }

    async fn handle_smvba_mvote(&mut self, mvote: MVote) -> ConsensusResult<()> {
        debug!("Processing  {:?}", mvote);
        // println!("Processing  {:?}", mvote);
        ensure!(
            self.smvba_msg_filter(mvote.epoch, mvote.height, mvote.round, FIN_PHASE),
            ConsensusError::TimeOutMessage(mvote.height, mvote.round)
        );

        if self.parameters.exp == 1 {
            mvote.verify(&self.committee, &self.pk_set)?;
        }

        let set = self
            .smvba_votes
            .entry((mvote.height, mvote.round))
            .or_insert(HashSet::new());

        set.insert(mvote.author);

        let weight = set.len() as Stake;

        match mvote.tag {
            MVoteTag::Yes(value, _, vote) => {
                if let Some(fin_proof) = self.aggregator.add_pre_vote(vote)? {
                    let mhalt = MHalt::new(
                        self.name,
                        mvote.leader,
                        value,
                        fin_proof,
                        self.signature_service.clone(),
                    )
                    .await;

                    let message = ConsensusMessage::SMVBAHalt(mhalt.clone());
                    Synchronizer::transmit(
                        message,
                        &self.name,
                        None,
                        &self.network_filter_smvba,
                        &self.committee,
                        PES,
                    )
                    .await?;
                    self.handle_smvba_halt(mhalt).await?;
                    return Ok(());
                }
            }
            MVoteTag::No() => {}
        };

        if weight == self.committee.quorum_threshold() {
            self.smvba_round_advance(mvote.epoch, mvote.height, mvote.round + 1)
                .await?;
        }

        Ok(())
    }

    async fn handle_smvba_rs(&mut self, share: RandomnessShare) -> ConsensusResult<()> {
        debug!("Processing  {:?}", share);

        ensure!(
            self.smvba_msg_filter(share.epoch, share.height, share.round, FIN_PHASE),
            ConsensusError::TimeOutMessage(share.height, share.round)
        );

        if self.parameters.exp == 1 {
            share.verify(&self.committee, &self.pk_set)?;
        }

        if self
            .leader_elector
            .get_coin_leader(share.height, share.round)
            .is_some()
        {
            return Ok(());
        }
        let height = share.height;
        let round = share.round;

        if let Some(coin) = self.aggregator.add_smvba_random(share, &self.pk_set)? {
            self.leader_elector.add_random_coin(coin.clone());

            let leader = coin.leader;

            // container finish?
            if self
                .spb_finishs
                .entry((coin.height, coin.round))
                .or_insert(HashMap::new())
                .contains_key(&leader)
            {
                let (value, proof) = self
                    .spb_finishs
                    .get(&(coin.height, coin.round))
                    .unwrap()
                    .get(&leader)
                    .unwrap();
                let mhalt = MHalt::new(
                    self.name,
                    leader,
                    value.clone(),
                    proof.clone(),
                    self.signature_service.clone(),
                )
                .await;

                let message = ConsensusMessage::SMVBAHalt(mhalt.clone());
                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter_smvba,
                    &self.committee,
                    PES,
                )
                .await?;
                self.handle_smvba_halt(mhalt).await?;
            } else {
                let mut pre_vote = MPreVote::new(
                    self.name,
                    leader,
                    self.signature_service.clone(),
                    round,
                    height,
                    coin.epoch,
                    PreVoteTag::No(),
                )
                .await;

                //container lock?
                if self
                    .spb_locks
                    .entry((coin.height, coin.round))
                    .or_insert(HashMap::new())
                    .contains_key(&leader)
                {
                    let (value, proof) = self
                        .spb_locks
                        .get(&(coin.height, coin.round))
                        .unwrap()
                        .get(&leader)
                        .unwrap();
                    pre_vote = MPreVote::new(
                        self.name,
                        leader,
                        self.signature_service.clone(),
                        round,
                        height,
                        coin.epoch,
                        PreVoteTag::Yes(value.clone(), proof.clone()),
                    )
                    .await;
                }
                let message = ConsensusMessage::SMVBAPreVote(pre_vote.clone());
                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter_smvba,
                    &self.committee,
                    PES,
                )
                .await?;
                self.handle_smvba_prevote(pre_vote).await?;
            }
        }

        Ok(())
    }

    async fn handle_smvba_halt(&mut self, halt: MHalt) -> ConsensusResult<()> {
        debug!("Processing {:?}", halt);

        ensure!(
            self.smvba_msg_filter(halt.epoch, halt.height, halt.round, FIN_PHASE),
            ConsensusError::TimeOutMessage(halt.height, halt.round)
        );
        if self.leader_elector.get_coin_leader(halt.height, halt.round)
            != Some(halt.value.block.author)
        // leader 是否与 finish value的proposer 相符
        {
            return Ok(());
        }
        // halt?
        if *self
            .smvba_halt_falg
            .entry((halt.height, halt.round))
            .or_insert(false)
        {
            return Ok(());
        }

        self.smvba_halt_falg.insert((halt.height, halt.round), true);

        if self.parameters.exp == 1 {
            halt.verify(&self.committee, &self.pk_set)?;
        }
        //smvba end -> send pes-prepare
        self.active_prepare_pahse(
            halt.height,
            &halt.value.block,
            PrePareProof::PES(halt.proof),
            PES,
        )
        .await?;

        Ok(())
    }

    async fn smvba_round_advance(
        &mut self,
        epoch: SeqNumber,
        height: SeqNumber,
        round: SeqNumber,
    ) -> ConsensusResult<()> {
        info!(
            "-------------smvba round advance height {}, round {}------------",
            height, round
        );
        self.update_smvba_state(height, round);
        let proof = SPBProof {
            height: self.height,
            phase: INIT_PHASE,
            round,
            shares: Vec::new(),
        };
        let block = self
            .generate_proposal(epoch, height, PES, self.high_qc.clone())
            .await;
        self.broadcast_pes_propose(block, proof).await?;
        Ok(())
    }

    async fn handle_par_prepare(&mut self, prepare: PrePare) -> ConsensusResult<()> {
        debug!("Processing {:?}", prepare);

        if prepare.epoch != self.epoch || prepare.height + 2 <= self.height {
            return Ok(());
        }

        if self.parameters.exp == 1 {
            prepare.verify(&self.committee, &self.pk_set)?;
        }

        let opt_set = self
            .par_prepare_opts
            .entry(prepare.height)
            .or_insert(HashSet::new());
        let pes_set = self
            .par_prepare_pess
            .entry(prepare.height)
            .or_insert(HashSet::new());

        ensure!(
            !opt_set.contains(&prepare.author) && !pes_set.contains(&prepare.author),
            ConsensusError::AuthorityReuseinPrePare(prepare.author)
        );

        let vals = self
            .par_values
            .entry(prepare.height)
            .or_insert([None, None]);

        let val = prepare.tag as usize;
        let height = prepare.height;

        if vals[prepare.tag as usize].is_none() {
            vals[prepare.tag as usize] = Some(prepare.block)
        }

        match prepare.tag {
            OPT => opt_set.insert(prepare.author),
            PES => pes_set.insert(prepare.author),
            _ => return Err(ConsensusError::InvalidPrePareTag(prepare.tag)),
        };
        let opt_nums = opt_set.len() as Stake;
        let pes_nums = pes_set.len() as Stake;
        let mut aba_val: Option<ABAVal> = None;
        if opt_nums + pes_nums == self.committee.quorum_threshold() {
            if opt_nums == self.committee.quorum_threshold() {
                // 啥事不干 已经保证了至少有f+1诚实节点已经收到了h-1高度的propose
            } else if opt_nums > 0 {
                aba_val = Some(
                    ABAVal::new(
                        self.name,
                        self.epoch,
                        prepare.height,
                        1,
                        0,
                        VAL_PHASE,
                        self.signature_service.clone(),
                    )
                    .await,
                )
            } else {
                //invoke aba -> 1
                aba_val = Some(
                    ABAVal::new(
                        self.name,
                        self.epoch,
                        prepare.height,
                        1,
                        1,
                        VAL_PHASE,
                        self.signature_service.clone(),
                    )
                    .await,
                )
            }
        }

        if let Some(val) = aba_val {
            let message = ConsensusMessage::ParABAVal(val.clone());
            Synchronizer::transmit(
                message,
                &self.name,
                None,
                &self.network_filter_smvba,
                &self.committee,
                PES,
            )
            .await?;
            self.handle_aba_val(val).await?;
        }

        //wait value?
        let wait = self.par_value_wait.entry(height).or_insert([false, false]);
        if wait[val] {
            wait[val] = false;
            self.handle_par_out(height, val).await?;
        }

        Ok(())
    }

    fn aba_message_filter(
        &mut self,
        epoch: SeqNumber,
        height: SeqNumber,
        round: SeqNumber,
        phase: u8,
    ) -> bool {
        if epoch != self.epoch || height + 2 <= self.height || phase > MUX_PHASE {
            return false;
        }
        let cur_round = self.aba_current_round.entry(height).or_insert(1);
        if *cur_round > round {
            return false;
        }

        if self.aba_output.entry(height).or_insert(None).is_some() {
            return false;
        }

        true
    }

    // val phase and aux phase
    #[async_recursion]
    async fn handle_aba_val(&mut self, aba_val: ABAVal) -> ConsensusResult<()> {
        debug!("Processing {:?}", aba_val);

        if self.parameters.exp == 1 {
            aba_val.verify()?;
        }

        if !self.aba_message_filter(aba_val.epoch, aba_val.height, aba_val.round, aba_val.phase) {
            return Ok(());
        }

        if self
            .aba_output
            .entry(aba_val.height)
            .or_insert(None)
            .is_some()
        {
            return Ok(());
        }

        let used = self
            .aba_vals_used
            .entry((aba_val.height, aba_val.round, aba_val.phase))
            .or_insert(HashSet::new());

        ensure!(
            used.insert(aba_val.author),
            ConsensusError::AuthorityReuseinABA(aba_val.author, aba_val.phase)
        );

        let temp_vals = self
            .aba_temp_vals
            .entry((aba_val.height, aba_val.round, aba_val.phase))
            .or_insert([0, 0]);

        temp_vals[aba_val.val] += 1;

        if aba_val.phase == VAL_PHASE {
            if temp_vals[aba_val.val] == self.committee.random_coin_threshold() //f+1
                && !used.contains(&self.name)
            {
                used.insert(self.name);
                temp_vals[aba_val.val] += 1;
                let val_t = ABAVal::new(
                    self.name,
                    self.epoch,
                    aba_val.height,
                    aba_val.round,
                    aba_val.val,
                    VAL_PHASE,
                    self.signature_service.clone(),
                )
                .await;
                let message = ConsensusMessage::ParABAVal(val_t);
                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter_smvba,
                    &self.committee,
                    PES,
                )
                .await?;
            }

            if temp_vals[aba_val.val] == self.committee.quorum_threshold() {
                //2f+1
                self.aba_bin_vals
                    .entry((aba_val.height, aba_val.round))
                    .or_insert([false, false])[aba_val.val] = true;
                let mux = ABAVal::new(
                    self.name,
                    self.epoch,
                    aba_val.height,
                    aba_val.round,
                    aba_val.val,
                    MUX_PHASE,
                    self.signature_service.clone(),
                )
                .await;
                let message = ConsensusMessage::ParABAVal(mux.clone());
                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter_smvba,
                    &self.committee,
                    PES,
                )
                .await?;
                self.handle_aba_val(mux).await?;
            }
        } else {
            let bin_vals = self
                .aba_bin_vals
                .entry((aba_val.height, aba_val.round))
                .or_insert([false, false]);

            let mux_vals = self
                .aba_mux_vals
                .entry((aba_val.height, aba_val.round))
                .or_insert([false, false]);

            let mut flag = false;

            if !(mux_vals[0] | mux_vals[1]) {
                let weight = self.committee.quorum_threshold();

                if temp_vals[0] >= weight && bin_vals[0] {
                    mux_vals[0] = true;
                    flag = true;
                } else if temp_vals[1] >= weight && bin_vals[1] {
                    mux_vals[1] = true;
                    flag = true;
                } else if temp_vals[1] + temp_vals[0] >= weight && bin_vals[0] & bin_vals[1] {
                    *mux_vals = [true, true];
                    flag = true;
                }
            }

            if flag {
                let share = RandomnessShare::new(
                    aba_val.height,
                    self.epoch,
                    aba_val.round,
                    self.name,
                    self.signature_service.clone(),
                )
                .await;

                let message = ConsensusMessage::ParABACoinShare(share.clone());

                Synchronizer::transmit(
                    message,
                    &self.name,
                    None,
                    &self.network_filter_smvba,
                    &self.committee,
                    PES,
                )
                .await?;

                self.handle_aba_rs(share).await?;
            }
        }

        Ok(())
    }

    async fn handle_aba_rs(&mut self, share: RandomnessShare) -> ConsensusResult<()> {
        debug!("Processing aba coin share {:?}", share);

        if self.parameters.exp == 1 {
            share.verify(&self.committee, &self.pk_set)?;
        }

        if !self.aba_message_filter(share.epoch, share.height, share.round, MUX_PHASE) {
            return Ok(());
        }

        let height = share.height;
        let round = share.round;

        let mux_vals = self
            .aba_mux_vals
            .entry((share.height, share.round))
            .or_insert([false, false]);
        if let Some(coin) = self.aggregator.add_aba_random(share, &self.pk_set)? {
            let mut val = coin;
            if mux_vals[0] ^ mux_vals[1] {
                if mux_vals[coin] {
                    //TODO broadcast aba exit message
                    let aba_out = ABAOutput::new(
                        self.name,
                        self.epoch,
                        height,
                        round,
                        coin,
                        self.signature_service.clone(),
                    )
                    .await;

                    let message = ConsensusMessage::ParABAOutput(aba_out.clone());
                    Synchronizer::transmit(
                        message,
                        &self.name,
                        None,
                        &self.network_filter_smvba,
                        &self.committee,
                        PES,
                    )
                    .await?;

                    return self.handle_aba_output(aba_out).await;
                } else {
                    val = coin ^ 1;
                }
            }
            // into next round
            let aba_val = ABAVal::new(
                self.name,
                self.epoch,
                height,
                round + 1,
                val,
                VAL_PHASE,
                self.signature_service.clone(),
            )
            .await;
            let message = ConsensusMessage::ParABAVal(aba_val.clone());

            Synchronizer::transmit(
                message,
                &self.name,
                None,
                &self.network_filter_smvba,
                &self.committee,
                PES,
            )
            .await?;

            self.handle_aba_val(aba_val).await?;
        }

        Ok(())
    }

    async fn handle_aba_output(&mut self, aba_out: ABAOutput) -> ConsensusResult<()> {
        debug!("Processing {:?}", aba_out);

        if self.parameters.exp == 1 {
            aba_out.verify()?;
        }

        if !self.aba_message_filter(aba_out.epoch, aba_out.height, aba_out.round, MUX_PHASE) {
            return Ok(());
        }

        let height = aba_out.height;
        let round = aba_out.round;

        let out_set = self
            .aba_output_messages
            .entry(height)
            .or_insert(HashSet::new());

        ensure!(
            out_set.insert(aba_out.author),
            ConsensusError::AuthorityReuseinABAOut(aba_out.author)
        );

        let mut nums = out_set.len() as Stake;

        if nums == self.committee.random_coin_threshold() && !out_set.contains(&self.name) {
            //f+1

            let out = ABAOutput::new(
                self.name,
                self.epoch,
                height,
                round,
                aba_out.val,
                self.signature_service.clone(),
            )
            .await;

            let message = ConsensusMessage::ParABAOutput(out);
            Synchronizer::transmit(
                message,
                &self.name,
                None,
                &self.network_filter_smvba,
                &self.committee,
                PES,
            )
            .await?;

            out_set.insert(self.name);
            nums += 1;
        }

        if nums == self.committee.quorum_threshold() {
            self.aba_output
                .entry(self.height)
                .or_insert(Some(aba_out.val));
            self.handle_par_out(height, aba_out.val).await?;
        }

        Ok(())
    }

    async fn handle_par_out(&mut self, height: SeqNumber, val: usize) -> ConsensusResult<()> {
        //TODO deal
        if height < self.last_committed_height {
            return Ok(());
        }

        let values = self.par_values.entry(height).or_insert([None, None]);

        if values[val].is_some() {
            //提交
            let mut block = values[val].clone().unwrap();
            if val as u8 == PES {
                block.tag = PES;
            }
            // Process the QC. This may allow us to advance round.
            self.process_qc(&block.qc).await;

            // Let's see if we have the block's data. If we don't, the mempool
            // will get it and then make us resume processing this block.
            if !self.mempool_driver.verify(block.clone(), PES).await? {
                debug!(
                    "Processing of {} suspended: missing payload",
                    block.digest()
                );
                return Ok(());
            }

            self.process_par_out(&block).await?;
        } else {
            self.par_value_wait.entry(height).or_insert([false, false])[val] = true;
        }
        Ok(())
    }

    async fn process_par_out(&mut self, block: &Block) -> ConsensusResult<()> {
        let tag = block.tag;
        // Store the block only if we have already processed all its ancestors.
        self.store_block(block).await;

        if block.height > self.last_committed_height {
            self.commit(block).await?;

            self.last_committed_height = block.height;
            debug!("Committed {:?}", block);
            if let Err(e) = self.commit_channel.send(block.clone()).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
            if tag == PES {
                //如果是悲观路径输出
                info!("---------ABA output 1,epoch {} end------------", self.epoch);

                return Err(ConsensusError::EpochEnd(self.epoch));
            } else {
                info!("---------ABA output 0----------");
            }
        }

        self.mempool_driver.cleanup_par(block).await;
        Ok(())
    }

    pub async fn run_epoch(&mut self) {
        let mut epoch = 0u64;
        loop {
            info!("---------------Epoch Run {}------------------", self.epoch);
            self.run().await; //运行当前epoch
            epoch += 1;
            self.epoch_init(epoch);

            while !self.unhandle_message.is_empty() {
                if let Some((e, msg)) = self.unhandle_message.pop_front() {
                    if e == self.epoch {
                        match msg {
                            ConsensusMessage::HsPropose(..) => {
                                if let Err(e) = self.tx_core.send(msg).await {
                                    panic!("Failed to send last epoch message: {}", e);
                                }
                            }
                            ConsensusMessage::SPBPropose(..) => {
                                if let Err(e) = self.tx_smvba.send(msg).await {
                                    panic!("Failed to send last epoch message: {}", e);
                                }
                            }
                            _ => break,
                        }
                    }
                }
            }
        }
    }

    pub async fn run(&mut self) {
        // Upon booting, generate the very first block (if we are the leader).
        // Also, schedule a timer in case we don't hear from the leader.
        if self.opt_path && self.name == self.leader_elector.get_leader(self.height) {
            let block = self
                .generate_proposal(self.epoch, self.height, OPT, self.high_qc.clone())
                .await;
            self.broadcast_opt_propose(block)
                .await
                .expect("Failed to send the first OPT block");
        }

        //如果启动了悲观路劲
        if self.pes_path {
            let message =
                ConsensusMessage::DelayProPose(self.epoch, self.height, self.high_qc.clone());
            if let Err(e) = self.tx_smvba.send(message).await {
                warn!("Failed to send block through the smvba channel: {}", e);
            }
        }

        // This is the main loop: it processes incoming blocks and votes,
        // and receive timeout notifications from our Timeout Manager.
        let mut pending_smvba = FuturesUnordered::new();
        loop {
            let result = tokio::select! {
                Some(message) = self.core_channel.recv() => {
                    match message {
                        ConsensusMessage::HsPropose(block) => self.handle_opt_proposal(&block).await,
                        ConsensusMessage::HSVote(vote) => self.handle_opt_vote(&vote).await,
                        ConsensusMessage::HsLoopBack(block) => self.process_opt_block(&block).await,
                        ConsensusMessage::SyncRequest(digest, sender) => self.handle_sync_request(digest, sender).await,
                        ConsensusMessage::SyncReply(block) => self.handle_opt_proposal(&block).await,
                        _=> Ok(()),
                    }
                },
                Some(message) = self.smvba_channel.recv() => {
                    match message {
                        ConsensusMessage::DelayProPose(e,h,qc)=> {
                            pending_smvba.push(Self::parbft2_smvba_delay(self.parameters.timeout_delay,e,h,qc));
                            Ok(())
                        }
                        ConsensusMessage::SPBPropose(value,proof)=> self.handle_spb_proposal(value,proof).await,
                        ConsensusMessage::SPBVote(vote)=> self.handle_spb_vote(&vote).await,
                        ConsensusMessage::SPBFinsh(value,proof)=> self.handle_spb_finish(value,proof).await,
                        ConsensusMessage::SPBDone(done) => self.handle_smvba_done(done).await,
                        ConsensusMessage::SMVBAPreVote(prevote) => self.handle_smvba_prevote(prevote).await,
                        ConsensusMessage::SMVBAVote(mvote) => self.handle_smvba_mvote(mvote).await,
                        ConsensusMessage::SMVBACoinShare(random_share)=> self.handle_smvba_rs(random_share).await,
                        ConsensusMessage::SMVBAHalt(halt) => self.handle_smvba_halt(halt).await,
                        ConsensusMessage::ParPrePare(prepare) => self.handle_par_prepare(prepare).await,
                        ConsensusMessage::ParABAVal(aba_val) => self.handle_aba_val(aba_val).await,
                        ConsensusMessage::ParABAOutput(aba_out) => self.handle_aba_output(aba_out).await,
                        ConsensusMessage::ParLoopBack(block) => self.process_par_out(&block).await,
                        ConsensusMessage::ParABACoinShare(rs) => self.handle_aba_rs(rs).await,
                        _=> Ok(()),
                    }
                },
                Some((epoch,height,qc)) = pending_smvba.next() =>{
                    if epoch>=self.epoch && height+2>self.height{
                        let proof = SPBProof {
                            height: self.height,
                            phase: INIT_PHASE,
                            round: 1,
                            shares: Vec::new(),
                        };
                        self.smvba_is_invoke.insert(height,true);
                        let block = self.generate_proposal(epoch, height,PES,qc).await;
                        self.broadcast_pes_propose(block, proof).await.expect("Failed to send the PES block");
                    }
                    Ok(())
                },
                else => break,
            };
            match result {
                Ok(()) => (),
                Err(ConsensusError::SerializationError(e)) => error!("Store corrupted. {}", e),
                Err(ConsensusError::EpochEnd(e)) => {
                    info!("---------------Epoch End {e}------------------");
                    return;
                }
                Err(ConsensusError::TimeOutMessage(..)) => {}
                Err(e) => {
                    warn!("{}", e)
                }
            }
        }
    }
}
