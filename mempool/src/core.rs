use crate::config::{Committee, Parameters};
use crate::error::{MempoolError, MempoolResult};
use crate::messages::Payload;
use crate::payload::PayloadMaker;
use crate::synchronizer::Synchronizer;
use consensus::{Block, ConsensusMempoolMessage, PayloadStatus, SeqNumber, OPT, PES};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
#[cfg(feature = "benchmark")]
use log::info;
use log::{error, warn};
use network::NetMessage;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

#[derive(Deserialize, Serialize, Debug)]
pub enum MempoolMessage {
    OwnPayload(Payload),
    Payload(Payload),
    PayloadRequest(Vec<Digest>, PublicKey),
}

pub struct Core {
    name: PublicKey,
    committee: Committee,
    parameters: Parameters,
    store: Store,
    synchronizer: Synchronizer,
    payload_maker: PayloadMaker,
    core_channel: Receiver<MempoolMessage>,
    consensus_channel: Receiver<ConsensusMempoolMessage>,
    network_channel: Sender<NetMessage>,
    opt_queue: HashSet<Digest>,
    pes_queue: HashSet<Digest>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        synchronizer: Synchronizer,
        payload_maker: PayloadMaker,
        core_channel: Receiver<MempoolMessage>,
        consensus_channel: Receiver<ConsensusMempoolMessage>,
        network_channel: Sender<NetMessage>,
    ) -> Self {
        let opt_queue = HashSet::with_capacity(parameters.queue_capacity);
        let pes_queue = HashSet::with_capacity(parameters.queue_capacity * 3 / 2);
        Self {
            name,
            committee,
            parameters,
            store,
            synchronizer,
            core_channel,
            consensus_channel,
            network_channel,
            opt_queue,
            pes_queue,
            payload_maker,
        }
    }

    async fn store_payload(&mut self, key: Vec<u8>, payload: &Payload) {
        let value = bincode::serialize(payload).expect("Failed to serialize payload");
        self.store.write(key, value).await;
    }

    async fn transmit(
        &mut self,
        message: &MempoolMessage,
        to: Option<&PublicKey>,
    ) -> MempoolResult<()> {
        Synchronizer::transmit(
            message,
            &self.name,
            to,
            &self.committee,
            &self.network_channel,
        )
        .await
    }

    async fn process_own_payload(
        &mut self,
        digest: &Digest,
        payload: Payload,
    ) -> MempoolResult<()> {
        // Drop the transaction if our mempool is full.
        ensure!(
            self.opt_queue.len() < self.opt_queue.capacity()
                && self.pes_queue.len() < self.pes_queue.capacity(),
            MempoolError::MempoolFull
        );

        #[cfg(feature = "benchmark")]
        // NOTE: This log entry is used to compute performance.
        info!("Payload {:?} contains {} B", digest, payload.size());

        #[cfg(feature = "benchmark")]
        for tx in &payload.transactions {
            // Look for sample txs (they all start with 0) and gather their
            // txs id (the next 8 bytes).
            if tx[0] == 0u8 && tx.len() > 8 {
                if let Ok(id) = tx[1..9].try_into() {
                    // NOTE: This log entry is used to compute performance.
                    info!(
                        "Payload {:?} contains sample tx {}",
                        digest,
                        u64::from_be_bytes(id)
                    );
                }
            }
        }

        // Store the payload.
        self.store_payload(digest.to_vec(), &payload).await;

        // Share the payload with all other nodes.
        let message = MempoolMessage::Payload(payload); //向其他节点发送这个payload
        self.transmit(&message, None).await
        // Ok(())
    }

    async fn handle_own_payload(&mut self, payload: Payload) -> MempoolResult<()> {
        // Drop the transaction if our mempool is full.
        ensure!(
            self.opt_queue.len() < self.parameters.queue_capacity,
            MempoolError::MempoolFull
        );

        // Otherwise, try to add the transaction to the next payload
        // we will add to the queue.
        let digest = payload.digest();
        self.process_own_payload(&digest, payload).await?; //payload存入queue中
        self.opt_queue.insert(digest.clone());
        self.pes_queue.insert(digest);
        Ok(())
    }

    async fn handle_others_payload(&mut self, payload: Payload) -> MempoolResult<()> {
        // Ensure the author of the payload is in the committee.
        let author = payload.author;
        ensure!(
            self.committee.exists(&author),
            MempoolError::UnknownAuthority(author)
        );

        // Verify that the payload does not exceed the maximum size.
        ensure!(
            payload.size() <= self.parameters.max_payload_size,
            MempoolError::PayloadTooBig
        );

        // Verify that the payload is correctly signed.
        let digest = payload.digest();
        payload.signature.verify(&digest, &author)?;

        // Store payload.
        // TODO [issue #18]: A bad node may make us store a lot of junk. There is no
        // limit to how many payloads they can send us, and we will store them all.
        self.store_payload(digest.to_vec(), &payload).await;

        // Add the payload to the queue.
        self.opt_queue.insert(digest.clone());
        self.pes_queue.insert(digest);
        Ok(())
    }

    async fn handle_request(
        &mut self,
        digests: Vec<Digest>,
        requestor: PublicKey,
    ) -> MempoolResult<()> {
        for digest in &digests {
            if let Some(bytes) = self.store.read(digest.to_vec()).await? {
                let payload = bincode::deserialize(&bytes)?;
                let message = MempoolMessage::Payload(payload);
                self.transmit(&message, Some(&requestor)).await?;
            }
        }
        Ok(())
    }

    async fn get_payload(&mut self, max: usize, tag: u8) -> MempoolResult<Vec<Digest>> {
        if (tag == OPT && self.opt_queue.is_empty()) || (tag == PES && self.pes_queue.is_empty()) {
            if let Some(payload) = self.payload_maker.make().await {
                let digest = payload.digest();
                self.process_own_payload(&digest, payload).await?;
                Ok(vec![digest])
            } else {
                Ok(Vec::new())
            }
        } else if tag == OPT {
            let digest_len = Digest::default().size();
            let digests = self
                .opt_queue
                .iter()
                .take(max / digest_len)
                .cloned()
                .collect();
            for x in &digests {
                self.opt_queue.remove(x); //去重
            }
            Ok(digests)
        } else {
            let digest_len = Digest::default().size();
            let digests = self
                .pes_queue
                .iter()
                .take(max / digest_len)
                .cloned()
                .collect();
            for x in &digests {
                self.pes_queue.remove(x); //去重
            }
            Ok(digests)
        }
    }

    async fn verify_payload(&mut self, block: Box<Block>, tag: u8) -> MempoolResult<bool> {
        self.synchronizer.verify_payload(*block, tag).await
    }

    async fn cleanup(&mut self, digests: Vec<Digest>, round: SeqNumber) {
        self.synchronizer.cleanup(round).await;
        for x in &digests {
            self.opt_queue.remove(x);
        }
    }

    pub async fn run(&mut self) {
        let log = |result: Result<&(), &MempoolError>| match result {
            Ok(()) => (),
            Err(MempoolError::StoreError(e)) => error!("{}", e),
            Err(MempoolError::SerializationError(e)) => error!("Store corrupted. {}", e),
            Err(e) => warn!("{}", e),
        };

        loop {
            let result = tokio::select! {
                Some(message) = self.core_channel.recv() => {
                    match message {
                        MempoolMessage::OwnPayload(payload) => self.handle_own_payload(payload).await, //处理本地生成的PayLoad,并向其他节点发送payload
                        MempoolMessage::Payload(payload) => self.handle_others_payload(payload).await,  //将其他人发送过来的payload存入本地
                        MempoolMessage::PayloadRequest(digest, sender) => self.handle_request(digest, sender).await,    //返回digest对应的payload
                    }
                },
                Some(message) = self.consensus_channel.recv() => {//处理共识发送的Payload请求
                    match message {
                        ConsensusMempoolMessage::Get(max, sender,tag) => {
                            let result = self.get_payload(max,tag).await;
                            log(result.as_ref().map(|_| &()));
                            let _ = sender.send(result.unwrap_or_default());
                        },
                        ConsensusMempoolMessage::Verify(block, sender,tag) => {
                            let result = self.verify_payload(block,tag).await;
                            log(result.as_ref().map(|_| &()));
                            let status = match result {
                                Ok(true) => PayloadStatus::Accept,
                                Ok(false) => PayloadStatus::Wait,
                                Err(_) => PayloadStatus::Reject,
                            };
                            let _ = sender.send(status);
                        },
                        ConsensusMempoolMessage::Cleanup(digests, round) => self.cleanup(digests, round).await,
                    }
                    Ok(())
                },
                else => break,
            };
            log(result.as_ref());
        }
    }
}
