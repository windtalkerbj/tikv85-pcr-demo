// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! PCR protobuf message types — local stubs for prototype compilation.
//!
//! These mirror `proto/pcrpb/pcrpb.proto`. Replace with `kvproto::pcrpb`
//! when the upstream crate is published.
//!
//! NOTE: `Message` trait is implemented with minimal stubs (zero-size, no-op
//! serialization) — sufficient for grpcio type system requirements but NOT
//! for actual wire protocol use. Replace before production.

use protobuf::RepeatedField;

/// Helper macro to implement protobuf::Message with no-op stubs.
/// Matches TiKV's pingcap/rust-protobuf fork API.
macro_rules! impl_message_stub {
    ($t:ty) => {
        impl ::protobuf::Message for $t {
            fn compute_size(&self) -> u32 { 0 }
            fn get_cached_size(&self) -> u32 { 0 }
            fn write_to_with_cached_sizes(&self, _: &mut ::protobuf::CodedOutputStream)
                -> ::protobuf::ProtobufResult<()> { Ok(()) }
            fn merge_from(&mut self, _: &mut ::protobuf::CodedInputStream)
                -> ::protobuf::ProtobufResult<()> { Ok(()) }
            fn is_initialized(&self) -> bool { true }
            fn new() -> Self { Self::default() }
            fn descriptor_static() -> &'static ::protobuf::reflect::MessageDescriptor {
                panic!("PCR stub: descriptor_static")
            }
            fn default_instance() -> &'static Self {
                Box::leak(Box::new(Self::default()))
            }
            fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
                Self::descriptor_static()
            }
            fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
                unimplemented!("PCR stub")
            }
            fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
                unimplemented!("PCR stub")
            }
            fn as_any(&self) -> &dyn ::std::any::Any { self }
        }
        impl ::protobuf::Clear for $t {
            fn clear(&mut self) { *self = Self::default() }
        }
    };
}

// ======================================================================
// PcrSubscribeRequest
// ======================================================================
#[derive(PartialEq, Clone, Default, Debug)]
pub struct PcrSubscribeRequest {
    pub stream_id: u64,
    pub start_ts: u64,
    pub partition: Option<PcrPartitionSpec>,
}
impl PcrSubscribeRequest {
    pub fn new() -> Self { Self::default() }
    pub fn get_stream_id(&self) -> u64 { self.stream_id }
    pub fn set_stream_id(&mut self, v: u64) { self.stream_id = v; }
    pub fn get_start_ts(&self) -> u64 { self.start_ts }
    pub fn set_start_ts(&mut self, v: u64) { self.start_ts = v; }
    pub fn get_partition(&self) -> Option<&PcrPartitionSpec> { self.partition.as_ref() }
    pub fn set_partition(&mut self, v: PcrPartitionSpec) { self.partition = Some(v); }
}

// ======================================================================
// PcrPartitionSpec
// ======================================================================
#[derive(PartialEq, Clone, Default, Debug)]
pub struct PcrPartitionSpec {
    pub region_id: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub region_epoch_conf_ver: u64,
    pub region_epoch_version: u64,
}
impl PcrPartitionSpec {
    pub fn new() -> Self { Self::default() }
    pub fn get_region_id(&self) -> u64 { self.region_id }
    pub fn set_region_id(&mut self, v: u64) { self.region_id = v; }
    pub fn get_start_key(&self) -> &[u8] { &self.start_key }
    pub fn set_start_key(&mut self, v: Vec<u8>) { self.start_key = v; }
    pub fn get_end_key(&self) -> &[u8] { &self.end_key }
    pub fn set_end_key(&mut self, v: Vec<u8>) { self.end_key = v; }
    pub fn get_region_epoch_conf_ver(&self) -> u64 { self.region_epoch_conf_ver }
    pub fn set_region_epoch_conf_ver(&mut self, v: u64) { self.region_epoch_conf_ver = v; }
    pub fn get_region_epoch_version(&self) -> u64 { self.region_epoch_version }
    pub fn set_region_epoch_version(&mut self, v: u64) { self.region_epoch_version = v; }
}

// ======================================================================
// PcrEvent + oneof
// ======================================================================
#[derive(PartialEq, Clone, Debug)]
pub enum PcrEvent_oneof_event {
    kv_batch(PcrKvBatch),
    sst_chunk(PcrSstChunk),
    checkpoint(PcrCheckpoint),
    delete_range(PcrDeleteRange),
    split(PcrSplit),
}

#[derive(PartialEq, Clone, Default, Debug)]
pub struct PcrEvent {
    pub stream_seq: u64,
    pub event: Option<PcrEvent_oneof_event>,
}
impl PcrEvent {
    pub fn new() -> Self { Self::default() }
    pub fn get_stream_seq(&self) -> u64 { self.stream_seq }
    pub fn set_stream_seq(&mut self, v: u64) { self.stream_seq = v; }
    pub fn has_kv_batch(&self) -> bool { matches!(&self.event, Some(PcrEvent_oneof_event::kv_batch(_))) }
    pub fn get_kv_batch(&self) -> &PcrKvBatch {
        match &self.event { Some(PcrEvent_oneof_event::kv_batch(b)) => b, _ => panic!("not kv_batch") }
    }
    pub fn set_kv_batch(&mut self, v: PcrKvBatch) { self.event = Some(PcrEvent_oneof_event::kv_batch(v)); }
    pub fn has_sst_chunk(&self) -> bool { matches!(&self.event, Some(PcrEvent_oneof_event::sst_chunk(_))) }
    pub fn get_sst_chunk(&self) -> &PcrSstChunk {
        match &self.event { Some(PcrEvent_oneof_event::sst_chunk(s)) => s, _ => panic!("not sst_chunk") }
    }
    pub fn set_sst_chunk(&mut self, v: PcrSstChunk) { self.event = Some(PcrEvent_oneof_event::sst_chunk(v)); }
    pub fn has_checkpoint(&self) -> bool { matches!(&self.event, Some(PcrEvent_oneof_event::checkpoint(_))) }
    pub fn get_checkpoint(&self) -> &PcrCheckpoint {
        match &self.event { Some(PcrEvent_oneof_event::checkpoint(c)) => c, _ => panic!("not checkpoint") }
    }
    pub fn set_checkpoint(&mut self, v: PcrCheckpoint) { self.event = Some(PcrEvent_oneof_event::checkpoint(v)); }
    pub fn has_delete_range(&self) -> bool { matches!(&self.event, Some(PcrEvent_oneof_event::delete_range(_))) }
    pub fn get_delete_range(&self) -> &PcrDeleteRange {
        match &self.event { Some(PcrEvent_oneof_event::delete_range(d)) => d, _ => panic!("not delete_range") }
    }
    pub fn set_delete_range(&mut self, v: PcrDeleteRange) { self.event = Some(PcrEvent_oneof_event::delete_range(v)); }
    pub fn has_split(&self) -> bool { matches!(&self.event, Some(PcrEvent_oneof_event::split(_))) }
    pub fn get_split(&self) -> &PcrSplit {
        match &self.event { Some(PcrEvent_oneof_event::split(s)) => s, _ => panic!("not split") }
    }
    pub fn set_split(&mut self, v: PcrSplit) { self.event = Some(PcrEvent_oneof_event::split(v)); }
}

// ======================================================================
// PcrKvBatch
// ======================================================================
#[derive(PartialEq, Clone, Default, Debug)]
pub struct PcrKvBatch {
    pub kvs: RepeatedField<PcrKV>,
}
impl PcrKvBatch {
    pub fn new() -> Self { Self::default() }
    pub fn get_kvs(&self) -> &[PcrKV] { &self.kvs }
    pub fn mut_kvs(&mut self) -> &mut RepeatedField<PcrKV> { &mut self.kvs }
    pub fn set_kvs(&mut self, v: RepeatedField<PcrKV>) { self.kvs = v; }
}

// ======================================================================
// PcrKV
// ======================================================================
#[derive(Clone, PartialEq, Debug)]
pub enum OpType { PUT = 0, DELETE = 1 }
impl Default for OpType { fn default() -> Self { OpType::PUT } }

#[derive(PartialEq, Clone, Default, Debug)]
pub struct PcrKV {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub op: i32,
}
impl PcrKV {
    pub fn new() -> Self { Self::default() }
    pub fn get_key(&self) -> &[u8] { &self.key }
    pub fn set_key(&mut self, v: Vec<u8>) { self.key = v; }
    pub fn get_value(&self) -> &[u8] { &self.value }
    pub fn set_value(&mut self, v: Vec<u8>) { self.value = v; }
    pub fn get_op(&self) -> OpType { if self.op == 1 { OpType::DELETE } else { OpType::PUT } }
    pub fn set_op(&mut self, v: OpType) { self.op = v as i32; }
}

// ======================================================================
// PcrSstChunk
// ======================================================================
#[derive(PartialEq, Clone, Default, Debug)]
pub struct PcrSstChunk {
    pub data: Vec<u8>,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub write_ts: u64,
}
impl PcrSstChunk {
    pub fn new() -> Self { Self::default() }
    pub fn get_data(&self) -> &[u8] { &self.data }
    pub fn set_data(&mut self, v: Vec<u8>) { self.data = v; }
    pub fn get_start_key(&self) -> &[u8] { &self.start_key }
    pub fn set_start_key(&mut self, v: Vec<u8>) { self.start_key = v; }
    pub fn get_end_key(&self) -> &[u8] { &self.end_key }
    pub fn set_end_key(&mut self, v: Vec<u8>) { self.end_key = v; }
    pub fn get_write_ts(&self) -> u64 { self.write_ts }
    pub fn set_write_ts(&mut self, v: u64) { self.write_ts = v; }
}

// ======================================================================
// PcrCheckpoint
// ======================================================================
#[derive(PartialEq, Clone, Default, Debug)]
pub struct PcrCheckpoint {
    pub resolved_ts: u64,
    pub region_ids: Vec<u64>,
}
impl PcrCheckpoint {
    pub fn new() -> Self { Self::default() }
    pub fn get_resolved_ts(&self) -> u64 { self.resolved_ts }
    pub fn set_resolved_ts(&mut self, v: u64) { self.resolved_ts = v; }
    pub fn get_region_ids(&self) -> &[u64] { &self.region_ids }
    pub fn set_region_ids(&mut self, v: Vec<u64>) { self.region_ids = v; }
    pub fn mut_region_ids(&mut self) -> &mut Vec<u64> { &mut self.region_ids }
}

// ======================================================================
// PcrDeleteRange
// ======================================================================
#[derive(PartialEq, Clone, Default, Debug)]
pub struct PcrDeleteRange {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub ts: u64,
}
impl PcrDeleteRange {
    pub fn new() -> Self { Self::default() }
    pub fn get_start_key(&self) -> &[u8] { &self.start_key }
    pub fn set_start_key(&mut self, v: Vec<u8>) { self.start_key = v; }
    pub fn get_end_key(&self) -> &[u8] { &self.end_key }
    pub fn set_end_key(&mut self, v: Vec<u8>) { self.end_key = v; }
    pub fn get_ts(&self) -> u64 { self.ts }
    pub fn set_ts(&mut self, v: u64) { self.ts = v; }
}

// ======================================================================
// PcrSplit
// ======================================================================
#[derive(PartialEq, Clone, Default, Debug)]
pub struct PcrSplit {
    pub split_key: Vec<u8>,
    pub new_region_ids: Vec<u64>,
}
impl PcrSplit {
    pub fn new() -> Self { Self::default() }
    pub fn get_split_key(&self) -> &[u8] { &self.split_key }
    pub fn set_split_key(&mut self, v: Vec<u8>) { self.split_key = v; }
    pub fn get_new_region_ids(&self) -> &[u64] { &self.new_region_ids }
    pub fn set_new_region_ids(&mut self, v: Vec<u64>) { self.new_region_ids = v; }
}

// ======================================================================
// Helper constructors
// ======================================================================
pub fn make_kv_batch_event(seq: u64, kvs: Vec<PcrKV>) -> PcrEvent {
    let mut event = PcrEvent::new();
    event.set_stream_seq(seq);
    event.set_kv_batch(PcrKvBatch { kvs: RepeatedField::from_vec(kvs) });
    event
}
pub fn make_checkpoint_event(seq: u64, resolved_ts: u64, region_ids: Vec<u64>) -> PcrEvent {
    let mut event = PcrEvent::new();
    event.set_stream_seq(seq);
    event.set_checkpoint(PcrCheckpoint { resolved_ts, region_ids });
    event
}
pub fn make_sst_chunk_event(seq: u64, data: Vec<u8>, start_key: Vec<u8>, end_key: Vec<u8>, write_ts: u64) -> PcrEvent {
    let mut event = PcrEvent::new();
    event.set_stream_seq(seq);
    event.set_sst_chunk(PcrSstChunk { data, start_key, end_key, write_ts });
    event
}
pub fn make_delete_range_event(seq: u64, start_key: Vec<u8>, end_key: Vec<u8>, ts: u64) -> PcrEvent {
    let mut event = PcrEvent::new();
    event.set_stream_seq(seq);
    event.set_delete_range(PcrDeleteRange { start_key, end_key, ts });
    event
}
pub fn make_split_event(seq: u64, split_key: Vec<u8>, new_region_ids: Vec<u64>) -> PcrEvent {
    let mut event = PcrEvent::new();
    event.set_stream_seq(seq);
    event.set_split(PcrSplit { split_key, new_region_ids });
    event
}

// ======================================================================
// Message trait stubs — satisfy grpcio type bounds
// ======================================================================
impl_message_stub!(PcrSubscribeRequest);
impl_message_stub!(PcrPartitionSpec);
impl_message_stub!(PcrEvent);
impl_message_stub!(PcrKvBatch);
impl_message_stub!(PcrKV);
impl_message_stub!(PcrSstChunk);
impl_message_stub!(PcrCheckpoint);
impl_message_stub!(PcrDeleteRange);
impl_message_stub!(PcrSplit);
