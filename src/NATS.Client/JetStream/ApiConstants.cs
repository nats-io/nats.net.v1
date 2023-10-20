// Copyright 2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace NATS.Client.JetStream
{
    public static class ApiConstants
    {
        public const string AckFloor = "ack_floor";
        public const string AckPolicy = "ack_policy";
        public const string AckWait = "ack_wait";
        public const string Active = "active";
        public const string AllowRollupHdrs = "allow_rollup_hdrs";
        public const string AllowDirect = "allow_direct";
        public const string Api = "api";
        public const string AuthRequired = "auth_required";
        public const string AverageProcessingTime = "average_processing_time";        
        public const string Backoff = "backoff";
        public const string Batch = "batch";
        public const string Bucket = "bucket";
        public const string Bytes = "bytes";
        public const string Chunks = "chunks";
        public const string ClientId = "client_id";
        public const string ClientIp = "client_ip";
        public const string Cluster = "cluster";
        public const string Code = "code";
        public const string Compression = "compression";
        public const string Config = "config";
        public const string ConnectUrls = "connect_urls";
        public const string ConsumerCount = "consumer_count";
        public const string ConsumerLimits = "consumer_limits";
        public const string ConsumerSeq = "consumer_seq";
        public const string Consumers = "consumers";
        public const string Created = "created";
        public const string Current = "current";
        public const string Data = "data";
        public const string Deleted = "deleted";
        public const string DeletedDetails = "deleted_details";
        public const string Deliver = "deliver";
        public const string DeliverGroup = "deliver_group";
        public const string DeliverPolicy = "deliver_policy";
        public const string DeliverSubject = "deliver_subject";
        public const string Delivered = "delivered";
        public const string DenyDelete = "deny_delete";
        public const string DenyPurge = "deny_purge";
        public const string Description = "description";
        public const string Dest = "dest";
        public const string Digest = "digest";
        public const string Discard = "discard";
        public const string DiscardNewPerSubject = "discard_new_per_subject";
        public const string Domain = "domain";
        public const string Duplicate = "duplicate";
        public const string DuplicateWindow = "duplicate_window";
        public const string DurableName = "durable_name";
        public const string Endpoints = "endpoints";
        public const string ErrCode = "err_code";
        public const string Error = "error";
        public const string Errors = "errors";
        public const string Expires = "expires";
        public const string External = "external";
        public const string Filter = "filter";
        public const string FilterSubject = "filter_subject";
        public const string FilterSubjects = "filter_subjects";
        public const string FirstSequence = "first_seq";
        public const string FirstTs = "first_ts";
        public const string FlowControl = "flow_control";
        public const string Go = "go";
        public const string Hdrs = "hdrs";
        public const string Headers = "headers";
        public const string HeadersOnly = "headers_only";
        public const string Host = "host";
        public const string Id = "id";
        public const string IdleHeartbeat = "idle_heartbeat";
        public const string InactiveThreshold = "inactive_threshold";
        public const string Internal = "internal";
        public const string Jetstream = "jetstream";
        public const string Keep = "keep";
        public const string Lag = "lag";
        public const string LameDuckMode = "ldm";
        public const string LastActive = "last_active";
        public const string LastBySubject = "last_by_subj";
        public const string LastError = "last_error";
        public const string LastSeq = "last_seq";
        public const string LastTs = "last_ts";
        public const string Leader = "leader";
        public const string Limit = "limit";
        public const string Limits = "limits";
        public const string Link = "link";
        public const string Lost = "lost";
        public const string MaxAckPending = "max_ack_pending";
        public const string MaxAge = "max_age";
        public const string MaxBatch = "max_batch";
        public const string MaxBytes = "max_bytes";
        public const string MaxBytesRequired = "max_bytes_required";
        public const string MaxConsumers = "max_consumers";
        public const string MaxChunkSize = "max_chunk_size";
        public const string MaxDeliver = "max_deliver";
        public const string MaxExpires = "max_expires";
        public const string MaxMemory = "max_memory";
        public const string MaxMsgSize = "max_msg_size";
        public const string MaxMsgs = "max_msgs";
        public const string MaxMsgsPerSubject = "max_msgs_per_subject";
        public const string MaxPayload = "max_payload";
        public const string MaxStorage = "max_storage";
        public const string MaxStreams = "max_streams";
        public const string MaxWaiting = "max_waiting";
        public const string Memory = "memory";
        public const string MemoryMaxStreamBytes = "memory_max_stream_bytes";
        public const string MemStorage = "mem_storage";
        public const string Message = "message";
        public const string Messages = "messages";
        public const string Metadata = "metadata";
        public const string Mirror = "mirror";
        public const string MirrorDirect = "mirror_direct";
        public const string Msgs = "msgs";
        public const string Name = "name";
        public const string NextBySubject = "next_by_subj";
        public const string NoAck = "no_ack";
        public const string NoErase = "no_erase";
        public const string Nonce = "nonce";
        public const string NoWait = "no_wait";
        public const string Nuid = "nuid";
        public const string NumAckPending = "num_ack_pending";
        public const string NumDeleted = "num_deleted";
        public const string NumErrors = "num_errors";
        public const string NumPending = "num_pending";
        public const string NumRedelivered = "num_redelivered";
        public const string NumReplicas = "num_replicas";
        public const string NumRequests = "num_requests";
        public const string NumSubjects = "num_subjects";
        public const string NumWaiting = "num_waiting";
        public const string Offline = "offline";
        public const string Offset = "offset";
        public const string Options = "options";
        public const string OptStartSeq = "opt_start_seq";
        public const string OptStartTime = "opt_start_time";
        public const string Placement = "placement";
        public const string ProcessingTime = "processing_time";
        public const string Republish = "republish";
        public const string Port = "port";
        public const string Proto = "proto";
        public const string Purged = "purged";
        public const string PushBound = "push_bound";
        public const string QueueGroup = "queue_group";
        public const string RateLimitBps = "rate_limit_bps";
        public const string ReplayPolicy = "replay_policy";
        public const string Replica = "replica";
        public const string Replicas = "replicas";
        public const string Request = "request";
        public const string Response = "response";
        public const string Retention = "retention";
        public const string SampleFreq = "sample_freq";
        public const string Schema = "schema";
        public const string Sealed = "sealed";
        public const string Seq = "seq";
        public const string ServerId = "server_id";
        public const string ServerName = "server_name";
        public const string Size = "size";
        public const string Source = "source";
        public const string Sources = "sources";
        public const string Src = "src";
        public const string Started = "started";
        public const string State = "state";
        public const string Stats = "stats";
        public const string Storage = "storage";
        public const string StorageMaxStreamBytes = "storage_max_stream_bytes";
        public const string StreamName = "stream_name";
        public const string StreamSeq = "stream_seq";
        public const string Stream = "stream";
        public const string Streams = "streams";
        public const string Subject = "subject";
        public const string SubjectTransform = "subject_transform";
        public const string SubjectTransforms = "subject_transforms";
        public const string Subjects = "subjects";
        public const string SubjectsFilter = "subjects_filter";
        public const string Success = "success";
        public const string Tags = "tags";
        public const string TemplateOwner = "template_owner";
        public const string Tiers = "tiers";
        public const string Time = "time";
        public const string Timestamp = "ts";
        public const string Tls = "tls_required";
        public const string Total = "total";
        public const string Type = "type";
        public const string Version = "version";
    }
}
