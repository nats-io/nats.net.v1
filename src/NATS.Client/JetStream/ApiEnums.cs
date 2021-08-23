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
    public enum AckPolicy { None, All, Explicit }

    public enum DeliverPolicy { All, Last, New, ByStartSequence, ByStartTime, LastPerSubject }

    public enum DiscardPolicy { New, Old }

    public enum ReplayPolicy { Instant, Original }

    public enum RetentionPolicy { Limits, Interest, WorkQueue }

    public enum StorageType { File, Memory }

    public static class ApiEnums
    {
        public static string GetString(this AckPolicy ackPolicy)
        {
            switch (ackPolicy)
            {
                case AckPolicy.None:     return "none";
                case AckPolicy.All:      return "all";
                case AckPolicy.Explicit: return "explicit";
            }
            return null;
        }
        
        public static string GetString(this DeliverPolicy deliverPolicy)
        {
            switch (deliverPolicy)
            {
                case DeliverPolicy.All:             return "all";
                case DeliverPolicy.Last:            return "last";
                case DeliverPolicy.New:             return "new";
                case DeliverPolicy.ByStartSequence: return "by_start_sequence";
                case DeliverPolicy.ByStartTime:     return "by_start_time";
                case DeliverPolicy.LastPerSubject:  return "last_per_subject";
            }
            return null;
        }
        
        public static string GetString(this DiscardPolicy discardPolicy)
        {
            switch (discardPolicy)
            {
                case DiscardPolicy.New: return "new";
                case DiscardPolicy.Old: return "old";
            }
            return null;
        }
        
        public static string GetString(this ReplayPolicy replayPolicy)
        {
            switch (replayPolicy)
            {
                case ReplayPolicy.Instant:  return "instant";
                case ReplayPolicy.Original: return "original";
            }
            return null;
        }

        public static string GetString(this RetentionPolicy retentionPolicy)
        {
            switch (retentionPolicy)
            {
                case RetentionPolicy.Limits:    return "limits";
                case RetentionPolicy.Interest:  return "interest";
                case RetentionPolicy.WorkQueue: return "workqueue";
            }
            return null;
        }
        
        public static string GetString(this StorageType storageType)
        {
            switch (storageType)
            {
                case StorageType.File:   return "file";
                case StorageType.Memory: return "memory";
            }
            return null;
        }

        public static AckPolicy GetValueOrDefault(string value, AckPolicy aDefault)
        {
            if (value != null)
            {
                switch (value)
                {
                    case "none":     return AckPolicy.None;
                    case "all":      return AckPolicy.All;
                    case "explicit": return AckPolicy.Explicit;
                }
            }

            return aDefault;
        }

        public static DeliverPolicy GetValueOrDefault(string value, DeliverPolicy aDefault)
        {
            if (value != null)
            {
                switch (value)
                {
                    case "all":               return DeliverPolicy.All;
                    case "last":              return DeliverPolicy.Last;
                    case "new":               return DeliverPolicy.New;
                    case "by_start_sequence": return DeliverPolicy.ByStartSequence;
                    case "by_start_time":     return DeliverPolicy.ByStartTime;
                }
            }

            return aDefault;
        }

        public static DiscardPolicy GetValueOrDefault(string value, DiscardPolicy aDefault)
        {
            if (value != null)
            {
                switch (value)
                {
                    case "new": return DiscardPolicy.New;
                    case "old": return DiscardPolicy.Old;
                }
            }

            return aDefault;
        }

        public static ReplayPolicy GetValueOrDefault(string value, ReplayPolicy aDefault)
        {
            if (value != null)
            {
                switch (value)
                {
                    case "instant": return ReplayPolicy.Instant;
                    case "original": return ReplayPolicy.Original;
                }
            }

            return aDefault;
        }

        public static RetentionPolicy GetValueOrDefault(string value, RetentionPolicy aDefault)
        {
            if (value != null)
            {
                switch (value)
                {
                    case "limits": return RetentionPolicy.Limits;
                    case "interest": return RetentionPolicy.Interest;
                    case "workqueue": return RetentionPolicy.WorkQueue;
                }
            }

            return aDefault;
        }

        public static StorageType GetValueOrDefault(string value, StorageType aDefault)
        {
            if (value != null)
            {
                switch (value)
                {
                    case "file": return StorageType.File;
                    case "memory": return StorageType.Memory;
                }
            }

            return aDefault;
        }
    }
}
