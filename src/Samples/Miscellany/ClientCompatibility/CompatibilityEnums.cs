// Copyright 2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;

namespace NATSExamples.ClientCompatibility
{
    public enum Kind { Command, Result }
    public enum Suite { Done, ObjectStore }
    public enum Test
    {
        DefaultBucket,
        CustomBucket,
        GetObject,
        PutObject,
        UpdateMetadata,
        Watch,
        WatchUpdate,
        GetLink,
        PutLink
    }

    public static class CompatibilityEnums
    {
        public static string GetString(this Kind kind) 
        {
            switch (kind)
            {
                case Kind.Command: return "command";
                case Kind.Result:  return "result";
            }
            return null;
        }
        
        public static string GetString(this Suite suite) 
        {
            switch (suite)
            {
                case Suite.Done:        return "done";
                case Suite.ObjectStore: return "object-store";
            }
            return null;
        }
        
        public static string GetString(this Test test) 
        {
            switch (test)
            {
                case Test.DefaultBucket:  return "default-bucket";
                case Test.CustomBucket:   return "custom-bucket";
                case Test.GetObject:      return "get-object";
                case Test.PutObject:      return "put-object";
                case Test.UpdateMetadata: return "update-metadata";
                case Test.Watch:          return "watch";
                case Test.WatchUpdate:    return "watch-updates";
                case Test.GetLink:        return "get-link";
                case Test.PutLink:        return "put-link";
            }
            return null;
        }

        public static Kind GetKind(string value)
        {
            if (value != null)
            {
                switch (value.ToLower())
                {
                    case "command": return Kind.Command;
                    case "result":  return Kind.Result;
                }
            }

            throw new ArgumentException($"Unknown kind {value}");
        }

        public static Suite GetSuite(string value)
        {
            if (value != null)
            {
                switch (value.ToLower())
                {
                    case "done":         return Suite.Done;
                    case "object-store": return Suite.ObjectStore;
                }
            }

            throw new ArgumentException($"Unknown suite {value}");
        }
        
        public static Test GetTest(string value) 
        {
            if (value != null)
            {
                switch (value.ToLower())
                {
                    case "default-bucket":  return Test.DefaultBucket;
                    case "custom-bucket":   return Test.CustomBucket;
                    case "get-object":      return Test.GetObject;
                    case "put-object":      return Test.PutObject;
                    case "update-metadata": return Test.UpdateMetadata;
                    case "watch":           return Test.Watch;
                    case "watch-updates":   return Test.WatchUpdate;
                    case "get-link":        return Test.GetLink;
                    case "put-link":        return Test.PutLink;
                }
            }

            throw new ArgumentException($"Unknown suite {value}");
        }
    }
}
