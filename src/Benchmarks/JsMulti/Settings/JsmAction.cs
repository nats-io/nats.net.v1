// Copyright 2022 The NATS Authors
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

using System.Collections.Generic;

namespace JsMulti.Settings
{
    public class JsmAction
    {
        public string Label { get; }
        public bool IsPubAction { get; }
        public bool IsPubSync { get; }
        public bool IsSubAction { get; }
        public bool IsPush { get; }
        public bool IsPull { get; }
        public bool IsQueue { get; }

        JsmAction(string label, bool isPubSync) {
            Label = label;
            IsPubAction = true;
            IsPubSync = isPubSync;
            IsSubAction = false;
            IsPush = false;
            IsPull = false;
            IsQueue = false;
            _actions[Label.ToLower()] = this;
        }

        JsmAction(string label, bool isPush, bool isQueue) {
            Label = label;
            IsPubAction = false;
            IsPubSync = false;
            IsSubAction = true;
            IsPush = isPush;
            IsPull = !isPush;
            IsQueue = isQueue;
            _actions[Label.ToLower()] = this;
        }

        private static readonly Dictionary<string, JsmAction> _actions = new Dictionary<string, JsmAction>();

        public const string PubSyncLabel = "PubSync";
        public const string PubAsyncLabel = "PubAsync";
        public const string PubCoreLabel = "PubCore";
        public const string SubPushLabel = "SubPush";
        public const string SubQueueLabel = "SubQueue";
        public const string SubPullLabel = "SubPull";
        public const string SubPullQueueLabel = "SubPullQueue";
        
        public static readonly JsmAction PubSync = new JsmAction(PubSyncLabel, true);
        public static readonly JsmAction PubAsync = new JsmAction(PubAsyncLabel, false);
        public static readonly JsmAction PubCore = new JsmAction(PubCoreLabel, true);
        public static readonly JsmAction SubPush = new JsmAction(SubPushLabel, true, false);
        public static readonly JsmAction SubQueue = new JsmAction(SubQueueLabel, true, true);
        public static readonly JsmAction SubPull = new JsmAction(SubPullLabel, false, false);
        public static readonly JsmAction SubPullQueue = new JsmAction(SubPullQueueLabel, false, true);
        
        public static JsmAction GetInstance(string s)
        {
            JsmAction act;
            if (_actions.TryGetValue(s.ToLower(), out act))
            {
                return act;
            }

            return null;
        }

        public override bool Equals(object obj) => obj is JsmAction other && Label.Equals(other.Label);

        public override int GetHashCode() => Label.GetHashCode();

        public bool Equals(JsmAction other) {
            return Label.Equals(other.Label);
        }

        public override string ToString()
        {
            return Label;
        }
        
    }
}