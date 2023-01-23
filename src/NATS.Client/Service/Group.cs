// Copyright 2023 The NATS Authors
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

using NATS.Client.Internals;

namespace NATS.Client.Service
{
    /// <summary>
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public class Group
    {
        private Group _next;
        public string Name { get; }
        public Group Next => _next;
        public string Subject => _next == null ? Name : $"{Name}.{_next.Subject}";

        public Group(string name)
        {
            Name = Validator.ValidateSubject(name, "Group Name", true, true);
        }

        public Group AppendGroup(Group group)
        {
            Group last = this;
            while (last._next != null) {
                last = last._next;
            }
            last._next = group;
            return this;
        }
        
        public override string ToString()
        {
            return "Group [" + Subject.Replace('.', '/') + "]";
        }

        protected bool Equals(Group other)
        {
            return Name == other.Name && Equals(Next, other.Next);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Group)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name != null ? Name.GetHashCode() : 0) * 397) ^ (Next != null ? Next.GetHashCode() : 0);
            }
        }
    }
}
