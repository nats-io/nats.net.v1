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
    /// Group is way to organize endpoints by serving as a common prefix to all endpoints registered in it.
    /// </summary>
    public class Group
    {
        private Group _next;
        
        /// <value>Get the name of the group.</value>
        public string Name { get; }
        
        /// <value>Get the next group after this group. May be null.</value>
        public Group Next => _next;
        
        /// <value>The resolved subject of a group by concatenating the group name and any groups.</value>
        /// <summary>
        /// For example, this:
        /// <code>
        /// Group g = new Group("A")
        ///     .appendGroup(new Group("B"))
        ///     .appendGroup(new Group("C"))
        ///     .appendGroup(new Group("D"));
        /// System.out.println(g.getSubject());
        /// </code>
        /// prints "A.B.C.D"
        /// </summary>
        public string Subject => _next == null ? Name : $"{Name}.{_next.Subject}";

        /// <summary>
        /// Construct a group.
        /// <p>Group names and subjects are considered 'Restricted Terms' and must only contain A-Z, a-z, 0-9, '-' or '_'</p>
        /// </summary>
        /// <param name="name">the group name</param>
        public Group(string name)
        {
            Name = Validator.ValidateSubject(name, "Group Name", true, true);
        }

        /// <summary>
        /// Append a group at the end of the list of groups this group starts or is a part of.
        /// Appended groups can be traversed by doing <see cref="Next"/>
        /// Subsequent appends add the group to the end of the list.
        /// </summary>
        /// <param name="group">the group to append</param>
        /// <returns>like a fluent builder, return the Group instance</returns>
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
