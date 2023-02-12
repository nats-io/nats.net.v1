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
using System.Collections.Generic;

namespace NATS.Client.Internals
{
    public static class MiscUtils
    {
        public static Random InsecureRandom { get; } = new Random();

        // Convenience method to shuffle a list. The list passed is modified.
        public static void ShuffleInPlace<T>(IList<T> list)
        {
            if (list == null)
            {
                return;
            }

            int n = list.Count;
            if (n < 2)
            {
                return;
            }

            while (n > 1)
            {
                n--;
                int k = InsecureRandom.Next(n + 1);
                var value = list[k];
                list[k] = list[n];
                list[n] = value;
            }
        }
    }
}