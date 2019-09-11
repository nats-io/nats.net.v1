// Copyright 2019 The NATS Authors
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

// Borrowed from https://github.com/CryptoManiac/Ed25519

using System;

namespace NATS.Client.NaCl.Internal.Ed25519Ref10
{
    internal struct FieldElement
    {
        internal int x0, x1, x2, x3, x4, x5, x6, x7, x8, x9;

        internal FieldElement(params int[] elements)
        {
            // Contract.Requires<ArgumentException>(elements.Length == 10);
            if (elements.Length != 10) throw new ArgumentException();

            x0 = elements[0];
            x1 = elements[1];
            x2 = elements[2];
            x3 = elements[3];
            x4 = elements[4];
            x5 = elements[5];
            x6 = elements[6];
            x7 = elements[7];
            x8 = elements[8];
            x9 = elements[9];
        }
    }
}
