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
    internal static partial class FieldOperations
    {
        /*
        return 1 if f is in {1,3,5,...,q-2}
        return 0 if f is in {0,2,4,...,q-1}

        Preconditions:
        |f| bounded by 1.1*2^26,1.1*2^25,1.1*2^26,1.1*2^25,etc.
        */
        //int fe_isnegative(const fe f)
        public static int fe_isnegative(ref FieldElement f)
        {
            FieldElement fr;
            fe_reduce(out fr, ref f);
            return fr.x0 & 1;
        }
    }
}