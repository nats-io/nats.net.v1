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
        return 1 if f == 0
        return 0 if f != 0

        Preconditions:
           |f| bounded by 1.1*2^26,1.1*2^25,1.1*2^26,1.1*2^25,etc.
        */
        // Todo: Discuss this with upstream
        // Above comment is from the original code. But I believe the original code returned
        //   0 if f == 0
        //  -1 if f != 0
        // This code actually returns 0 if f==0 and 1 if f != 0
        internal static int fe_isnonzero(ref FieldElement f)
        {
            FieldElement fr;
            fe_reduce(out fr, ref f);
            int differentBits = 0;
            differentBits |= fr.x0;
            differentBits |= fr.x1;
            differentBits |= fr.x2;
            differentBits |= fr.x3;
            differentBits |= fr.x4;
            differentBits |= fr.x5;
            differentBits |= fr.x6;
            differentBits |= fr.x7;
            differentBits |= fr.x8;
            differentBits |= fr.x9;
            return (int)((unchecked((uint)differentBits - 1) >> 31) ^ 1);
        }
    }
}