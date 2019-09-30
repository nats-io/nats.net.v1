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
	internal static partial class GroupOperations
	{
		public static void ge_tobytes(byte[] s, int offset, ref  GroupElementP2 h)
		{
			FieldElement recip;
			FieldElement x, y;

			FieldOperations.fe_invert(out recip, ref h.Z);
			FieldOperations.fe_mul(out x, ref h.X, ref recip);
			FieldOperations.fe_mul(out y, ref h.Y, ref recip);
			FieldOperations.fe_tobytes(s, offset, ref y);
			s[offset + 31] ^= (byte)(FieldOperations.fe_isnegative(ref x) << 7);
		}
	}
}