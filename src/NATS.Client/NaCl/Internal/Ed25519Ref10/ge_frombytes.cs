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
		public static int ge_frombytes_negate_vartime(out GroupElementP3 h, byte[] data, int offset)
		{
			FieldElement u, v, v3, vxx, check;

			FieldOperations.fe_frombytes(out h.Y, data, offset);
			FieldOperations.fe_1(out h.Z);
			FieldOperations.fe_sq(out u, ref  h.Y);
			FieldOperations.fe_mul(out v, ref  u, ref  LookupTables.d);
			FieldOperations.fe_sub(out u, ref  u, ref  h.Z);       /* u = y^2-1 */
			FieldOperations.fe_add(out v, ref  v, ref  h.Z);       /* v = dy^2+1 */

			FieldOperations.fe_sq(out v3, ref  v);
			FieldOperations.fe_mul(out v3, ref  v3, ref  v);        /* v3 = v^3 */
			FieldOperations.fe_sq(out h.X, ref  v3);
			FieldOperations.fe_mul(out h.X, ref  h.X, ref  v);
			FieldOperations.fe_mul(out h.X, ref  h.X, ref  u);    /* x = uv^7 */

			FieldOperations.fe_pow22523(out h.X, ref  h.X); /* x = (uv^7)^((q-5)/8) */
			FieldOperations.fe_mul(out h.X, ref  h.X, ref  v3);
			FieldOperations.fe_mul(out h.X, ref  h.X, ref  u);    /* x = uv^3(uv^7)^((q-5)/8) */

			FieldOperations.fe_sq(out vxx, ref  h.X);
			FieldOperations.fe_mul(out vxx, ref  vxx, ref  v);
			FieldOperations.fe_sub(out check, ref  vxx, ref  u);    /* vx^2-u */
			if (FieldOperations.fe_isnonzero(ref check) != 0)
			{
				FieldOperations.fe_add(out check, ref  vxx, ref  u);  /* vx^2+u */
				if (FieldOperations.fe_isnonzero(ref check) != 0)
				{
					h = default(GroupElementP3);
					return -1;
				}
				FieldOperations.fe_mul(out h.X, ref  h.X, ref  LookupTables.sqrtm1);
			}

			if (FieldOperations.fe_isnegative(ref h.X) == (data[offset + 31] >> 7))
				FieldOperations.fe_neg(out h.X, ref h.X);

			FieldOperations.fe_mul(out h.T, ref h.X, ref h.Y);
			return 0;
		}

	}
}