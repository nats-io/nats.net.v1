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

using NATS.Client.Internals;

namespace NATS.Client.KeyValue
{
    public sealed class KeyValuePurgeOptions
    {
        public static long DefaultThresholdMillis = Duration.OfMinutes(30).Millis;

        public long DeleteMarkersThresholdMillis { get; }

        private KeyValuePurgeOptions(long deleteMarkersThresholdMillis)
        {
            DeleteMarkersThresholdMillis = deleteMarkersThresholdMillis;
        }
       
        /// <summary>
        /// Gets a KeyValuePurgeOptions builder.
        /// </summary>
        /// <returns>
        /// The builder
        /// </returns>
        public static KeyValuePurgeOptionsBuilder Builder()
        {
            return new KeyValuePurgeOptionsBuilder();
        }

        public sealed class KeyValuePurgeOptionsBuilder
        {
            private long _deleteMarkersThresholdMillis;

            /// <summary>
            /// Construct a builder
            /// </summary>
            public KeyValuePurgeOptionsBuilder() {}
            
            /// <summary>
            /// Set the delete marker threshold.
            /// 0 will assume the default threshold.
            /// Less than zero will assume no threshold and will not keep any markers.
            /// </summary>
            /// <param name="thresholdMillis">the threshold millis</param>
            /// <returns>The KeyValuePurgeOptionsBuilder</returns>
            public KeyValuePurgeOptionsBuilder WithDeleteMarkersThresholdMillis(long thresholdMillis)
            {
                _deleteMarkersThresholdMillis = thresholdMillis;
                return this;
            }
            
            /// <summary>
            /// Set the delete marker threshold.
            /// null or duration of 0 will assume the default threshold.
            /// Duration less than zero will assume no threshold and will not keep any markers.
            /// </summary>
            /// <param name="threshold">the threshold duration or null</param>
            /// <returns>The KeyValuePurgeOptionsBuilder</returns>
            public KeyValuePurgeOptionsBuilder WithDeleteMarkersThreshold(Duration threshold)
            {
                _deleteMarkersThresholdMillis = threshold?.Millis ?? DefaultThresholdMillis;
                return this;
            }
            
            /// <summary>
            /// Set the delete marker threshold to -1 so as to not keep any markers
            /// </summary>
            /// <returns>The KeyValuePurgeOptionsBuilder</returns>
            public KeyValuePurgeOptionsBuilder WithDeleteMarkersNoThreshold()
            {
                _deleteMarkersThresholdMillis = -1;
                return this;
            }

            /// <summary>
            /// Builds the KeyValueOptions
            /// </summary>
            /// <returns>The KeyValueOptions object.</returns>
            public KeyValuePurgeOptions Build()
            {
                if (_deleteMarkersThresholdMillis < 0) {
                    _deleteMarkersThresholdMillis = -1;
                }
                else if (_deleteMarkersThresholdMillis == 0) {
                    _deleteMarkersThresholdMillis = DefaultThresholdMillis;
                }
                return new KeyValuePurgeOptions(_deleteMarkersThresholdMillis);
            }
        }
    }
}
