
// Copyright 2021 The NATS Authors
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

namespace NATS.Client.JetStream
{
    /// <summary>
    /// The exception thrown when there is an error with JetStream.
    /// </summary>
    public class NATSJetStreamException : NATSException
    {
        /// <summary>
        /// Unspecified error code.
        /// </summary>
        public static int ErrorCodeUnspecified = -1;

        /// <summary>
        /// Gets the error code returned by JetStream.  Returns ErrorCodeUnspecified
        /// if the error is local.
        /// </summary>
        public int ErrorCode { get; }

        /// <summary>
        /// Gets the error code returned by JetStream.  Returns ErrorCodeUnspecified
        /// if the error is local.
        /// </summary>
        public int ApiErrorCode { get; }

        /// <summary>
        /// Returns the description of the error.
        /// </summary>
        public string ErrorDescription => Message;

        /// <summary>
        /// Construct a NATSJetStreamException from an ApiResponse that is an Error
        /// </summary>
        /// <param name="apiResponse"></param>
        public NATSJetStreamException(ApiResponse apiResponse) : base(apiResponse.Error.ToString())
        {
            ErrorCode = apiResponse.ErrorCode;
            ApiErrorCode = apiResponse.ApiErrorCode;
        }

        /// <summary>
        /// Construct a NATSJetStreamException directly from an Error
        /// </summary>
        /// <param name="error"></param>
        public NATSJetStreamException(Error error) : base(error.ToString())
        {
            ErrorCode = error.Code;
            ApiErrorCode = error.ApiErrorCode;
        }

        /// <summary>
        /// Construct a NATSJetStreamException from a string.
        /// </summary>
        /// <param name="s">The exception message.</param>
        public NATSJetStreamException(string s) : base(s)
        {
            ErrorCode = ErrorCodeUnspecified;
            ApiErrorCode = ErrorCodeUnspecified;
        }
    }
}
