
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

using System;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// The exception thrown when there is an error with JetStream.
    /// </summary>
    public class NATSJetStreamException : NATSException
    {
        private int _errorCode = ErrorCodeUnspecified;
        private int _apiErrorCode = ErrorCodeUnspecified;

        /// <summary>
        /// Unspecified error code.
        /// </summary>
        public static int ErrorCodeUnspecified = -1;

        /// <summary>
        /// Gets the error code returned by JetStream.  Returns ErrorCodeUnspecified
        /// if the error is local.
        /// </summary>
        public int ErrorCode { get { return _errorCode; } }

        /// <summary>
        /// Gets the error code returned by JetStream.  Returns ErrorCodeUnspecified
        /// if the error is local.
        /// </summary>
        public int ApiErrorCode { get { return _apiErrorCode; } }

        /// <summary>
        /// Returns the description of the error.
        /// </summary>
        public string ErrorDescription => Message;

        /// <summary>
        /// Supplied for unit testing.  Not normally used.
        /// </summary>
        /// <param name="apiResponse"></param>
        public NATSJetStreamException(ApiResponse apiResponse) : base(apiResponse.Error.Desc)
        {
            _errorCode = apiResponse.ErrorCode;
            _apiErrorCode = apiResponse.ApiErrorCode;
        }

        /// <summary>
        /// Supplied for unit testing.  Not normally used.
        /// </summary>
        /// <param name="m"></param>
        public NATSJetStreamException(Msg m) : this(new ApiResponse(m)) { }

        /// <summary>
        /// Supplied for unit testing.  Not normally used.
        /// </summary>
        /// <param name="s">The exception message.</param>
        public NATSJetStreamException(string s) : base(s) { }

        /// <summary>
        /// Supplied for unit testing.  Not normally used.
        /// </summary>
        /// <param name="err">The exception message.</param>
        /// <param name="innerEx">The inner exception.</param>
        public NATSJetStreamException(string err, Exception innerEx) : base(err, innerEx) { }
    }
}