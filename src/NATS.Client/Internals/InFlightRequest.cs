// Copyright 2017-2020 The NATS Authors
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
using System.Threading;
using System.Threading.Tasks;

namespace NATS.Client.Internals
{
    /// <summary>
    /// Represents an in-flight request/reply operation.
    /// </summary>
    /// <remarks>
    /// This class is not used when using the legacy request/reply
    /// pattern (see <see cref="Options.UseOldRequestStyle"/>).
    /// </remarks>
    internal sealed class InFlightRequest : IDisposable
    {
        private readonly Action<string> _onCompleted;
        private readonly CancellationTokenSource _tokenSource;
        private readonly CancellationTokenRegistration _tokenRegistration;
        private readonly CancellationToken _clientProvidedToken;

        internal readonly TaskCompletionSource<Msg> Waiter = new TaskCompletionSource<Msg>(TaskCreationOptions.RunContinuationsAsynchronously);

        public readonly string Id;
        public readonly CancellationToken Token;

        /// <summary>
        /// Initializes a new instance of <see cref="InFlightRequest"/> class.
        /// </summary>
        /// <param name="id">The id associated with the request.</param>
        /// <param name="token">The cancellation token used to cancel the request.</param>
        /// <param name="timeout">A timeout (ms) after which the request is canceled.</param>
        /// <param name="onCompleted">The delegate that will be executed after the request ended.</param>
        /// <exception cref="TaskCanceledException">Thrown if the request is cancelled by <paramref name="token"/> before receiving a response.</exception>
        /// <exception cref="NATSTimeoutException">Thrown if the request is cancelled because <paramref name="timeout"/> period has elapsed before receiving a response.</exception>
        internal InFlightRequest(string id, CancellationToken token, int timeout, Action<string> onCompleted)
        {
            _onCompleted = onCompleted ?? throw new ArgumentNullException(nameof(onCompleted));
            _clientProvidedToken = token;
            Id = id;

            if (timeout > 0 && token == default)
            {
                _tokenSource = new CancellationTokenSource();
                Token = _tokenSource.Token;
            }
            else if (timeout > 0)
            {
                _tokenSource = CancellationTokenSource.CreateLinkedTokenSource(token);
                Token = _tokenSource.Token;
            }
            else
            {
                Token = token;
            }

            _tokenRegistration = Token.Register(CancellationCallback, this);

            if (timeout > 0)
                _tokenSource.CancelAfter(timeout);
        }

        private static void CancellationCallback(object req)
        {
            var request = req as InFlightRequest;

            if (request._clientProvidedToken.IsCancellationRequested)
                request.Waiter.TrySetCanceled();

            request.Waiter.TrySetException(new NATSTimeoutException());
        }

        /// <summary>
        /// Releases all resources used by the current instance of the <see cref="InFlightRequest"/>
        /// class and invokes the <c>onCompleted</c> delegate.
        /// </summary>
        public void Dispose()
        {
            _tokenRegistration.Dispose();
            _tokenSource?.Dispose();
            _onCompleted.Invoke(Id);
        }
    }
}
