// Copyright 2020 The NATS Authors
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
using NATS.Client;
using NATS.Client.Internals;
using Xunit;

namespace UnitTests.Internals
{
    public class InFlightRequestTests
    {
        [Fact]
        public async Task Timeout_ThrowsNatsTimeoutException()
        {
            // Arrange
            var sut = new InFlightRequest("Foo", default, 1, _ => { });

            // Assert
            await Assert.ThrowsAsync<NATSTimeoutException>(() => sut.Task);
        }

        [Fact]
        public async Task TimeoutWithToken_ThrowsNATSTimeoutExcpetion()
        {
            // Arrange
            var cts = new CancellationTokenSource();
            var sut = new InFlightRequest("Foo", cts.Token, 1, _ => { });

            // Assert
            await Assert.ThrowsAsync<NATSTimeoutException>(() => sut.Task);
        }

        [Fact]
        public async Task Canceled_ThrowsTaskCanceledExcpetion()
        {
            // Arrange
            var cts = new CancellationTokenSource();
            var sut = new InFlightRequest("Foo", cts.Token, 0, _ => { });

            // Act
            cts.Cancel();

            // Assert
            await Assert.ThrowsAsync<TaskCanceledException>(() => sut.Task);
        }

        [Fact]
        public async Task CanceledWithTimeout_ThrowsTaskCanceledException()
        {
            // Arrange
            var cts = new CancellationTokenSource();
            var sut = new InFlightRequest("Foo", cts.Token, int.MaxValue, _ => { });

            // Act
            cts.Cancel();

            // Assert
            await Assert.ThrowsAsync<TaskCanceledException>(() => sut.Task);
        }

        [Fact]
        public void Dispose_InvokesOnCompletedDelegate()
        {
            // Arrange
            var onCompletedArg = "";
            var sut = new InFlightRequest("Foo", default, 0, id => { onCompletedArg = id; });

            // Act
            sut.Dispose();

            // Assert
            Assert.Equal("Foo", onCompletedArg);
        }

        [Fact]
        public async Task TrySetResult_CompletedSuccessfullyWithResult()
        {
            // Arrange
            var msg = new Msg();
            var sut = new InFlightRequest("Subject", default, 0, _ => {});
            
            // Act
            sut.TrySetResult(msg);
            
            // Assert
            Assert.Equal(msg,  await sut.Task);
        }
        
        [Fact]
        public async Task TrySetCancelled_TaskCanceledExceptionThrownWhenAwaiting()
        {
            // Arrange
            var sut = new InFlightRequest("Subject", default, 0, _ => {});
            
            // Act
            sut.TrySetCanceled();
            
            // Assert
            await Assert.ThrowsAsync<TaskCanceledException>(() => sut.Task);
        }
        
        [Fact]
        public async Task TrySetException_ExceptionThrownWhenAwaiting()
        {
            // Arrange
            var sut = new InFlightRequest("Subject", default, 0, _ => {});
            
            // Act
            sut.TrySetException(new InvalidOperationException());
            
            // Assert
            await Assert.ThrowsAsync<InvalidOperationException>(() => sut.Task);
        }
        
        [Fact]
        public async Task ContinueWith_ContinuationInvokedOnCompletion()
        {
            // Arrange
            var guard = new TaskCompletionSource<bool>();
            Task task = default;

            var sut = new InFlightRequest("Subject", default, 0, _ => {});
            sut.ContinueWith(t => { task = t; guard.TrySetResult(false); }, TaskContinuationOptions.None);

            // Act
            sut.TrySetResult(new Msg());

            // Assert
            await guard.Task;
            Assert.Equal(sut.Task, task);
        }
        
        [Fact]
        public void Ctor_ThrowsForNullArg()
        {
            Assert.Throws<ArgumentNullException>("onCompleted", () => new InFlightRequest("Foo", default, 0, null));
        }
    }
}
