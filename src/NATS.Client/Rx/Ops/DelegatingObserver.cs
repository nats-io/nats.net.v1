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

using System;

namespace NATS.Client.Rx.Ops
{
    internal sealed class DelegatingObserver<T> : IObserver<T>
    {
        private readonly Action<T> onNext;
        private readonly Action<Exception> onError;
        private readonly Action onCompleted;

        internal DelegatingObserver(
            Action<T> onNext,
            Action<Exception> onError = null,
            Action onCompleted = null)
        {
            this.onNext = onNext ?? throw new ArgumentNullException(nameof(onNext));
            this.onError = onError;
            this.onCompleted = onCompleted;
        }

        public void OnNext(T value)
            => onNext(value);

        public void OnError(Exception error)
            => onError?.Invoke(error);

        public void OnCompleted()
            => onCompleted?.Invoke();
    }
}