using System;

namespace NATS.Client
{
    internal sealed class Disposable : IDisposable
    {
        private bool _isDisposed;
        private readonly Action _dispose;

        public Disposable(Action dispose)
        {
            _dispose = dispose;
        }

        public void Dispose()
        {
            if (_isDisposed) return;

            _dispose();
            _isDisposed = true;
        }
    }
}