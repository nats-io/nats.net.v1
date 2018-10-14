using System;

namespace NATS.Client
{
    internal sealed class MsgObservable<T> : IObservable<T> where T : EventArgs
    {
        private readonly Func<EventHandler<T>, IAsyncSubscription> _subscriptionFactory;

        public MsgObservable(Func<EventHandler<T>, IAsyncSubscription> subscriptionFactory)
        {
            _subscriptionFactory = subscriptionFactory;
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            var subscription = _subscriptionFactory((sender, args) => observer.OnNext(args));
            return new Disposable(() =>
            {
                subscription.Dispose();
                observer.OnCompleted();
            });
        }
    }
}