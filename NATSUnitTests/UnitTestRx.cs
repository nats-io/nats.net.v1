using System;
using Moq;
using NATS.Client;
using Xunit;

namespace NATSUnitTests
{
    public class UnitTestRx
    {
        private const string SUBJECT = "some_subject";
        private const string QUEUE = "some_queue";

        [Fact]
        public void TestConnectionToObservable()
        {
            var connection = new Mock<IConnection>();
            var subscription = new Mock<IAsyncSubscription>();
            EventHandler<MsgHandlerEventArgs> handler = null;
            connection.Setup(c => c.SubscribeAsync(SUBJECT, QUEUE, It.IsAny<EventHandler<MsgHandlerEventArgs>>()))
                .Returns((string _, string __, EventHandler<MsgHandlerEventArgs> h) =>
                {
                    handler = h;
                    return subscription.Object;
                });
            var observer = new Mock<IObserver<MsgHandlerEventArgs>>();

            var observable = connection.Object.ToObservable(SUBJECT, QUEUE);
            connection.Verify(
                c => c.SubscribeAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<EventHandler<MsgHandlerEventArgs>>()),
                Times.Never
            );

            var disposable = observable.Subscribe(observer.Object);
            connection.Verify(
                c => c.SubscribeAsync(SUBJECT, QUEUE, It.IsAny<EventHandler<MsgHandlerEventArgs>>()),
                Times.Once
            );
            var eventArgs = new MsgHandlerEventArgs();
            handler(null, eventArgs);
            observer.Verify(o => o.OnNext(eventArgs));

            disposable.Dispose();
            subscription.Verify(s => s.Dispose());
            observer.Verify(o => o.OnCompleted());
        }

        [Fact]
        public void TestEncodedConnectionToObservable()
        {
            var connection = new Mock<IEncodedConnection>();
            var subscription = new Mock<IAsyncSubscription>();
            EventHandler<EncodedMessageEventArgs> handler = null;
            connection.Setup(c => c.SubscribeAsync(SUBJECT, QUEUE, It.IsAny<EventHandler<EncodedMessageEventArgs>>()))
                .Returns((string _, string __, EventHandler<EncodedMessageEventArgs> h) =>
                {
                    handler = h;
                    return subscription.Object;
                });
            var observer = new Mock<IObserver<EncodedMessageEventArgs>>();

            var observable = connection.Object.ToObservable(SUBJECT, QUEUE);
            connection.Verify(
                c => c.SubscribeAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<EventHandler<EncodedMessageEventArgs>>()),
                Times.Never
            );

            var disposable = observable.Subscribe(observer.Object);
            connection.Verify(
                c => c.SubscribeAsync(SUBJECT, QUEUE, It.IsAny<EventHandler<EncodedMessageEventArgs>>()),
                Times.Once
            );
            handler(null, null);
            observer.Verify(o => o.OnNext(null));

            disposable.Dispose();
            subscription.Verify(s => s.Dispose());
            observer.Verify(o => o.OnCompleted());
        }
    }
}