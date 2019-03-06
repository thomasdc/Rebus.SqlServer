using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Exceptions;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;

namespace Rebus.SqlServer.Tests.Bugs
{
    [TestFixture, Description("Message handlers should exhibit transactional behavior when wrapped inside a transaction scope. See https://github.com/rebus-org/Rebus.SqlServer/issues/42")]
    public class TestMessageHandlerEnlistingInAmbientTransaction : FixtureBase
    {
        static readonly string ConnectionString = SqlTestHelper.ConnectionString;

        protected override void SetUp() => SqlTestHelper.DropAllTables();

        [TestCase(false, false, false, true, false, TestName = "Test happy path")]
        [TestCase(false, true, true, true, false, TestName = "Test happy path enlisting in ambient transaction")]
        [TestCase(true, false, false, false, true, TestName = "Message shouldn't propagate if message handler throws an exception after sending message")]
        [TestCase(true, true, true, false, true,
            TestName = "Message shouldn't propagate if message handler throws an exception after sending message and enlisting in ambient transaction with transaction scope",
            IgnoreReason = "fixme")]
        public async Task ItWorks(bool messageHandlerThrows, bool enlistInAmbientTransaction,
            bool handleMessagesInsideTransactionScope,
            bool messageHandlerShouldProduceMessage, bool expectAnErrorMessageInTheErrorQueue)
        {
            // Arrange
            var gotTheString = new ManualResetEvent(false);
            var gotTheInt = false;
            var messageInErrorQueue = false;

            //// Set up regular receiver
            var receiver = new BuiltinHandlerActivator();

            Using(receiver);

            receiver.Handle<string>(async (buzz, msg) =>
            {
                gotTheString.Set();
                await buzz.Send(42);

                if (messageHandlerThrows)
                {
                    throw new RebusApplicationException("Didn't see that one coming did ya?");
                }
            });

            receiver.Handle<int>(async msg =>
            {
                await Task.CompletedTask;
                gotTheInt = true;
            });

            var bus = Configure.With(receiver)
                .Transport(t => t.UseSqlServer(ConnectionString, "receiver", enlistInAmbientTransaction))
                .Routing(r => r.TypeBased()
                    .Map<string>("receiver")
                    .Map<int>("receiver"))
                .Options(o =>
                {
                    if (handleMessagesInsideTransactionScope)
                    {
                        o.HandleMessagesInsideTransactionScope();
                    }
                })
                .Start();

            Using(bus);

            //// Set up error handler
            var errorHandler = new BuiltinHandlerActivator();

            Using(errorHandler);

            errorHandler.Handle<object>(async msg =>
            {
                await Task.CompletedTask;
                messageInErrorQueue = true;
            });

            var errorBus = Configure.With(errorHandler)
                .Transport(t => t.UseSqlServer(ConnectionString, "error"))
                .Start();

            Using(errorBus);

            // Act
            await bus.Send("Dag iedereen!");

            gotTheString.WaitOrDie(TimeSpan.FromSeconds(5));
            await Task.Delay(TimeSpan.FromSeconds(5));

            // Assert
            Assert.AreEqual(messageHandlerShouldProduceMessage, gotTheInt,
                messageHandlerShouldProduceMessage
                    ? "The message handler should produce a new message"
                    : "The message handler shouldn't produce a new message");
            Assert.AreEqual(expectAnErrorMessageInTheErrorQueue, messageInErrorQueue,
                expectAnErrorMessageInTheErrorQueue
                    ? "There should be an error message in the error queue"
                    : "The error queue should be empty");
        }
    }
}
