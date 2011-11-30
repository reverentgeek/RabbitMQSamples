using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.MessagePatterns;

namespace CompetingConsumers.Consumer
{
	class Program
	{
		static void Main()
		{
			// Set up the RabbitMQ connection and channel
			var connectionFactory = new ConnectionFactory
			{
				HostName = "localhost",
				Port = 5672,
				UserName = "guest",
				Password = "guest",
				Protocol = Protocols.AMQP_0_9_1,
				RequestedFrameMax = UInt32.MaxValue,
				RequestedHeartbeat = UInt16.MaxValue
			};

			using (var connection = connectionFactory.CreateConnection())
			using (var channel = connection.CreateModel())
			{
				// This instructs the channel not to prefetch more than one message
				channel.BasicQos(0, 1, false);

				// Create a new, durable exchange
				channel.ExchangeDeclare("sample-ex", ExchangeType.Direct, true, false, null);
				// Create a new, durable queue
				channel.QueueDeclare("sample-queue", true, false, false, null);
				// Bind the queue to the exchange
				channel.QueueBind("sample-queue", "sample-ex", "optional-routing-key");

				using (var subscription = new Subscription(channel, "sample-queue", false))
				{
					Console.WriteLine("Waiting for messages...");
					var encoding = new UTF8Encoding();
					while (channel.IsOpen)
					{
						BasicDeliverEventArgs eventArgs;
						var success = subscription.Next(2000, out eventArgs);
						if (success == false) continue;
						var msgBytes = eventArgs.Body;
						var message = encoding.GetString(msgBytes);
						Console.WriteLine(message);
						channel.BasicAck(eventArgs.DeliveryTag, false);
					}
				}
			}
		}
	}
}
