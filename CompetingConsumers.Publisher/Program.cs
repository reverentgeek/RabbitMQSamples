using System;
using System.Text;
using RabbitMQ.Client;

namespace CompetingConsumers.Publisher
{
	internal class Program
	{
		private static void Main()
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
				// Create a new, durable exchange
				channel.ExchangeDeclare("sample-ex", ExchangeType.Direct, true, false, null);
				// Create a new, durable queue
				channel.QueueDeclare("sample-queue", true, false, false, null);
				// Bind the queue to the exchange
				channel.QueueBind("sample-queue", "sample-ex", "optional-routing-key");

				// Set up message properties
				var properties = channel.CreateBasicProperties();
				properties.DeliveryMode = 2; // Messages are persistent and will survive a server restart

				// Ready to start publishing
				// The message to publish can be anything that can be serialized to a byte array, 
				// such as a serializable object, an ID for an entity, or simply a string
				var encoding = new UTF8Encoding();
				for (var i = 0; i < 10; i++)
				{
					var msg = string.Format("This is message #{0}?", i+1);
					var msgBytes = encoding.GetBytes(msg);
					channel.BasicPublish("sample-ex", "optional-routing-key", false, false, properties, msgBytes);
				}
				channel.Close();
			}
			Console.WriteLine("Messages published");
			Console.ReadKey(true);
		}
	}
}
