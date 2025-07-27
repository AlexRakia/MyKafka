using Confluent.Kafka;
using System;
using System.Threading;

namespace Simple_Kafka_Consumer
{
	public class Simple_Kafka_Consumer
	{
		public static void ConsumeMessages(string bootstrapServers, string topicName, string consumerGroup = "Consumers-Group")
		{
			var config = new ConsumerConfig
			{
				BootstrapServers = bootstrapServers,
				GroupId = consumerGroup,
				AutoOffsetReset = AutoOffsetReset.Earliest // Start consuming from the beginning if no offset is committed
			};

			using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
			{
				consumer.Subscribe(topicName);
				Console.WriteLine($"Consuming messages in group: '{consumerGroup}' \tfrom Topic: '{topicName}'");

				CancellationTokenSource cts = new CancellationTokenSource();
				Console.CancelKeyPress += (_, e) =>
				{
					e.Cancel = true; // Prevent the process from terminating immediately
					cts.Cancel();
				};

				try
				{
					while (true)
					{
						try
						{
							var consumeResult = consumer.Consume(cts.Token);
							Console.WriteLine($"At: '{consumeResult.TopicPartitionOffset}', \tConsumed message: '{consumeResult.Value}' "); // consumeResult.Message.Value
						}
						catch (ConsumeException e)
						{
							Console.WriteLine($"Error occurred: {e.Error.Reason}");
						}
					}
				}
				catch (OperationCanceledException)
				{
					// Ctrl+C was pressed.
					consumer.Close(); // Commit offsets and leave the group cleanly.
				}
			}
		}
	}
}
