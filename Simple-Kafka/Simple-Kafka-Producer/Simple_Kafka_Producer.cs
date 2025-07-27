using Confluent.Kafka;
using System;
using System.Threading.Tasks;


namespace Simple_Kafka_Producer
{
	internal class Simple_Kafka_Producer
	{
		public static async Task ProduceMessages(string bootstrapServers, string topicName, string message)
		{
			var config = new ProducerConfig { BootstrapServers = bootstrapServers, Partitioner = Partitioner.Random };

			using (var producer = new ProducerBuilder<Null, string>(config).Build())
			{
				//Console.WriteLine($"Sending to Topic: '{topicName}', \t Msg: '{message}'");
				try
				{
					var deliveryReport = await producer.ProduceAsync(
						topicName, new Message<Null, string> { Value = message });

					Console.WriteLine($"Delivered to '{deliveryReport.TopicPartitionOffset}', \tMsg: '{deliveryReport.Value}'");
				}
				catch (ProduceException<Null, string> e)
				{
					Console.WriteLine($"Delivery failed: {e.Error.Reason}");
				}
				producer.Flush(TimeSpan.FromSeconds(10)); // Ensure all messages are sent
														  //Console.WriteLine($"Sent to Topic: '{topicName}', \t Msg: '{message}'");
			}
		}
	}
}
