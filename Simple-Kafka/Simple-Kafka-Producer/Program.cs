using System;
using System.Threading.Tasks;

namespace Simple_Kafka_Producer
{
	internal class Program
	{
		static string server = "127.0.0.1:9092";
		static async Task Main(string[] args)
		{
			await Run();
		}

		static async Task Run()
		{
			Console.WriteLine("Preparing and sending messages to Kafka. \nPress Ctrl-C to exit;");
			string topic = string.Empty;
			if (topic == string.Empty)
			{
				Console.Write("# Provide a Topic for a Producer: ");
				topic = Console.ReadLine();
			}
			else
			{
				Console.Write(string.Format("# Provide a Topic for a Producer: [{0}]: ", topic));
				var temp = Console.ReadLine();
				if (temp != string.Empty)
					topic = temp;
			}
			while (true)
			{
				Console.Write("# Provide a Message: ");
				var message = Console.ReadLine();
				await Simple_Kafka_Producer.ProduceMessages(server, topic, message);
			}
		}
	}
}
