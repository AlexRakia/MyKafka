using System;

namespace Simple_Kafka_Consumer
{
    internal class Program
    {
        static string server = "127.0.0.1:9092";
        static void Main(string[] args)
        {
            Console.WriteLine("Consuming...");
            Console.Write("Provide a topic to consume: ");
            var topic = Console.ReadLine();
            string dynamicGroup = $"consumer-group-{DateTime.Now:yyyyMMdd-HHmmss-fff}";
            Simple_Kafka_Consumer.ConsumeMessages(server, topic, dynamicGroup);
        }
    }
}
