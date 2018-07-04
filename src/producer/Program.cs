using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace producer
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", "localhost:9092" }
            };

            using (var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8)))
            {
                string text = null;

                while (text != "exit")
                {
                    System.Console.Write("Text: ");
                    text = Console.ReadLine();                                                                                                                                                              
                    var result = producer.ProduceAsync("hello-topic", null, text).Result;
                    System.Console.WriteLine($"Error: {result.Error}");
                }

                producer.Flush(100);
            }
        }
    }
}
