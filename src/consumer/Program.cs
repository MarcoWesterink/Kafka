using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new Dictionary<string, object>
            {
                { "group.id", "sample-consumer" },
                { "bootstrap.servers", "localhost:9092" },
                { "enable.auto.commit", "false"},
                { "auto.offset.reset", "earliest"}
            };

            using (var consumer = new Consumer<Null, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {                
                consumer.Subscribe(new string[]{"hello-topic"});

                consumer.OnMessage += (_, msg) => 
                {
                    Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");
                    consumer.CommitAsync(msg);
                };

                Console.WriteLine("Waiting for messages...");
                while (true)
                {
                    consumer.Poll(100);
                }
        }
        }
    }
}
