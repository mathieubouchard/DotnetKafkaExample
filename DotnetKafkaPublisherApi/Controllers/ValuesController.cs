using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

using Confluent.Kafka;

namespace DotnetKafkaPublisherApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ValuesController : ControllerBase
    {
        // POST api/values
        [HttpPost]
        public string Post([FromBody] string value)
        {
            var config = new ProducerConfig { BootstrapServers = "my-cluster-kafka:9092" };

            Action<DeliveryReport<Null, string>> handler = r =>
                Console.WriteLine(!r.Error.IsError
                ? $"Delivered message to {r.TopicPartitionOffset}"
                : $"Delivery error {r.Error.Reason}");

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try {
                    producer.ProduceAsync("dotnetexampletopic", new Message<Null, string> { Value = value});
                    producer.Flush(timeout: TimeSpan.FromSeconds(10));
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }

            return "Succès";
        }
    }
}
