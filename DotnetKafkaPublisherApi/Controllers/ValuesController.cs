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
        // GET api/values
/*        [HttpGet]
        public ActionResult<IEnumerable<string>> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET api/values/5
        [HttpGet("{id}")]
        public ActionResult<string> Get(int id)
        {
            return "value";
        } */

        // POST api/values
        [HttpPost]
        public void Post([FromBody] string value)
        {
            var config = new ProducerConfig { BootstrapServers = "my-cluster-kafka:9092" };

            Action<DeliveryReport<Null, string>> handler = r =>
                Console.WriteLine(!r.Error.IsError
                ? $"Delivered message to {r.TopicPartitionOffset}"
                : $"Delivery error {r.Error.Reason}");

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                var stringValue = "";
                for (int i = 0; i < 100; ++i)
                {
                    producer.ProduceAsync("dotnetexampletopic", new Message<Null, string> { Value = stringValue});
                }

                producer.Flush(timeout: TimeSpan.FromSeconds(10));
            }
        }

        // PUT api/values/5
/*        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        } */

        // DELETE api/values/5
/*        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        } */
    }
}
