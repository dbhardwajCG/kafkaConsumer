using Confluent.Kafka;
using System.Text.Json;

namespace kafkaConsumer
{
    public class OrderMessage
    {
        public string OrderMsgId { get; set; }
        public string From { get; set; }
        public string PartnerID { get; set; }
        public string ProductType { get; set; }
        public string ProductID { get; set; }
        public string ServiceList { get; set; }
        public string UpdateType { get; set; }
        public string UpdateTime { get; set; }
        public string EffectiveTime { get; set; }
        public string ExpiryTime { get; set; }
        public string ChannelID { get; set; }
        public string ShortMessage { get; set; }
        public string Status { get; set; }
        public string IsFree { get; set; }
        public string RateCode { get; set; }
        public string ProductPeriodType { get; set; }
        public string ActualValidate { get; set; }
        public string NextChargingTime { get; set; }
    }

    internal class Program
    {
        private static readonly string kafkaTopic = "glo-webhook-topic";
        private static readonly string kafkaBootstrapServers = "192.168.0.136:9092"; // Update with your Kafka broker address

        static async Task Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                GroupId = "order-message-consumer-group",
                BootstrapServers = kafkaBootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe(kafkaTopic);

                while (true)
                {
                    var consumeResult = consumer.Consume();
                    var message = JsonSerializer.Deserialize<OrderMessage>(consumeResult.Message.Value);

                    Console.WriteLine($"Received message: {JsonSerializer.Serialize(message, new JsonSerializerOptions { WriteIndented = true })}");
                }
            }
        }
    }
}
