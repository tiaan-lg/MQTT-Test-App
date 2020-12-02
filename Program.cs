using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using MQTTnet.Formatter;
using MQTTnet.Protocol;

namespace MQTT
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var limit = 10000;
            var id = 0;

            while (id < limit)
            {
                await Mqtt.Connect(Guid.NewGuid().ToString());
                Console.WriteLine($"Connected Clients: {Mqtt.clients.Count}");
            }

            Console.WriteLine($"Connected Clients: {Mqtt.clients.Count}");
        }
    }


    public static class Mqtt
    {
        private const string Host = "102.133.134.75";
        private const int Port = 1883;
        private const string UserName = "test";
        private const string Password = "Test1234";
        public static MqttFactory factory = new MqttFactory();
        public static ConcurrentBag<IMqttClient> clients = new ConcurrentBag<IMqttClient>();

        public static async Task Connect(string clientId)
        {

            var options = new MqttClientOptions
            {
                ClientId = clientId,
                ProtocolVersion = MqttProtocolVersion.V311,
                ChannelOptions = new MqttClientTcpOptions
                {
                    Server = Host,
                    Port = Port,
                    TlsOptions = new MqttClientTlsOptions
                    {
                        UseTls = false,
                        IgnoreCertificateChainErrors = true,
                        IgnoreCertificateRevocationErrors = true,
                        AllowUntrustedCertificates = true
                    }
                },
                Credentials = new MqttClientCredentials
                {
                    Username = UserName,
                    Password = Encoding.UTF8.GetBytes(Password)
                },
                CleanSession = true,
                KeepAlivePeriod = TimeSpan.FromMinutes(2),
                CommunicationTimeout = TimeSpan.FromSeconds(30),
                WillMessage = new MqttApplicationMessageBuilder()
                    .WithTopic("test")
                    .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce)
                    .Build()
            };


            var client = factory.CreateMqttClient();

            try
            {
                await client.ConnectAsync(options);
            }
            catch (Exception exception)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(exception);
                Console.ForegroundColor = ConsoleColor.White;
            }

            clients.Add(client);
        }
    }
}
