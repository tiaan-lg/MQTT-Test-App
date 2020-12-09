using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Connecting;
using MQTTnet.Client.Disconnecting;
using MQTTnet.Client.Options;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MQTT
{
    public static class Mqtt
    {
        private const string Host = "102.133.134.75";
        private const int Port = 1883;

        private const string UserName = "guest";
        private const string Password = "guest";

        private static readonly Random Random = new Random();

        private static readonly MqttFactory Factory = new MqttFactory();
        public static readonly ConcurrentDictionary<string, IMqttClient> Clients = new ConcurrentDictionary<string, IMqttClient>();
        public static readonly ConcurrentDictionary<string, IMqttClient> Publishers = new ConcurrentDictionary<string, IMqttClient>();

        public static readonly List<int> ConnectionTimes = new List<int>();
        public static readonly Buffer<int> MessageSendTimes = new Buffer<int>(10000000);

        public static async Task Connect(string clientId, string[] topics = null)
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
                    },
                    BufferSize = 2048
                },
                Credentials = new MqttClientCredentials
                {
                    Username = UserName,
                    Password = Encoding.UTF8.GetBytes(Password)
                },
                CleanSession = true,
                KeepAlivePeriod = TimeSpan.FromMinutes(2),
                CommunicationTimeout = TimeSpan.FromSeconds(60)
            };


            var client = Factory.CreateMqttClient();

            client.ConnectedHandler = new MqttClientConnectedHandlerDelegate((args) => OnConnected(args, client));
            client.DisconnectedHandler = new MqttClientDisconnectedHandlerDelegate((args) => OnDisconnected(args, clientId));
            try
            {
                //var st = new Stopwatch();
                //st.Start();
                await client.ConnectAsync(options);
                
                if (topics != null && topics.Length != 0)
                {
                    foreach (var topic in topics)
                    {
                        await client.SubscribeAsync(topic, MqttQualityOfServiceLevel.AtLeastOnce);
                    }
                }

                //st.Stop();
                //Console.WriteLine($"{DateTime.Now:g} - {client.Options.ClientId.PadRight(36)} Connected \t {st.ElapsedMilliseconds} ms");
                Console.WriteLine($"{DateTime.Now:g} - {client.Options.ClientId.PadRight(36)} Connected");
                //ConnectionTimes.Add((int) st.ElapsedMilliseconds);
            }
            catch (Exception exception)
            {
                Program.ConnectedClients--;
                LogExc(exception);
            }
        }

        public static async Task<IMqttClient> GetClient(string clientId, string[] topics = null)
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
                    },
                    BufferSize = 2048
                },
                Credentials = new MqttClientCredentials
                {
                    Username = UserName,
                    Password = Encoding.UTF8.GetBytes(Password)
                },
                CleanSession = true,
                KeepAlivePeriod = TimeSpan.FromMinutes(2),
                CommunicationTimeout = TimeSpan.FromSeconds(15)
            };

            var client = Factory.CreateMqttClient();

            try
            {
                var st = new Stopwatch();
                st.Start();
                await client.ConnectAsync(options);
                st.Stop();
                Console.WriteLine($"{DateTime.Now:g} - {client.Options.ClientId.PadRight(36)} Connected \t {st.ElapsedMilliseconds} ms");

                st.Restart();
                if (topics != null && topics.Length != 0)
                {
                    foreach (var topic in topics)
                    {
                        await client.SubscribeAsync(topic, MqttQualityOfServiceLevel.AtLeastOnce);
                    }
                }
                st.Stop();
                Console.WriteLine($"{DateTime.Now:g} - {client.Options.ClientId.PadRight(36)} Subscribed \t {st.ElapsedMilliseconds} ms");
            }
            catch (Exception exception)
            {
                LogExc(exception);
            }

            return client;
        }

        public static async Task Publish(IMqttClient client, string topic)
        {
            try
            {
                var payload = new byte[1024];

                Random.NextBytes(payload);

                var message = new MqttApplicationMessageBuilder()
                    .WithTopic(topic)
                    .WithPayload(payload)
                    .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build();

                var st = new Stopwatch();
                st.Start();
                await client.PublishAsync(message);
                st.Stop();


                Console.WriteLine($"{DateTime.Now:g} - {client.Options.ClientId.PadRight(36)} \t Message to {topic} \t {st.ElapsedMilliseconds} ms");
                MessageSendTimes.Enqueue((int)st.ElapsedMilliseconds);
            }
            catch (Exception exception)
            {
                LogExc(exception);
            }
        }

        public static string GetRandomTopic()
        {
            return $"d/{Clients.ElementAt(Random.Next(0, Clients.Count)).Key}";
        }

        public static KeyValuePair<string, IMqttClient>? GetPublisher()
        {
            try
            {
                var publisher = Clients.ElementAt(Random.Next(0, Clients.Count));

                if (Publishers.TryAdd(publisher.Key, publisher.Value))
                {
                    return publisher;
                }

            }
            catch (Exception e)
            {
                LogExc(e);
                return null;
            }
            
            return null;
        }

        private static void OnConnected(MqttClientConnectedEventArgs x, IMqttClient client)
        {
            Clients.TryAdd(client.Options.ClientId, client);
            Console.WriteLine($"{DateTime.Now:g} - {Clients.Count} Clients Connected");
        }

        private static void OnDisconnected(MqttClientDisconnectedEventArgs x, string clientId)
        {
            Program.ConnectedClients--;
            Clients.TryRemove(clientId, out _);
            Console.WriteLine($"{DateTime.Now:g} - {clientId.PadRight(36)} Disconnected");
            Console.WriteLine($"{DateTime.Now:g} - {Clients.Count} Clients Connected");
        }

        private static void LogExc(Exception ex)
        {
            Console.WriteLine($"==========================================================================================");
            Console.WriteLine(ex);
            Console.WriteLine($"==========================================================================================");
        }
    }
}