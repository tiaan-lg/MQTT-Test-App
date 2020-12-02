using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Connecting;
using MQTTnet.Client.Disconnecting;
using MQTTnet.Client.Options;
using MQTTnet.Formatter;
using MQTTnet.Protocol;

namespace MQTT
{
    class Program
    {
        private const int Limmit = 10000;
        static async Task Main(string[] args)
        {
            var count = 0;

            while (Mqtt.Clients.Count < Limmit)
            {
                var id = Guid.NewGuid().ToString();
                await Mqtt.Connect(id);
            }

            Console.WriteLine($"{DateTime.Now:g} - Connection limit of {Limmit} reached.");
            Console.WriteLine($"Press ENTER to exit....");

            while (Console.ReadKey(true).Key != ConsoleKey.Enter)
            {
                Console.WriteLine($"Press ENTER to exit....");
            }
        }
    }


    public static class Mqtt
    {
        private const string Host = "102.133.134.75";
        //private const string Host = "127.0.0.1";
        private const int Port = 1883;
        private const string UserName = "test";
        private const string Password = "Test1234";

        private static readonly MqttFactory Factory = new MqttFactory();
        public static readonly Dictionary<string,IMqttClient> Clients = new Dictionary<string,IMqttClient>();

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
                CommunicationTimeout = TimeSpan.FromSeconds(10)
            };


            var client = Factory.CreateMqttClient();
            client.ConnectedHandler = new MqttClientConnectedHandlerDelegate((args) => OnConnected(args,client));
            client.DisconnectedHandler = new MqttClientDisconnectedHandlerDelegate((args) => OnDisconnected(args,clientId));
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
        }

        private static void OnConnected(MqttClientConnectedEventArgs x, IMqttClient client)
        {
            Clients.Add(client.Options.ClientId,client);
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine($"{DateTime.Now:g} - {client.Options.ClientId} Connected");
            Console.WriteLine($"{DateTime.Now:g} -- {Clients.Count} Clients Connected");
            Console.ForegroundColor = ConsoleColor.White;
        }

        private static void OnDisconnected(MqttClientDisconnectedEventArgs x, string clientId)
        {
            Clients.Remove(clientId);
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine($"{DateTime.Now:g} - {clientId} Disconnected");
            Console.WriteLine($"{DateTime.Now:g} -- {Clients.Count} Clients Connected");
            Console.ForegroundColor = ConsoleColor.White;
        }
    }
}
