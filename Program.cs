using System;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MQTT
{
    class Program
    {
        private static int SubscriberCount { get; set; }

        public static int ConnectedClients { get; set; }

        private static int PublisherCount { get; set; }

        private static int PublishingRateSec { get; set; }

        private static readonly Random Rng = new Random();

        static async Task Main(string[] args)
        {
            ConnectedClients = 0;

            Console.WriteLine("***********************************************************");
            Console.WriteLine("**              MQTT IoT Connection Tester               **");
            Console.WriteLine("***********************************************************");


            Console.Write($"Enter amount of Subscriber: ");

            var subscriberCount = 0;

            while (!int.TryParse(Console.ReadLine(), out subscriberCount))
            {
                Console.Write($"Enter amount of Subscriber: ");
            }

            SubscriberCount = subscriberCount;


            //Console.Write($"Enter amount of Publishers: ");

            //var publisherCount = 0;

            //while (!int.TryParse(Console.ReadLine(), out publisherCount))
            //{
            //    Console.Write($"Enter amount of Publishers: ");
            //}

            //PublisherCount = publisherCount;

            //Console.Write($"Enter Publisher rate in sec: ");

            //var publisherRate = 0;

            //while (!int.TryParse(Console.ReadLine(), out publisherRate))
            //{
            //    Console.Write($"Enter Publisher rate in sec: ");
            //}

            //PublishingRateSec = publisherRate;


            Console.WriteLine($"Subscribers Set To {SubscriberCount}");
            //Console.WriteLine($"Publishers Set To {PublisherCount}");
            //Console.WriteLine($"Publishers Rate Set To {PublishingRateSec} sec");

            PrintPublishStats();

            //await SubscriberTest();

            //await PublishFromApiTest();

            //Console.WriteLine($"{DateTime.Now:g} - Starting Publishers");

            //await PublisherTest();

            //Console.WriteLine($"{DateTime.Now:g} - Publishers sending messages");

            SubscriberTestSpinning();

            Console.WriteLine($"Press ENTER to exit at anytime....");
            while (Console.ReadKey(true).Key != ConsoleKey.Enter)
            {
                Console.WriteLine($"Press ENTER to exit....");
            }
        }

        private static async Task SubscriberTest()
        {
            Console.WriteLine($"{DateTime.Now:g} - Connecting Subscribers");
            while (Mqtt.Clients.Count < SubscriberCount)
            {
                var id = Guid.NewGuid().ToString();
                await Mqtt.Connect(id, new[] { $"d/{id}" });
            }

            Console.WriteLine($"{DateTime.Now:g} - All Subscribers Connected \t\t\t\t Avg Connection Time: {Mqtt.ConnectionTimes.Average()} ms");
            await Task.Delay(2500);
        }

        private static void SubscriberTestSpinning()
        {
            Console.WriteLine($"{DateTime.Now:g} - Connecting Subscribers");

            while (true)
            {
                if (ConnectedClients < SubscriberCount)
                {
                    ConnectedClients++;
                    var id = Guid.NewGuid().ToString();
                    var task = Mqtt.Connect(id, new[] {$"d/{id}"});
                    Task.Run(() => task);
                }
                else
                {
                    Thread.Sleep(1000);
                }
            }
        }

        private static async Task PublishFromApiTest()
        {
            Console.WriteLine($"{DateTime.Now:g} - Connecting API");
            var client = await Mqtt.GetClient($"api-{Guid.NewGuid().ToString().Substring(0, 8)}", Mqtt.Clients.Select(x => "a/" + x.Key).ToArray());
            
            var timer = new System.Timers.Timer { Interval = PublishingRateSec * 1000 };
            timer.Elapsed += async (sender, args) =>
            {
               await Mqtt.Publish(client, Mqtt.GetRandomTopic());
            };

            timer.Start();
        }

        private static async Task PublisherTest()
        {
            while (Mqtt.Publishers.Count < PublisherCount)
            {
                StartRandomPublisher();
                await Task.Delay(Rng.Next(PublishingRateSec * 1000));
            }
        }

        private static void StartRandomPublisher()
        {
            var publisher = Mqtt.GetPublisher();

            if (publisher == null)
            {
                return;
            }

            Console.WriteLine($"{DateTime.Now:g} - Starting Publisher {publisher.Value.Key}");
            var timer = new System.Timers.Timer { Interval = PublishingRateSec * 1000 };
            timer.Elapsed += async (sender, args) =>
            {
               await Mqtt.Publish(publisher.Value.Value, $"a/{publisher.Value.Key}");
            };

            timer.Start();
        }

        private static void PrintPublishStats()
        {
            var timer = new System.Timers.Timer { Interval = 15000 };
            timer.Elapsed += (sender, args) =>
            {
                var min = Mqtt.MessageSendTimes.Any() ? Mqtt.MessageSendTimes.Min() : 0;
                var avg = Mqtt.MessageSendTimes.Any() ? Mqtt.MessageSendTimes.Average() : 0;
                var max = Mqtt.MessageSendTimes.Any() ? Mqtt.MessageSendTimes.Max() : 0;
                Console.WriteLine("---------------------------------------------------------------------------------------------------------");
                Console.WriteLine($"{DateTime.Now:g} - Clients: {Mqtt.Clients.Count}/{SubscriberCount}");
                //Console.WriteLine($"{DateTime.Now:g} - Subscribers: {Mqtt.Clients.Count}/{SubscriberCount} \t Publishers: {Mqtt.Publishers.Count}/{PublisherCount} \t Rate: {PublishingRateSec} sec");
                //Console.WriteLine($"{DateTime.Now:g} - Message Latency \t Min:{min:f0} ms \t Avg:{avg:f0} ms \t Max:{max:f0} ms");
                Console.WriteLine("---------------------------------------------------------------------------------------------------------");
            };

            timer.Start();
        }
    }
}
