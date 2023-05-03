using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace RabbitMQ_OK
{
    class Recevie
    {

        //主題消費者
        public static void TopicConsumer()
        {
            string queueName = "topic_queue1";
            var connection = RabbitMQHelper.GetConnection();
            {

                var channel = connection.CreateModel();
                {

                    channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
                    var consumer = new EventingBasicConsumer(channel);

                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: true);
                    int i = 1;
                    consumer.Received += (model, ea) =>
                    {

                        var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                        Console.WriteLine($"{i},隊列{queueName}消費消息長度:{message.Length}");
                        channel.BasicAck(ea.DeliveryTag, true); //消息ack確認，可以從mq删除了
                        i++;
                    };
                    //autoAck: 改True
                    channel.BasicConsume(queueName, autoAck: true, consumer);
                }
            }
        }

        //這裡只消費direct_errorlog隊列做示範, 路由模式
        public static void DirectConsumer()
        {
            string queueName = "error_log";
            var connection = RabbitMQHelper.GetConnection();
            {

                var channel = connection.CreateModel();
                {

                    channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
                    var consumer = new EventingBasicConsumer(channel);

                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: true);
                    int i = 1;
                    consumer.Received += (model, ea) =>
                    {

                        var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                        Console.WriteLine($"{i},隊列{queueName}消費消息長度:{message.Length}");
                        channel.BasicAck(ea.DeliveryTag, true); //消息ack確認，可以從mq删除了
                        i++;
                    };
                    //autoAck: 改True
                    channel.BasicConsume(queueName, autoAck: true, consumer);
                }
            }
        }

        //發布訂閱消費者
        public static void FanoutConsumer()
        {
            string queueName = "fanout_queue1";
            var connection = RabbitMQHelper.GetConnection();

            {
                var channel = connection.CreateModel();
                {
                    channel.QueueDeclare(queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);
                    var consumer = new EventingBasicConsumer(channel);

                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                    int i = 1;
                    int index = new Random().Next(10);
                    consumer.Received += (model, ea) =>
                    {
                        //處理業務
                        var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                        Console.WriteLine($"{i},消費者:{index},隊列{queueName} 消費消息長度:{message.Length}");
                        channel.BasicAck(ea.DeliveryTag, true); //消息ack確認,告訴mq這條隊列處理完,可以從mq刪除了
                        Thread.Sleep(1000);
                        i++;
                    };
                    channel.BasicConsume(queueName, autoAck: true, consumer);
                }
            }
        }

        //工作隊列模式, 消費者目前無作用
        public static void WorkerConsumer()
        {
            string queueName = "worker_order";
            var connection = RabbitMQHelper.GetConnection();

            {
                //創建信道
                var channel = connection.CreateModel();
                {
                    //聲明同一個隊列時，durable, auto_delete, passive等參數都要保持一致。
                    //創建隊列
                    channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
                    var consumer = new EventingBasicConsumer(channel);
                    //prefetchCount:1 來告知RabbitMQ, 不要同時給一個消費者推送多於 N個消息,也確保了消費速度和性能
                    //prefetchSize：每條消息大小，一般設為0，表示不限制。：每條消息大小，一般設為0，表示不限制。
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                    int i = 1;
                    int index = new Random().Next(10);
                    consumer.Received += (model, ea) =>
                    {
                        //處理業務
                        var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                        Console.WriteLine($"{i},消費者:{index},隊列{queueName} 消費消息長度:{message.Length}");
                        //channel.BasicAck(ea.DeliveryTag, true); //消息ack確認,告訴mq這條隊列處理完,可以從mq刪除了
                        Thread.Sleep(1000);
                        i++;
                    };
                    channel.BasicConsume(queueName, autoAck: true, consumer);
                }
            }
        }

        //RPC模式客戶端
        public class RPCClient
        {
            private readonly IConnection connection;
            private readonly IModel channel;
            private readonly string replyQueueName;
            private readonly EventingBasicConsumer consumer;
            private readonly BlockingCollection<string> respQueue = new BlockingCollection<string>();
            private readonly IBasicProperties props;


            public RPCClient()
            {
                //先連線
                connection = RabbitMQHelper.GetConnection();

                channel = connection.CreateModel();
                replyQueueName = channel.QueueDeclare().QueueName;
                consumer = new EventingBasicConsumer(channel);

                props = channel.CreateBasicProperties();
                var correlationId = Guid.NewGuid().ToString();
                props.CorrelationId = correlationId; //給消息id
                props.ReplyTo = replyQueueName; //回調的隊列名, Client關閉後會自動刪除

                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var response = Encoding.UTF8.GetString(body);
                    //監聽的消息Id和定義的消息Id相同代表這條消息服務端處理完成
                    if (ea.BasicProperties.CorrelationId == correlationId)
                    {
                        respQueue.Add(response);
                    }
                };

                channel.BasicConsume(
                    consumer: consumer,
                    queue: replyQueueName,
                    autoAck: true);
            }

            public string Call(string message)
            {
                var messageBytes = Encoding.UTF8.GetBytes(message);
                //發送消息
                channel.BasicPublish(
                    exchange: "",
                    routingKey: "rpc_queue",
                    basicProperties: props,
                    body: messageBytes);
                //等待回復
                return respQueue.Take();
            }
            public void Close()
            {
                connection.Close();
            }
        }

        public static void SimpleConsumer()
        {
            string queueName = "simple_order";
            var connection = RabbitMQHelper.GetConnection();
            //創建萬神通道?)
            var channel = connection.CreateModel();
            {
                //創建隊列
                channel.QueueDeclare(queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);
                var consumer = new EventingBasicConsumer(channel);
                int i = 0;
                consumer.Received += (IModel, ea) =>
                {
                    //消費者業務處理
                    var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                    Console.WriteLine($"{i},隊列{queueName} 消費消息長度:{message.Length}");
                    i++;
                };
                channel.BasicConsume(queueName, true, consumer);

            }


        }

    }
}
