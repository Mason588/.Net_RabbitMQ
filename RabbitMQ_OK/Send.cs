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
    internal class Send
    {


        public static void SendDirect()
        {
            //創連接
            using (var connection = RabbitMQHelper.GetConnection())
            {
                //創信道
                using (var channel = connection.CreateModel())
                {
                    //聲明交換對象, fanout類型
                    string exchangeName = "direct_exchange";
                    channel.ExchangeDeclare(exchangeName, ExchangeType.Direct);
                    //創建隊列
                    string queueName1 = "direct_errorlog";
                    string queueName2 = "direct_alllog";
                    channel.QueueDeclare(queueName1, true, false, false);
                    channel.QueueDeclare(queueName2, true, false, false);

                    //把創建的隊列綁定交換機,direct_errorlog隊列只綁定routingKey:error
                    channel.QueueBind(queue: queueName1, exchange: exchangeName, routingKey: "error");
                    //direct_alllog隊列綁定routingKey:error,info
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: "info");
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: "error");
                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true; //消息持久化
                    //寫10條訊息給交換機
                    for (int i = 0; i < 10; i++)
                    {
                        string message = $"RabbitMQ Direct {i + 1} error Message";
                        var body = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchangeName, routingKey: "error", properties, body);
                        Console.WriteLine($"發送Direct消息error:{message}");

                        string message2 = $"RabbitMQ Direct {i + 1} info Message";
                        var body2 = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchangeName, routingKey: "info", properties, body2);
                        Console.WriteLine($"info:{message2}");

                    }
                }
            }
        }

        //主题模式是可以
        //模糊匹配路由键 routingKey，
        //类似于SQL中 = 和 like 的关系。
        public static void SendMessageTopic()
        {
            //創建連接
            using (var connection = RabbitMQHelper.GetConnection())
            {
                //創建信道
                using (var channel = connection.CreateModel())
                {
                    //聲明交換機對象,fanout類型
                    string exchangeName = "topic_exchange";
                    channel.ExchangeDeclare(exchangeName, ExchangeType.Topic);
                    //隊列名
                    string queueName1 = "topic_queue1";
                    string queueName2 = "topic_queue2";
                    //路由名
                    string routingKey1 = "*.orange.*";
                    string routingKey2 = "*.*.rabbit";
                    string routingKey3 = "lazy.#";
                    channel.QueueDeclare(queueName1, true, false, false);
                    channel.QueueDeclare(queueName2, true, false, false);

                    //把創建的隊列綁定交換機, routingKey指定routingKey
                    channel.QueueBind(queue: queueName1, exchange: exchangeName, routingKey: routingKey1);
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: routingKey2);
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: routingKey3);
                    //向交換機寫10條消息
                    for (int i = 0; i < 10; i++)
                    {
                        string message = $"RabbitMQ Direct {i + 1} Message";
                        var body = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchangeName, routingKey: "aaa.orange.rabbit", null, body);
                        channel.BasicPublish(exchangeName, routingKey: "lazy.aa.rabbit", null, body);
                        Console.WriteLine($"發送Topic消息:{message}");
                    }
                }
            }

        }
        /// <summary>
        /// 路由模式，通過路由RoutingKey匹配, 點到點直連隊列,推薦使用
        /// </summary>
        public static void SendMessageDirect()
        {
            //創建連接
            using (var connection = RabbitMQHelper.GetConnection())
            {
                //創建萬神通道
                using (var channel = connection.CreateModel())
                {
                    //聲明交換機對象,fanout類型
                    string exchangeName = "direct_exchange";
                    channel.ExchangeDeclare(exchangeName, ExchangeType.Direct);
                    //創建隊列
                    string queueName1 = "direct_errorlog";
                    string queueName2 = "direct_alllog";
                    channel.QueueDeclare(queueName1, true, false, false);
                    channel.QueueDeclare(queueName2, true, false, false);
                    //把創建的隊列綁定交換機, direct_errorlog隊列只綁定routingkey:error
                    channel.QueueBind(queue: queueName1, exchange: exchangeName, routingKey: "error");
                    //direct_alllog隊列綁定routingKey:error,info
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: "info");
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: "error");
                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true; //消息持久化
                    //向交換機寫10條錯誤日誌和10條Info日誌
                    for (int i = 10; i < 10; i++)
                    {
                        string message = $"RabbitMQ Direct{i + 1} error Message";
                        var body = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchangeName, routingKey: "error", properties, body);
                        Console.WriteLine($"發送Direct消息error:{message}");

                        string message2 = $"RabbitMQ Direct{i + 1} info Message";
                        var body2 = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchangeName, routingKey: "info", properties, body2);
                        Console.WriteLine($"info:{message2}");

                    }
                }
            }

        }

        //發布訂閱,扇形隊列,只要訂閱X交換機的人都能獲取服務 ->有訂閱頻道的人都能得到消息
        public static void SendMessageFanout()
        {
            //創建連接
            using (var connection = RabbitMQHelper.GetConnection())
            {
                //創建信息通道
                using (var channel = connection.CreateModel())
                {
                    string exchangeName = "fanout_exchange";
                    //創建交換機,fanout類型
                    channel.ExchangeDeclare(exchangeName, ExchangeType.Fanout);
                    string queueName1 = "fanout_queue1";
                    string queueName2 = "fanout_queue2";
                    string queueName3 = "fanout_queue3";
                    //創建隊列
                    channel.QueueDeclare(queueName1, false, false, false);
                    channel.QueueDeclare(queueName2, false, false, false);
                    channel.QueueDeclare(queueName3, false, false, false);

                    //把創建的隊列綁定交換機,routingKey 不用給值, 給了也沒意義的
                    channel.QueueBind(queue: queueName1, exchange: exchangeName, routingKey: "");
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: "");
                    channel.QueueBind(queue: queueName3, exchange: exchangeName, routingKey: "");
                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true; //消息持久化
                    //向交換機寫10條消息
                    for (int i = 0; i < 10; i++)
                    {
                        string message = $"RabbitMQ Fanout {i + 1} message";
                        var body = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchangeName, routingKey: "", null, body);
                        Console.WriteLine($"發送Fanout消息{message}");
                    }
                }
            }
        }

        //工作隊列模式(負載均衡)
        // 一個消息生產者，一個交換器，一個消息隊列，多個消費者。同樣也稱為點對點模式

        public static void WorkersendMsg()
        {
            string queueName = "worker_order"; //隊列名
            //創建連接
            using (var connection = RabbitMQHelper.GetConnection())
            {
                //創建通道
                using (var channel = connection.CreateModel())
                {
                    //創建queue
                    channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true; //消息持久化
                    for (var i = 0; i < 10; i++)
                    {
                        string message = $"Hello RabbitMQ MessageHello,{i + 1}";
                        var body = Encoding.UTF8.GetBytes(message);
                        //發送消息到rabbitmq
                        channel.BasicPublish(exchange: "", routingKey: queueName, mandatory: false, basicProperties: properties, body);
                        Console.WriteLine($"發送消息到隊列:{queueName}, 內容:{message} ");
                    }
                }
            }
        }

        //簡單點到點模式,一個Consumer, 一個Producer, 一個Routing ,單個queue
        //測試上SimpleOrder會有10個
        public static void SimpleSendMsg()
        {
            //隊列名
            string queueName = "simple_order";
            //創建連接
            using (var connection = RabbitMQHelper.GetConnection())
            {
                //創建通道
                using (var channel = connection.CreateModel())
                { //創建隊列
                    channel.QueueDeclare(queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);
                    for (var i = 0; i < 10; i++)
                    {
                        string message = $"Hello RabbitMQ MessageHello,{i + 1}";
                        var body = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchange: "", routingKey: queueName, mandatory: false, basicProperties: null, body);
                        Console.WriteLine($"發送消息到隊列:{queueName}, 內容:{message}");
                    }

                }
            }
        }




    }
}

