// See https://aka.ms/new-console-template for more information
//簡單隊列模式
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ_OK;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Text;
using static RabbitMQ_OK.Recevie;
using static RabbitMQ_OK.Send;

public class RabbitMQHelper
{
   

    //Consumer類
    private static ConnectionFactory factory;
    private static object lookObj = new object();

    public static IConnection GetConnection()
    {
        if (factory == null)
        {
            lock (lookObj)
            {
                if (factory == null)
                {
                    factory = new ConnectionFactory
                    {
                        HostName = "192.168.100.215",
                        Port = 5672,
                        UserName = "newt",
                        Password = "newt",
                    };
                }
            }
        }
        return factory.CreateConnection();
    }



    //主程式
    static void Main(string[] args)
    {
        Console.WriteLine("Hello World!");
        //簡單點對點模式
        Send.SimpleSendMsg();   //產生十個SimpleOrder1消息
        Recevie.SimpleConsumer();  //消耗掉SimpleOrder1

        //工作模式, 一個生產者,多個消費者,是競爭關係
        //Send.WorkersendMsg();
        //Recevie.WorkerConsumer();
        //Recevie.WorkerConsumer();

        //發布訂閱模式
        //Send.SendMessageFanout();
        //Recevie.FanoutConsumer();

        //路由模式
        //Send.SendDirect();
        //Recevie.DirectConsumer();

        //主題模式
        //Send.SendMessageTopic();
        // Recevie.TopicConsumer();

        //RPC模式 ,Client端設定兩屬性，ReplyTo代表隊列名稱, correlationId：標記request
        //Server端根據這兩屬性回傳，Client端最後接收根據這兩個屬性判斷是符合的就正確了
        //啟動服務端,正常邏輯在另一個程式
        //RPCServer.RpcHandle();
        //創立客戶端物件
        //var rpcClient = new RPCClient();
        //string message = $"消息id:{new Random().Next(1, 1000)}";
        //Console.WriteLine($"[客戶端]RPC請求中,{message}");
        ////項服務端發送消息,等待回復
        //var response = rpcClient.Call(message);
        //Console.WriteLine("[客戶端]收到回覆響應:{0}", response);
        //rpcClient.Close();
        //Console.ReadKey();
    }



    public class RPCServer
    {
        public static void RpcHandle()
        {

            var connection = RabbitMQHelper.GetConnection();
            {
                var channel = connection.CreateModel();
                {
                    string queueName = "rpc_queue";
                    channel.QueueDeclare(queue: queueName, durable: false,
                      exclusive: false, autoDelete: false, arguments: null);
                    channel.BasicQos(0, 1, false);
                    var consumer = new EventingBasicConsumer(channel);
                    channel.BasicConsume(queue: queueName,
                      autoAck: false, consumer: consumer);
                    Console.WriteLine("【服務端】等待RPC請求...");

                    consumer.Received += (model, ea) =>
                    {
                        string response = null;

                        var body = ea.Body.ToArray();
                        var props = ea.BasicProperties;
                        var replyProps = channel.CreateBasicProperties();
                        replyProps.CorrelationId = props.CorrelationId;

                        try
                        {
                            var message = Encoding.UTF8.GetString(body);
                            Console.WriteLine($"【服務端】接收到資料:{message},開始處理");
                            response = $"消息:{message},處理完成";
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("錯誤:" + e.Message);
                            response = "";
                        }
                        finally
                        {
                            var responseBytes = Encoding.UTF8.GetBytes(response);
                            channel.BasicPublish(exchange: "", routingKey: props.ReplyTo,
                              basicProperties: replyProps, body: responseBytes);
                            channel.BasicAck(deliveryTag: ea.DeliveryTag,
                              multiple: false);
                        }
                    };
                }
            }
        }

    }

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
            connection = RabbitMQHelper.GetConnection();

            channel = connection.CreateModel();
            replyQueueName = channel.QueueDeclare().QueueName;
            consumer = new EventingBasicConsumer(channel);

            props = channel.CreateBasicProperties();
            var correlationId = Guid.NewGuid().ToString();
            props.CorrelationId = correlationId; //给消息id
            props.ReplyTo = replyQueueName;//回調的隊列名，Client關閉後會自動刪除

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
}