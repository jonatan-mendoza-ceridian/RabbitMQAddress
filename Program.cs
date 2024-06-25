using DataLayer;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();
string queueName = "address";
channel.QueueDeclare(queue: queueName,
                     durable: false,
                     exclusive: false,
                     autoDelete: false,
                     arguments: null);
channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
var consumer = new EventingBasicConsumer(channel);
channel.BasicConsume(queue: queueName,
                     autoAck: false,
                     consumer: consumer);
Console.WriteLine($" [x] Awaiting RPC requests - Queue Name:{queueName}");
IUserRepository repositories = new UserRepository();
consumer.Received += (model, ea) =>
{
    string response = string.Empty;
    var body = ea.Body.ToArray();
    var props = ea.BasicProperties;
    var replyProps = channel.CreateBasicProperties();
    replyProps.CorrelationId = props.CorrelationId;
    try
    {
        var message = Encoding.UTF8.GetString(body);
        Console.WriteLine($"Message:{message}");
        Address? address = JsonConvert.DeserializeObject<Address>(message);
        if (address != null)
        {            
            Task<bool> interTask = repositories.UpdateAddress(address);
            if (interTask != null)
            {
                string result = "address update fail";
                if (interTask.Result)
                {
                    result = "address update ok";
                }
                response = JsonConvert.SerializeObject(new ActionResult { Result = result });
            }
        }
    }
    catch (Exception e)
    {
        Console.WriteLine($" [.] {e.Message}");
        response = e.Message;
    }
    finally
    {
        Console.WriteLine($"Response : {response}");
        var responseBytes = Encoding.UTF8.GetBytes(response);
        channel.BasicPublish(exchange: string.Empty,
                             routingKey: props.ReplyTo,
                             basicProperties: replyProps,
                             body: responseBytes);
        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
    }


};
Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();