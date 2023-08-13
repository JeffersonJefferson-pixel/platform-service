using System.Text;
using System.Text.Json;
using PlatformService.Dtos;
using RabbitMQ.Client;

namespace PlatformService.AsyncDataServices
{
  public class MessageBusClient : IMessageBusClient
  {
    private readonly IConfiguration _configuration;
    private readonly IConnection _connection;
    private readonly IModel _channel;

    public MessageBusClient(IConfiguration configuration)
    {
      _configuration = configuration;
      var factory = new ConnectionFactory()
      {
        HostName = _configuration["RabbitMQHost"],
        Port = int.Parse(_configuration["RabbitMQPort"])
      };
      try
      {
        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();

        _channel.ExchangeDeclare(exchange: "trigger", type: ExchangeType.Fanout);
        _connection.ConnectionShutdown += RabbitMQ_ConnectionShutdown;

        Console.WriteLine("--> Connected to Message Bus");
      }
      catch (Exception ex)
      {
        Console.WriteLine($"--> Could not connect to the Message Bus {ex.Message}");
      }
    }

    public void PublishNewPlatform(PlatformPublishDto platformPublishDto)
    {
      var message = JsonSerializer.Serialize(platformPublishDto);

      if (_connection.IsOpen)
      {
        Console.WriteLine("--> RabbitMQ Connection Open, sending message...");
        sendMessage(message);
      }
      else
      {
        Console.WriteLine("--> RabbitMQ connection is closed, not sending");
      }
    }

    public void Dispose()
    {
      Console.WriteLine("Message Bus Disposed");
      if (_channel.IsOpen)
      {
        _channel.Close();
        _connection.Close();
      }
    }

    private void sendMessage(String message)
    {
      var body = Encoding.UTF8.GetBytes(message);
      _channel.BasicPublish(
        exchange: "trigger",
        routingKey: "",
        basicProperties: null,
        body: body
      );
      Console.WriteLine($"--> Sent {message}");
    }

    private void RabbitMQ_ConnectionShutdown(object sender, ShutdownEventArgs e)
    {
      Console.WriteLine("--> RabbitMQ Connection Shutdown");
    }
  }
}