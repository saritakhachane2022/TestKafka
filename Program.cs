using Confluent.Kafka;
using Google.Apis.Auth.OAuth2;
using KafkaConsoleApp;
using static System.Formats.Asn1.AsnWriter;

class Program
{
    private const string BootstrapServers = "bootstrap.kafka-dev-cluster.us-east4.managedkafka.dev-soc2-001.cloud.goog:9092";
    private const string Topic = "docstore-document-upload-dev";
    private const string Scope = "https://www.googleapis.com/auth/cloud-platform";
    static async Task Main(string[] args)
    {
      //  var tokenProvider = new TokenProvider();
      //  var token = await tokenProvider.GetTokenAsync();

        var producer = new KafkaProducer(BootstrapServers, Topic);
        await producer.ProduceMessageAsync("this is test key", "this is first message to Kafka topic");
    }
}



public class KafkaProducer
{
    private readonly string _bootstrapServers;
    private readonly string _topic;
    private readonly TokenProvider _tokenProvider;

    public KafkaProducer(string bootstrapServers, string topic)
    {
        _bootstrapServers = bootstrapServers;
        _topic = topic;
        _tokenProvider = new TokenProvider();
    }

    // Fix for CS0246: The type or namespace name 'OAuthBearerToken' could not be found

    // The error indicates that the type 'OAuthBearerToken' is not defined or available in the current context.
    // After reviewing the code and the context, it seems that 'OAuthBearerToken' is not a valid type in the Confluent.Kafka library.
    // Instead, the OAuthBearer token refresh logic should be implemented using the 'SetSaslCredentials' method or by setting the appropriate configuration properties.

    public async Task ProduceMessageAsync(string key, string value)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.OAuthBearer,
            MessageTimeoutMs = 30000, // 30 seconds
            RequestTimeoutMs = 30000  // 30 seconds
        };

        using var producer = new ProducerBuilder<string, string>(config)
            .SetOAuthBearerTokenRefreshHandler(async (_, _) =>
            {
                try
                {
                    var (token, expiry) = await _tokenProvider.GetTokenAsync();
                    config.Set("sasl.oauthbearer.token", token);
                    config.Set("sasl.oauthbearer.token.expiration", expiry.ToString());
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] Token refresh failed: {ex.Message}");
                }
            })
            .Build();

        try
        {
            var deliveryResult = await producer.ProduceAsync(_topic, new Message<string, string> { Key = key, Value = value });

            Console.WriteLine($"[SUCCESS] Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}'");
        }
        catch (ProduceException<string, string> e)
        {
            Console.WriteLine($"[ERROR] Delivery failed: {e.Error.Reason}");
        }
    }
}

/*
public class KafkaProducer
{
    private readonly string _bootstrapServers;
    private readonly string _topic;
    private readonly string _token;

    public KafkaProducer(string bootstrapServers, string topic, string token)
    {
        _bootstrapServers = bootstrapServers;
        _topic = topic;
        _token = token;
    }

    public async Task ProduceMessageAsync(string key, string value)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.OAuthBearer,
            MessageTimeoutMs = 30000, // 30 seconds
            RequestTimeoutMs = 30000  // 30 seconds
        };

        using var producer = new ProducerBuilder<string, string>(config)
            .SetOAuthBearerTokenRefreshHandler((_, _) =>
            {
                // Fix: Replace the incorrect usage of OAuthBearerToken with a valid token refresh logic
                var token = _token; // Use the provided token
                var expiration = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeSeconds();
                config.Set("sasl.oauthbearer.token", token);
                config.Set("sasl.oauthbearer.token.expiration", expiration.ToString());
            })
            .Build();

        try
        {
            var deliveryResult = await producer.ProduceAsync(_topic, new Message<string, string>
            {
                Key = key,
                Value = value
            });

            Console.WriteLine($"Message delivered to {deliveryResult.TopicPartitionOffset}");
        }
        catch (ProduceException<string, string> ex)
        {
            Console.WriteLine($"Delivery failed: {ex.Error.Reason}");
        }
    }
}

public class GcpTokenProvider
{
    private readonly string _scope;

    public GcpTokenProvider(string scope)
    {
        _scope = scope;
    }

    public async Task<string> GetTokenAsync()
    {
        var googleCredential = await GoogleCredential.GetApplicationDefaultAsync();
        if (googleCredential.IsCreateScopedRequired)
        {
            googleCredential = googleCredential.CreateScoped(_scope);
        }

        var token = await googleCredential.UnderlyingCredential.GetAccessTokenForRequestAsync();
        return token;
    }
}

*/
