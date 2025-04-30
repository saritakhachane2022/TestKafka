using Confluent.Kafka;
using Google.Apis.Auth.OAuth2;
using KafkaConsoleApp;
using System.Net.NetworkInformation;
using static System.Formats.Asn1.AsnWriter;


using System;
using System.Threading.Tasks;


public class Program
{
    static string? googleToken = null;
   static DateTime lastTokenRefresh;
    static async Task Main(string[] args)
    {
       // await Producerr();
        await Consumerr();
        // var token = await FetchAccessTokenAsync();

        //var config = new ProducerConfig
        //{
        //    BootstrapServers = "bootstrap.kafka-dev-cluster.us-east4.managedkafka.dev-soc2-001.cloud.goog:9092",
        //    SecurityProtocol = SecurityProtocol.SaslSsl,
        //    SaslMechanism = SaslMechanism.OAuthBearer,
        //    MessageTimeoutMs = 30000, // 30 seconds
        //    RequestTimeoutMs = 30000  // 30 seconds
        //};

        // Inject the dynamically fetched token
        //  config.Set("sasl.oauthbearer.token", token);
        //  config.Set("sasl.oauthbearer.config", $"token={token}");
        // config.Set("sasl.oauthbearer.config", "scope=your-scope principal=your-principal");

        /*  using var producer = new ProducerBuilder<string, string>(config)
         .SetOAuthBearerTokenRefreshHandler(async (c, _) =>
         {
             try
             {
                 var token = await FetchAccessTokenAsync();
                 Console.WriteLine($"Fetched Token: {token}");
                 var expirationInMilliseconds = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeMilliseconds();

                 Console.WriteLine($"Now: {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}, Exp: {expirationInMilliseconds}");
                 c.OAuthBearerSetToken(token, expirationInMilliseconds, "unused");
             }
             catch (Exception ex)
             {
                 c.OAuthBearerSetTokenFailure(ex.Message);
             }
         })
         .Build();
        */

    }
    /*

    // Fetch the access token dynamically
    static async Task<string> FetchAccessTokenAsync()
    {
         GoogleCredential credential = await GoogleCredential.GetApplicationDefaultAsync();
          // Request an access token for Cloud Platform scope
          var accessToken = await credential.UnderlyingCredential.GetAccessTokenForRequestAsync();
          return accessToken;
        

       
    }
    */

    static async Task Producerr()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "bootstrap.kafka-dev-cluster.us-east4.managedkafka.dev-soc2-001.cloud.goog:9092",
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            // sasl.jaas.config = org.apache.kafka.common.security.plain.PlainLoginModule required
            SaslUsername = "phoenix-dev-sa@global-sharedsvc-dev-001.iam.gserviceaccount.com",
            SaslPassword = await GetGoogleAccessToken(),
            MessageTimeoutMs = 30000, // 30 seconds
            RequestTimeoutMs = 30000  // 30 seconds
        };
        using var producer = new ProducerBuilder<string, string>(config)
  .Build();
        //using var producer = new ProducerBuilder<string, string>(config).Build();

        var topic = "docstore-document-upload-dev";
        var message = new Message<string, string> { Key = "key1", Value = "Hello Kafka with dynamic OAuthBearer!" };

        try
        {
            var deliveryResult = await producer.ProduceAsync(topic, message);
            Console.WriteLine($"Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}'");
        }
        catch (ProduceException<string, string> ex)
        {
            Console.WriteLine($"Delivery failed: {ex.Error.Reason}");
        }
    }






    static async Task Consumerr()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "bootstrap.kafka-dev-cluster.us-east4.managedkafka.dev-soc2-001.cloud.goog:9092",
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = "phoenix-dev-sa@global-sharedsvc-dev-001.iam.gserviceaccount.com",
            SaslPassword = await GetGoogleAccessToken(),
            GroupId = "doc-consumer-group-id",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        var topic = "docstore-document-upload-dev";

        using var consumer = new ConsumerBuilder<string, string>(config).Build();

        consumer.Subscribe(topic);

        Console.WriteLine("Consumer started. Press Ctrl+C to stop.");

        var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        try
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    var cr = consumer.Consume(cts.Token);
                    Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Consume error: {e.Error.Reason}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Gracefully exit
        }
        finally 
        {
            consumer.Close();
        }
        try
        {
            var cr = consumer.Consume(cts.Token);
            Console.WriteLine($"Consumed message '{cr.Message.Value}' at: '{cr.TopicPartitionOffset}'.");
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Consume error: {e.Error.Reason}");
        }
        {
            consumer.Close();
        }

    }



    static  async Task<string> GetGoogleAccessToken()
    {
        if (!string.IsNullOrEmpty(googleToken) && (lastTokenRefresh.AddSeconds(10) >= DateTime.UtcNow))
        {
            return googleToken;
        }

        try
        {
            GoogleCredential credential = await GoogleCredential.GetApplicationDefaultAsync();
            ITokenAccess tokenAccess = credential.CreateScoped("https://www.googleapis.com/auth/cloud-platform");
            googleToken = await tokenAccess.GetAccessTokenForRequestAsync();
            lastTokenRefresh = DateTime.UtcNow;
            return googleToken;
        }
        catch (Exception ex)
        {
            throw new Exception(ex.Message); // "Please use `gcloud auth application-default print-access-token` on the commandline to authenticate with google.");
        }
    }
}

/*
class Program
{
    private const string BootstrapServers = "bootstrap.kafka-dev-cluster.us-east4.managedkafka.dev-soc2-001.cloud.goog:9092";
    private const string Topic = "docstore-document-upload-dev";
    private const string Scope = "https://www.googleapis.com/auth/cloud-platform";

    static async Task Main(string[] args)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.OAuthBearer
        };
          var tokenProvider = new TokenProvider();
         var token = await tokenProvider.GetTokenAsync();


        using var producer = new ProducerBuilder<string, string>(config)
            .SetOAuthBearerTokenRefreshHandler(async (c, _) =>
            {
                try
                {
                    var credential = await GoogleCredential.GetApplicationDefaultAsync();
                    if (credential.IsCreateScopedRequired)
                    {
                        credential = credential.CreateScoped(Scope);
                    }

                   // var accessToken = token;// await credential.UnderlyingCredential.GetAccessTokenForRequestAsync();
                   // var expiry = DateTime.UtcNow.AddMinutes(50); // Adjust according to token expiry

                   
c.OAuthBearerSetToken(token.token, ((DateTimeOffset)DateTimeOffset.FromUnixTimeSeconds((long)token.expiry)).ToUnixTimeSeconds(), "unused");
                }
                catch (Exception ex)
                {
                    c.OAuthBearerSetTokenFailure(ex.Message);
                }
            })
            .Build();

        try
        {
            var deliveryResult = await producer.ProduceAsync(Topic, new Message<string, string>
            {
                Key = "key-test",
                Value = "Hello Kafka from C# with OAuth!"
            });

            Console.WriteLine($"Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}'");
        }
        catch (ProduceException<string, string> e)
        {
            Console.WriteLine($"[ERROR] {e.Error.Reason}");
        }
    }
}

*/
/*
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

*/
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
