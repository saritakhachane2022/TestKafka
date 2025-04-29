using Confluent.Kafka;
using Google.Apis.Auth.OAuth2;
using KafkaConsoleApp;
using System.Net.NetworkInformation;
using static System.Formats.Asn1.AsnWriter;


using System;
using System.Threading.Tasks;


public class Program
{
    static async Task Main(string[] args)
    {
       // var token = await FetchAccessTokenAsync();

        var config = new ProducerConfig
        {
            BootstrapServers = "bootstrap.kafka-dev-cluster.us-east4.managedkafka.dev-soc2-001.cloud.goog:9092",
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.OAuthBearer,
            MessageTimeoutMs = 30000, // 30 seconds
            RequestTimeoutMs = 30000  // 30 seconds
        };

        // Inject the dynamically fetched token
        //  config.Set("sasl.oauthbearer.token", token);
        //  config.Set("sasl.oauthbearer.config", $"token={token}");
        // config.Set("sasl.oauthbearer.config", "scope=your-scope principal=your-principal");

        using var producer = new ProducerBuilder<string, string>(config)
       .SetOAuthBearerTokenRefreshHandler(async (c, _) =>
       {
           try
           {
               var token = "eyJ0eXAiOiAiSldUIiwgImFsZyI6ICJHT09HX09BVVRIMl9UT0tFTiJ9.eyJleHAiOiAxNzQ1OTQ0OTY4LjE1NTU0NSwgImlhdCI6IDE3NDU5NDQwODQuMTU1NTkxLCAiaXNzIjogIkdvb2dsZSIsICJzY29wZSI6ICJrYWZrYSIsICJzdWIiOiAicGhvZW5peC1kZXYtc2FAZ2xvYmFsLXNoYXJlZHN2Yy1kZXYtMDAxLmlhbS5nc2VydmljZWFjY291bnQuY29tIn0.eWEyOS5jLmMwQVNSSzBHYkgyZjN0MlB2dkpNeUExQzhaN2gyWDdaQU5Dblp1aWdfZ3M3Y2d1RXBXTEhfeHRUaXJQMXVYLWJGemFWeHRYZ3d0bGJLU1VIaTV2SWhEaXpjd1VrR0dHeTVkYzVqQ1dYMlFuVmtBYThwUmVMLThOTDZXTVhMbzBMZ0kzRUpISVI2SEJEaHhLem5IY3hjRG9PVV9FRGFLZ3VoT1B3RVVJY3pTMXZzZDJ0Tkl2dDJYQUdfZmNDcm93QmdWY2p2R1FLcng3cnRGQTJXaHgwUk9EczV3RmNsN3BLcERvZkhFWG9mMUNIcHZ1dnBaSUdfcm1aNXFUZkxpV2c5RmFtQUw0M3BidF9VT0J0Q3ZrSXBHWWVQRXdoTDFXRUpHQXVTcTlTUDlZR19Xd2U2NklEeVF3ZURVc1FLZ2NqZW54ck1hU21wRVM5NG9sV3UyX0RPdDhqYURXMmpXZWdMZkNzaGJmZ0xwejgzazltZi1XcEtURkVvcm5nNXRhVmlaTEMxdGhZdVJ6bTVSTkFBempDUFZhcGlmaDc1RmNfemFuOTB5NlZ3ci02eVVNU3J4SDRQRXJuay1VTEpkcjcxc1VibndfZ2VZT3RWdklyemNrODhkNi1wdW5ZSXExeDFMbUdxX3d3VjFIX2pZTE1ZVGtHa1doQTZwUFpqR2xDTGRWUDN5ZGxaVDhQSDdCZVE0UnlFMnJtRDRxMFZQM1JET3JQaUVKUGRRVU42eW5rTUt0SWpQRFRsazJ6bDB2Rnh0WkhrbVBUQnRhalBObkJ6bTJ3ZlRVOE9CYXF1aTRiMlBsT1VFNjIwUFNVVXR0VVM0OTVrcDBtc0pJNlpqcGdvdldrUXk1clNJajBrdzN1QnhzNk9pOHh2cEY5UVFTOXNiU2FvNFhmU21pXzBwdjdzcEp3NXl2OVdJUjMya0pReGM0WUJteFhZb2RXVWxWVWcyand5XzF2Snd2TXJhWC1hd2FxbTRZV2pfNy1tMTd4alljZndkblVtbWF4bTZiZlowd3p2NFVRMmYxMG9RdzZkYmMtcWJmZmFfWkJfMDdZTVVJcThYZXRoY3JqWTg1Rl9rVThfendkUVdpbFNSMW1GaHRneDFpSS1WbGNXRmtqMkYzdGZ5bmZpbmtPU0JrenB4VXJRX25oeGdPZ19ucU1GMFM1a2w1endjLUIzZDkyLU9PcnptZlV6SnY3QkpXZVpRMnJTZXFPRjI3d2N0WVNTenNjMmtNY3VhVVJWWm4ya2Qzel9SZmFnaHJ4NllTQjcwazRqeWlRWmRRMnNnRlUtdTBqcEl2UnFraDRGMDJ3MnhNdlZNLVJxbGplNGl6Z3ZyMWJ6dm1aTQ";
                   //await FetchAccessTokenAsync();
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

    // Fetch the access token dynamically
    static async Task<string> FetchAccessTokenAsync()
    {
         GoogleCredential credential = await GoogleCredential.GetApplicationDefaultAsync();
          // Request an access token for Cloud Platform scope
          var accessToken = await credential.UnderlyingCredential.GetAccessTokenForRequestAsync();
          return accessToken;
        

       
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
