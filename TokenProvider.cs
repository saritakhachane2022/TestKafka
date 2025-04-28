using Google.Apis.Auth.OAuth2;


using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using Google.Apis.Util;
using Google.Apis.Auth.OAuth2.Responses;

namespace KafkaConsoleApp
{
    public class TokenProvider
    {
        private GoogleCredential _credential;

        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        private const string HeaderJson = "{\"typ\":\"JWT\",\"alg\":\"GOOG_OAUTH2_TOKEN\"}";

        public TokenProvider()
        {
            _credential = GoogleCredential.GetApplicationDefault();
        }

        private async Task<GoogleCredential> GetCredentialsAsync()
        {
            if (_credential.IsCreateScopedRequired)
            {
                _credential = _credential.CreateScoped("https://www.googleapis.com/auth/cloud-platform");
            }

            var accessToken = await _credential.UnderlyingCredential.GetAccessTokenForRequestAsync();
            if (string.IsNullOrEmpty(accessToken))
            {
                throw new InvalidOperationException("Unable to retrieve access token.");
            }

            return _credential;
        }

        private string GetJwtPayload(ITokenAccess tokenAccess)
        {
            var expiry = DateTimeOffset.UtcNow.AddMinutes(60).ToUnixTimeSeconds();
            var issuedAt = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            var payload = new
            {
                exp = expiry,
                iat = issuedAt,
                iss = "Google",
                scope = "kafka",
                sub = "service-account@example.com" // Hard-coded: ideally fetch dynamically if needed
            };

            return JsonSerializer.Serialize(payload, JsonOptions);
        }

        private static string Base64UrlEncode(string input)
        {
            var bytes = Encoding.UTF8.GetBytes(input);
            var base64 = Convert.ToBase64String(bytes);
            return base64.Replace("+", "-").Replace("/", "_").TrimEnd('=');
        }

        public async Task<(string token, double expiry)> GetTokenAsync()
        {
            await GetCredentialsAsync();

            var accessToken = await _credential.UnderlyingCredential.GetAccessTokenForRequestAsync();

            var header = Base64UrlEncode(HeaderJson);
            var payload = Base64UrlEncode(GetJwtPayload(_credential.UnderlyingCredential));
            var tokenPart = Base64UrlEncode(accessToken);

            var token = $"{header}.{payload}.{tokenPart}";

            var expiry = DateTimeOffset.UtcNow.AddMinutes(60).ToUnixTimeSeconds();

            return (token, expiry);
        }
    }

}
