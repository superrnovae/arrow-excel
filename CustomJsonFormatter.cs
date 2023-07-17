using System;
using System.IO;
using System.Net.Http;
using System.Net;
using System.Net.Http.Formatting;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Text.Json;
using System.Net.Http.Headers;

namespace WebApplication13
{
    public class CustomJsonFormatter : JsonMediaTypeFormatter
    {
        public CustomJsonFormatter()
        {
            SupportedMediaTypes.Add(MediaTypeHeaderValue.Parse("application/json"));
            SupportedMediaTypes.Add(MediaTypeHeaderValue.Parse("text/json"));
            SupportedMediaTypes.Add(MediaTypeHeaderValue.Parse("text/plain"));
            SupportedEncodings.Add(new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true));
            SupportedEncodings.Add(new UnicodeEncoding(bigEndian: false, byteOrderMark: true, throwOnInvalidBytes: true));
        }

        public override bool CanReadType(Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException(nameof(type));
            }

            return true;
        }

        public override bool CanWriteType(Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException(nameof(type));
            }

            return true;
        }

        public override object ReadFromStream(Type type, Stream readStream, Encoding effectiveEncoding, IFormatterLogger formatterLogger)
        {
            if (type is null)
            {
                throw new ArgumentNullException(nameof(type));
            }

            if (readStream is null)
            {
                throw new ArgumentNullException(nameof(readStream));
            }

            try
            {
                return JsonSerializer.Deserialize(readStream, type);
            }
            catch(JsonException ex)
            {
                if (formatterLogger == null)
                    throw;

                formatterLogger?.LogError(ex.Path, ex.Message);
                return GetDefaultValueForType(type);
            }
        }

        public override void WriteToStream(Type type, object value, Stream writeStream, Encoding effectiveEncoding)
        {
            if (type is null)
            {
                throw new ArgumentNullException(nameof(type));
            }

            if (value is null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            if (writeStream is null)
            {
                throw new ArgumentNullException(nameof(writeStream));
            }

            JsonSerializer.Serialize(writeStream, value, type);
        }

        public override Task<object> ReadFromStreamAsync(Type type, Stream readStream, HttpContent content, IFormatterLogger formatterLogger, CancellationToken cancellationToken = default)
        {
            return DeserializeFromStreamAsync(readStream, type, content, formatterLogger, cancellationToken);
        }

        public override Task<object> ReadFromStreamAsync(Type type, Stream readStream, HttpContent content, IFormatterLogger formatterLogger)
        {
            return DeserializeFromStreamAsync(readStream, type, content, formatterLogger);
        }

        public override Task WriteToStreamAsync(Type type, object value, Stream writeStream, HttpContent content, TransportContext transportContext)
        {
            return SerializeToStreamAsync(type, value, writeStream);
        }

        public override Task WriteToStreamAsync(Type type, object value, Stream writeStream, HttpContent content, TransportContext transportContext, CancellationToken cancellationToken)
        {
            return SerializeToStreamAsync(type, value, writeStream, token: cancellationToken);
        }

        private async Task<object> DeserializeFromStreamAsync(Stream readStream, Type type, HttpContent content, IFormatterLogger formatterLogger, CancellationToken cancellationToken = default)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));
            if (readStream == null) throw new ArgumentNullException(nameof(readStream));

            HttpContentHeaders httpContentHeaders = content?.Headers;

            if (httpContentHeaders != null && httpContentHeaders.ContentLength == 0)
            {
                return GetDefaultValueForType(type);
            }

            try
            {
                return await JsonSerializer.DeserializeAsync(readStream, type, cancellationToken: cancellationToken);
            }
            catch (Exception exception)
            {
                if (formatterLogger == null)
                {
                    throw;
                }

                formatterLogger.LogError(string.Empty, exception);
                return GetDefaultValueForType(type);
            }
        }

        private async Task SerializeToStreamAsync(Type type, object value, Stream writeStream, CancellationToken token = default)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));
            if (value == null) throw new ArgumentNullException(nameof(value));
            if (writeStream == null) throw new ArgumentNullException(nameof(writeStream));

            await JsonSerializer.SerializeAsync(writeStream, value, type, cancellationToken: token);
        }
    }
}
