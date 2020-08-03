using System;
using System.IO;
using System.Security.Cryptography;
using DokanNet;

namespace LocalAzureStorage
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            const string connectionString =
                "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://192.168.1.5:10000/devstoreaccount1;QueueEndpoint=http://192.168.1.5:10001/devstoreaccount1;";
            const string containerName = "test1";
            var cache = new DirectoryInfo(Path.Combine("cache"));
            if (!cache.Exists)
            {
                cache.Create();
                cache.Refresh();
            }

            var mount = new DirectoryInfo(Path.Combine("mount"));
            if (!mount.Exists)
            {
                mount.Create();
                mount.Refresh();
            }

            var fs = new AzureBlobFileSystem(connectionString, containerName, cache);
            fs.Mount(mount.FullName);

            // var cancellationToken = new CancellationToken();
            // var container = new BlobContainerClient(connectionString, containerName);
            // await container.CreateIfNotExistsAsync(cancellationToken: cancellationToken);
            // foreach (var file in Directory.GetFiles(@"C:\_Development\Sprocket\Sprocket\Sprocket.Web\SavedFiles"))
            // {
            //     var name = $"savedFiles\\{file}";
            //     var blobClient = container.GetBlobClient(name);
            //     if (!await blobClient.ExistsAsync(cancellationToken))
            //     {
            //         await blobClient.UploadAsync(File.OpenRead(file), cancellationToken);
            //     }
            //
            //     var properties = await blobClient.GetPropertiesAsync(cancellationToken: cancellationToken);
            //     if (properties.Value.Metadata.TryGetValue("MD5", out string md5Hash))
            //     {
            //         var newHash = CalculateMd5(file);
            //         if (newHash == md5Hash) continue;
            //         await blobClient.DeleteIfExistsAsync(cancellationToken: cancellationToken);
            //         await blobClient.UploadAsync(File.OpenRead(file), cancellationToken);
            //     }
            //
            //     properties.Value.Metadata["MD5"] = CalculateMd5(file);
            //     await blobClient.SetMetadataAsync(properties.Value.Metadata, cancellationToken: cancellationToken);
            // }
            // //var blob = container.GetBlobClient();
            // //blob.Upload(filePath);
        }

        private static string CalculateMd5(string filename)
        {
            using var md5 = MD5.Create();
            using var stream = File.OpenRead(filename);
            var hash = md5.ComputeHash(stream);
            return BitConverter.ToString(hash).Replace("-", string.Empty).ToLowerInvariant();
        }
    }
}