using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using Azure.Storage.Blobs;
using DokanNet;

namespace LocalAzureStorage
{
    public class BlobFile
    {
        public const string DirectoryFileName = ".dir";
        public const char DefaultDirectorySeparator = '/';

        public BlobFile(BlobContainerClient containerClient, string fileName, IDokanFileInfo info)
        {
            BlobClient =
                containerClient.GetBlobClient(GetAzurePath(fileName, info, containerClient.Name, out var isNormalFile));
            IsNormalFile = isNormalFile;
        }

        public BlobClient BlobClient { get; }
        public bool IsNormalFile { get; }

        public static string GetAzurePath(string fileName, IDokanFileInfo info, string containerName,
            out bool isNormalFile)
        {
            if (fileName == null) throw new ArgumentNullException(nameof(fileName));
            var azureFilePath = fileName.Replace(Path.DirectorySeparatorChar, DefaultDirectorySeparator);
            var fullAzureFilePath = $"{containerName}{DefaultDirectorySeparator}{azureFilePath}";
            isNormalFile = true;
            if ((info == null || !info.IsDirectory) && !fileName.EndsWith(Path.DirectorySeparatorChar) &&
                !fileName.EndsWith($"{Path.DirectorySeparatorChar}{DirectoryFileName}"))
                return fullAzureFilePath;
            isNormalFile = false;
            if (fullAzureFilePath.EndsWith($"{DefaultDirectorySeparator}{DirectoryFileName}"))
                return azureFilePath;
            if (fullAzureFilePath.EndsWith($"{DefaultDirectorySeparator}"))
                return $"{fullAzureFilePath}{DirectoryFileName}";
            return $"{fullAzureFilePath}{DefaultDirectorySeparator}{DirectoryFileName}";
        }

        // public static string GetLocalPath(string fileName, IDokanFileInfo info, string cacheDirectoryPath)
        // {
        //     if (fileName == null) throw new ArgumentNullException(nameof(fileName));
        //     var localDirectoryName = Path.GetDirectoryName(fileName) ?? string.Empty;
        //     if ((info == null || !info.IsDirectory) && !fileName.EndsWith(Path.DirectorySeparatorChar) &&
        //         !fileName.EndsWith($"{Path.DirectorySeparatorChar}{DirectoryFileName}"))
        //         return Path.Combine(cacheDirectoryPath, fileName);
        //     return Path.Combine(cacheDirectoryPath, localDirectoryName);
        // }

        public static string GetFileName(string fileName)
        {
            return fileName.Split(DefaultDirectorySeparator).LastOrDefault();
        }

        public bool Exists()
        {
            return BlobClient.Exists();
        }

        public void CreateEmptyFile()
        {
            if (!BlobClient.Exists())
                BlobClient.Upload(new MemoryStream(0));
            // if (BlobClient.Exists() && !LocalCachePath.Exists)
            // {
            //     BlobClient.DownloadTo(LocalCachePath.FullName);
            //     LocalCachePath.Refresh();
            // }
        }

        public void IfNotExistsCreate()
        {
            if (!Exists())
                CreateEmptyFile();
            if (IsNormalFile) return;
            var json = JsonSerializer.Serialize(new DirectoryMetaData {TotalBytesUsed = 0});
            var bytes = Encoding.Default.GetBytes(json);
            BlobClient.Upload(new MemoryStream(bytes));
        }

        public long TotalBytes()
        {
            if (IsNormalFile)
            {
                var prop = BlobClient.GetProperties().Value;
                return prop.ContentLength;
            }

            return JsonSerializer.Deserialize<DirectoryMetaData>(File.ReadAllText(LocalCachePath.FullName))
                .TotalBytesUsed;
        }
    }
}