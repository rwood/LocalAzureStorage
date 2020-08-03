using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using System.Threading;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using DokanNet;
using FileAccess = DokanNet.FileAccess;

namespace LocalAzureStorage
{
    public class AzureBlobFileSystem : IDokanOperations
    {
        private readonly BlobContainerEncryptionScopeOptions _blobContainerEncryptionScopeOptions;

        private readonly ConcurrentDictionary<string, BlobFile> _cached =
            new ConcurrentDictionary<string, BlobFile>();

        private readonly DirectoryInfo _cacheDirectory;
        private readonly CancellationToken _cancellationToken = new CancellationToken();
        private readonly string _connectionString;
        private readonly IDictionary<string, string> _containerMetaData;
        private readonly string _containerName;
        private readonly PublicAccessType _publicAccessType;
        private BlobContainerClient _containerClient;
        private BlobFile _rootDir;

        public AzureBlobFileSystem(string connectionString, string containerName, DirectoryInfo cacheDirectory,
            PublicAccessType publicAccessType = PublicAccessType.None,
            IDictionary<string, string> containerMetaData = null,
            BlobContainerEncryptionScopeOptions blobContainerEncryptionScopeOptions = null)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _containerName = containerName ?? throw new ArgumentNullException(nameof(containerName));
            _cacheDirectory = cacheDirectory ?? throw new ArgumentNullException(nameof(cacheDirectory));
            _publicAccessType = publicAccessType;
            _containerMetaData = containerMetaData;
            _blobContainerEncryptionScopeOptions = blobContainerEncryptionScopeOptions;
            if (!_cacheDirectory.Exists)
                throw new DirectoryNotFoundException(nameof(cacheDirectory));
        }

        public NtStatus Mounted(IDokanFileInfo info)
        {
            try
            {
                _containerClient = new BlobContainerClient(_connectionString, _containerName);
                _containerClient.CreateIfNotExists(_publicAccessType, _containerMetaData,
                    _blobContainerEncryptionScopeOptions, _cancellationToken);
                if (!IsNormalFile(Path.DirectorySeparatorChar.ToString(), null, out _rootDir) && !_rootDir.Exists())
                    _rootDir.IfNotExistsCreate();
                return DokanResult.Success;
            }
            catch (Exception ex)
            {
                return DokanResult.Error;
            }
        }

        public NtStatus Unmounted(IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }


        public NtStatus CreateFile(string fileName, FileAccess access, FileShare share, FileMode mode,
            FileOptions options,
            FileAttributes attributes, IDokanFileInfo info)
        {
            if (!IsNormalFile(fileName, info, out var file))
                switch (mode)
                {
                    case FileMode.Open:
                        return file.Exists() ? DokanResult.Success : DokanResult.PathNotFound;
                    case FileMode.CreateNew:
                        if (file.Exists())
                            return DokanResult.FileExists;
                        file.CreateEmptyFile();
                        return DokanResult.Success;
                    case FileMode.Create:
                    case FileMode.OpenOrCreate:
                    case FileMode.Truncate:
                    case FileMode.Append:
                    default:
                        throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
                }

            var result = DokanResult.Success;
            switch (mode)
            {
                case FileMode.CreateNew:
                    if (file.Exists())
                        result = DokanResult.FileExists;
                    file.CreateEmptyFile();
                    break;
                case FileMode.Append:
                case FileMode.Open:
                    result = file.Exists() ? DokanResult.Success : DokanResult.FileNotFound;
                    break;
                case FileMode.Create:
                case FileMode.OpenOrCreate:
                    if (!file.Exists())
                        file.CreateEmptyFile();
                    result = file.Exists() ? DokanResult.Success : DokanResult.FileNotFound;
                    break;
                case FileMode.Truncate:
                    file.BlobClient.DeleteIfExists();
                    file.CreateEmptyFile();
                    result = DokanResult.Success;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
            }

            if (result != DokanResult.Success) return result;
            var fs = new FileStream(file.LocalCachePath.FullName, mode);
            if (mode == FileMode.Append)
                fs.Seek(0, SeekOrigin.End);
            info.Context = fs;
            return result;
        }

        public void Cleanup(string fileName, IDokanFileInfo info)
        {
        }

        public void CloseFile(string fileName, IDokanFileInfo info)
        {
        }

        public NtStatus ReadFile(string fileName, byte[] buffer, out int bytesRead, long offset, IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus WriteFile(string fileName, byte[] buffer, out int bytesWritten, long offset,
            IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus FlushFileBuffers(string fileName, IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus GetFileInformation(string fileName, out FileInformation fileInfo, IDokanFileInfo info)
        {
            var isFile = IsNormalFile(fileName, info, out var file);
            if (file.Exists())
            {
                var properties = file.BlobClient.GetProperties();
                fileInfo = new FileInformation
                {
                    Attributes = isFile ? FileAttributes.Normal : FileAttributes.Directory,
                    Length = properties.Value.ContentLength,
                    CreationTime = properties.Value.CreatedOn.LocalDateTime, FileName = BlobFile.GetFileName(fileName),
                    LastAccessTime = properties.Value.LastModified.LocalDateTime,
                    LastWriteTime = properties.Value.LastModified.LocalDateTime
                };
                return DokanResult.Success;
            }

            fileInfo = new FileInformation();
            return DokanResult.AccessDenied;
        }

        public NtStatus FindFiles(string fileName, out IList<FileInformation> files, IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus FindFilesWithPattern(string fileName, string searchPattern, out IList<FileInformation> files,
            IDokanFileInfo info)
        {
            if (!IsNormalFile(fileName, info, out var cachedDir))
            {
                files = _containerClient
                    .GetBlobsByHierarchy(
                        prefix: cachedDir.BlobClient.Name.Replace(BlobFile.DirectoryFileName, string.Empty))
                    .Where(f => !f.Blob.Name.EndsWith(
                                    $"{BlobFile.DefaultDirectorySeparator}{BlobFile.DirectoryFileName}") &&
                                DokanHelper.DokanIsNameInExpression(searchPattern, fileName, true))
                    .Select(f =>
                    {
                        var fi = new FileInformation();
                        fi.FileName = BlobFile.GetFileName(f.Blob.Name);
                        fi.Attributes = f.Blob.Name.EndsWith(BlobFile.DefaultDirectorySeparator)
                            ? FileAttributes.Directory
                            : FileAttributes.Normal;
                        if (f.Blob.Properties.ContentLength.HasValue)
                            fi.Length = f.Blob.Properties.ContentLength.Value;
                        if (f.Blob.Properties.CreatedOn.HasValue)
                            fi.CreationTime = f.Blob.Properties.CreatedOn.Value.LocalDateTime;
                        if (f.Blob.Properties.LastModified != null)
                        {
                            fi.LastAccessTime = f.Blob.Properties.LastModified.Value.LocalDateTime;
                            fi.LastWriteTime = f.Blob.Properties.LastModified.Value.LocalDateTime;
                        }

                        return fi;
                    })
                    .ToList();
                return DokanResult.Success;
            }

            files = new List<FileInformation>();
            return DokanResult.FileNotFound;
        }

        public NtStatus SetFileAttributes(string fileName, FileAttributes attributes, IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus SetFileTime(string fileName, DateTime? creationTime, DateTime? lastAccessTime,
            DateTime? lastWriteTime,
            IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus DeleteFile(string fileName, IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus DeleteDirectory(string fileName, IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus MoveFile(string oldName, string newName, bool replace, IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus SetEndOfFile(string fileName, long length, IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus SetAllocationSize(string fileName, long length, IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus LockFile(string fileName, long offset, long length, IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus UnlockFile(string fileName, long offset, long length, IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus GetDiskFreeSpace(out long freeBytesAvailable, out long totalNumberOfBytes,
            out long totalNumberOfFreeBytes,
            IDokanFileInfo info)
        {
            freeBytesAvailable = long.MaxValue;
            totalNumberOfFreeBytes = long.MaxValue;
            totalNumberOfBytes = _rootDir.TotalBytes();
            return DokanResult.Success;
        }

        public NtStatus GetVolumeInformation(out string volumeLabel, out FileSystemFeatures features,
            out string fileSystemName,
            out uint maximumComponentLength, IDokanFileInfo info)
        {
            volumeLabel = _containerName;
            fileSystemName = "NTFS";
            maximumComponentLength = 256;
            features = FileSystemFeatures.CasePreservedNames | FileSystemFeatures.CaseSensitiveSearch |
                       FileSystemFeatures.PersistentAcls | FileSystemFeatures.SupportsRemoteStorage |
                       FileSystemFeatures.UnicodeOnDisk;
            return NtStatus.Success;
        }

        public NtStatus GetFileSecurity(string fileName, out FileSystemSecurity security,
            AccessControlSections sections,
            IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus SetFileSecurity(string fileName, FileSystemSecurity security, AccessControlSections sections,
            IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }


        public NtStatus FindStreams(string fileName, out IList<FileInformation> streams, IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        private bool IsNormalFile(string fileName, IDokanFileInfo info, out BlobFile cachedFile)
        {
            cachedFile = _cached.GetOrAdd(fileName, fn => new BlobFile(_containerClient, fn, info, _cacheDirectory));
            return cachedFile.IsNormalFile;
        }
    }
}