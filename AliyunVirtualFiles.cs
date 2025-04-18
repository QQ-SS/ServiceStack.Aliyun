using Aliyun.OSS;
using Aliyun.OSS.Common;
using ServiceStack.Aliyun;
using ServiceStack.Text;
using ServiceStack.VirtualPath;
using System.Runtime.CompilerServices;
using System.Text;

namespace ServiceStack.IO;

public partial class AliyunVirtualFiles : AbstractVirtualPathProviderBase, IVirtualFiles
{
    public const int MultiObjectLimit = 1000;

    public OssClient ossClient { get; private set; }
    public string BucketName { get; private set; }
    protected readonly AliyunVirtualDirectory rootDirectory;

    public AliyunVirtualFiles(
        string endpoint,
        string accessKeyId,
        string accessSecret,
        string bucketName
    )
    {
        this.ossClient = new OssClient(endpoint, accessKeyId, accessSecret);
        this.BucketName = bucketName;
        this.rootDirectory = new AliyunVirtualDirectory(this, null, null);
    }

    public const char DirSep = '/';

    public override IVirtualDirectory RootDirectory => rootDirectory;

    public override string VirtualPathSeparator => "/";

    public override string RealPathSeparator => "/";

    protected override void Initialize() { }

    public override IVirtualFile GetFile(string virtualPath)
    {
        if (string.IsNullOrEmpty(virtualPath))
            return null;

        var filePath = SanitizePath(virtualPath);
        try
        {
            var response = ossClient.GetObject(
                new GetObjectRequest(bucketName: BucketName, key: filePath)
            );

            var dirPath = GetDirPath(filePath);
            var dir = dirPath == null ? RootDirectory : GetParentDirectory(dirPath);
            return new AliyunVirtualFile(this, dir).Init(response);
        }
        catch (OssException ex)
        {
            if (ex.ErrorCode == OssErrorCode.NoSuchKey)
                return null;

            throw;
        }
    }

    public virtual async Task<IVirtualFile> GetFileAsync(string virtualPath)
    {
        if (string.IsNullOrEmpty(virtualPath))
            return null;

        var filePath = SanitizePath(virtualPath);
        try
        {
            var response = await Task.Run(
                    () =>
                        ossClient.GetObject(
                            new GetObjectRequest(bucketName: BucketName, key: filePath)
                        )
                )
                .ConfigAwait();

            var dirPath = GetDirPath(filePath);
            var dir = dirPath == null ? RootDirectory : GetParentDirectory(dirPath);
            return new AliyunVirtualFile(this, dir).Init(response);
        }
        catch (OssException ex)
        {
            if (ex.ErrorCode == OssErrorCode.NoSuchKey)
                return null;

            throw;
        }
    }

    public virtual AliyunVirtualDirectory GetParentDirectory(string dirPath)
    {
        if (string.IsNullOrEmpty(dirPath))
            return null;

        var parentDirPath = GetDirPath(dirPath.TrimEnd(DirSep));
        var parentDir =
            parentDirPath != null
                ? GetParentDirectory(parentDirPath)
                : (AliyunVirtualDirectory)RootDirectory;
        return new AliyunVirtualDirectory(this, dirPath, parentDir);
    }

    public override IVirtualDirectory GetDirectory(string virtualPath)
    {
        if (virtualPath == null)
            return null;

        var dirPath = SanitizePath(virtualPath);
        if (string.IsNullOrEmpty(dirPath))
            return RootDirectory;

        var seekPath = dirPath[dirPath.Length - 1] != DirSep ? dirPath + DirSep : dirPath;

        var response = ossClient.ListObjects(
            new ListObjectsRequest(BucketName) { Prefix = seekPath, MaxKeys = 1, }
        );

        if (response.ObjectSummaries.Count() == 0)
            return null;

        return new AliyunVirtualDirectory(this, dirPath, GetParentDirectory(dirPath));
    }

    public override bool DirectoryExists(string virtualPath)
    {
        return GetDirectory(virtualPath) != null;
    }

    public override bool FileExists(string virtualPath)
    {
        return GetFile(virtualPath) != null;
    }

    public virtual void WriteFile(string filePath, string contents)
    {
        var encoding = Encoding.UTF8;
        var stream = new MemoryStream(encoding.GetByteCount(contents));
        using var writer = new StreamWriter(stream, encoding, -1, true);
        writer.Write(contents);
        writer.Flush();
        stream.Position = 0;

        ossClient.PutObject(
            new PutObjectRequest(
                bucketName: BucketName,
                key: SanitizePath(filePath),
                content: stream
            )
        );
    }

    public virtual void WriteFile(string filePath, Stream stream)
    {
        ossClient.PutObject(
            new PutObjectRequest(
                bucketName: BucketName,
                key: SanitizePath(filePath),
                content: stream
            )
        );
    }

    public override async Task WriteFileAsync(
        string filePath,
        object contents,
        CancellationToken token = default
    )
    {
        await Task.Run(
            () =>
            {
                WriteFile(filePath, contents);
            },
            token
        );

        // // need to buffer otherwise hangs when trying to send an uploaded file stream (depends on provider)
        // var buffer = contents is not MemoryStream;
        // var fileContents = await FileContents.GetAsync(contents, buffer);
        // if (fileContents?.Stream != null)
        // {
        //     await ossClient.PutObjectAsync(new PutObjectRequest
        //     {
        //         Key = SanitizePath(filePath),
        //         BucketName = BucketName,
        //         InputStream = fileContents.Stream,
        //     }, token).ConfigAwait();
        // }
        // else if (fileContents?.Text != null)
        // {
        //     await ossClient.PutObjectAsync(new PutObjectRequest
        //     {
        //         Key = SanitizePath(filePath),
        //         BucketName = BucketName,
        //         ContentBody = fileContents.Text,
        //     }, token).ConfigAwait();
        // }
        // else throw new NotSupportedException($"Unknown File Content Type: {contents.GetType().Name}");

        // if (buffer && fileContents.Stream != null) // Dispose MemoryStream buffer created by FileContents
        //     using (fileContents.Stream) { }
    }

    public virtual void WriteFiles(
        IEnumerable<IVirtualFile> files,
        Func<IVirtualFile, string> toPath = null
    )
    {
        this.CopyFrom(files, toPath);
    }

    public virtual void AppendFile(string filePath, string textContents)
    {
        throw new NotImplementedException("Aliyun doesn't support appending to files");
    }

    public virtual void AppendFile(string filePath, Stream stream)
    {
        throw new NotImplementedException("Aliyun doesn't support appending to files");
    }

    public virtual void DeleteFile(string filePath)
    {
        ossClient.DeleteObject(
            new DeleteObjectRequest(bucketName: BucketName, key: SanitizePath(filePath))
        );
    }

    public virtual async Task DeleteFileAsync(string filePath)
    {
        await Task.Run(() =>
        {
            DeleteFile(filePath);
        });
    }

    public virtual void DeleteFiles(IEnumerable<string> filePaths)
    {
        var batches = filePaths.BatchesOf(MultiObjectLimit);

        foreach (var batch in batches)
        {
            var request = new DeleteObjectsRequest(
                bucketName: BucketName,
                keys: batch.Select(SanitizePath).ToArray()
            );

            ossClient.DeleteObjects(request);
        }
    }

    public virtual async Task DeleteFilesAsync(IEnumerable<string> filePaths)
    {
        var batches = filePaths.BatchesOf(MultiObjectLimit);

        foreach (var batch in batches)
        {
            await Task.Run(() =>
                {
                    var request = new DeleteObjectsRequest(
                        bucketName: BucketName,
                        keys: batch.Select(SanitizePath).ToArray()
                    );
                    ossClient.DeleteObjects(request);
                })
                .ConfigAwait();
        }
    }

    public virtual void DeleteFolder(string dirPath)
    {
        dirPath = SanitizePath(dirPath);
        var nestedFiles = EnumerateFiles(dirPath).Map(x => x.FilePath);
        DeleteFiles(nestedFiles);
    }

#if NET6_0_OR_GREATER
    public virtual async Task DeleteFolderAsync(string dirPath, CancellationToken token = default)
    {
        dirPath = SanitizePath(dirPath);
        var nestedFiles = await EnumerateFilesAsync(dirPath, token)
            .Select(x => x.FilePath)
            .ToListAsync(token);
        DeleteFiles(nestedFiles);
    }
#endif

    public virtual IEnumerable<AliyunVirtualFile> EnumerateFiles(string prefix = null)
    {
        var response = ossClient.ListObjects(
            new ListObjectsRequest(bucketName: BucketName) { Prefix = prefix, }
        );

        foreach (var file in response.ObjectSummaries)
        {
            var filePath = SanitizePath(file.Key);

            var dirPath = GetDirPath(filePath);
            yield return new AliyunVirtualFile(
                this,
                new AliyunVirtualDirectory(this, dirPath, GetParentDirectory(dirPath))
            )
            {
                FilePath = filePath,
                ContentLength = file.Size,
                FileLastModified = file.LastModified,
                Etag = file.ETag,
            };
        }
    }

#if NET6_0_OR_GREATER
    public virtual async IAsyncEnumerable<AliyunVirtualFile> EnumerateFilesAsync(
        string prefix = null,
        [EnumeratorCancellation] CancellationToken token = default
    )
    {
        ObjectListing response = null;

        while (true)
        {
            response = await Task.Run(
                () =>
                    ossClient.ListObjects(
                        new ListObjectsRequest(bucketName: BucketName)
                        {
                            Prefix = prefix,
                            Marker = response?.NextMarker
                        }
                    ),
                token
            );

            foreach (var file in response.ObjectSummaries)
            {
                var filePath = SanitizePath(file.Key);

                var dirPath = GetDirPath(filePath);
                yield return new AliyunVirtualFile(
                    this,
                    new AliyunVirtualDirectory(this, dirPath, GetParentDirectory(dirPath))
                )
                {
                    FilePath = filePath,
                    ContentLength = file.Size,
                    FileLastModified = file.LastModified,
                    Etag = file.ETag,
                };
            }

            if (!response.IsTruncated)
                yield break;
        }
    }
#endif

    public override IEnumerable<IVirtualFile> GetAllFiles()
    {
        return EnumerateFiles();
    }

#if NET6_0_OR_GREATER
    public IAsyncEnumerable<AliyunVirtualFile> GetAllFilesAsync(
        CancellationToken token = default
    ) => EnumerateFilesAsync(token: token);
#endif

    public virtual IEnumerable<AliyunVirtualDirectory> GetImmediateDirectories(string fromDirPath)
    {
        var files = EnumerateFiles(fromDirPath);
        var dirPaths = files
            .Map(x => x.DirPath)
            .Distinct()
            .Map(x => GetImmediateSubDirPath(fromDirPath, x))
            .Where(x => x != null)
            .Distinct();

        return dirPaths.Select(x => new AliyunVirtualDirectory(this, x, GetParentDirectory(x)));
    }

#if NET6_0_OR_GREATER
    public virtual IAsyncEnumerable<AliyunVirtualDirectory> GetImmediateDirectoriesAsync(
        string fromDirPath,
        CancellationToken token = default
    )
    {
        var dirPaths = EnumerateFilesAsync(fromDirPath, token)
            .Select(x => x.DirPath)
            .Distinct()
            .Select(x => GetImmediateSubDirPath(fromDirPath, x))
            .Where(x => x != null)
            .Distinct();

        var parentDir = GetParentDirectory(fromDirPath);
        return dirPaths.Select(x => new AliyunVirtualDirectory(this, x, parentDir));
    }
#endif

    public virtual IEnumerable<AliyunVirtualFile> GetImmediateFiles(string fromDirPath)
    {
        return EnumerateFiles(fromDirPath).Where(x => x.DirPath == fromDirPath);
    }

#if NET6_0_OR_GREATER
    public virtual IAsyncEnumerable<AliyunVirtualFile> GetImmediateFilesAsync(
        string fromDirPath,
        CancellationToken token = default
    )
    {
        return EnumerateFilesAsync(fromDirPath, token).Where(x => x.DirPath == fromDirPath);
    }
#endif

    public virtual string GetDirPath(string filePath)
    {
        if (string.IsNullOrEmpty(filePath))
            return null;

        var lastDirPos = filePath.LastIndexOf(DirSep);
        return lastDirPos >= 0 ? filePath.Substring(0, lastDirPos) : null;
    }

    public virtual string GetImmediateSubDirPath(string fromDirPath, string subDirPath)
    {
        if (string.IsNullOrEmpty(subDirPath))
            return null;

        if (fromDirPath == null)
        {
            return subDirPath.CountOccurrencesOf(DirSep) == 0
                ? subDirPath
                : subDirPath.LeftPart(DirSep);
        }

        if (!subDirPath.StartsWith(fromDirPath))
            return null;

        return fromDirPath.CountOccurrencesOf(DirSep) == subDirPath.CountOccurrencesOf(DirSep) - 1
            ? subDirPath
            : null;
    }

    public override string SanitizePath(string filePath)
    {
        var sanitizedPath = string.IsNullOrEmpty(filePath)
            ? null
            : (filePath[0] == DirSep ? filePath.Substring(1) : filePath);

        return sanitizedPath?.Replace('\\', DirSep);
    }

    public static string GetFileName(string filePath)
    {
        return filePath.SplitOnLast(DirSep).Last();
    }
}

// public partial class AliyunVirtualFiles : IOss
// {
//     public virtual void ClearBucket()
//     {
//         var allFilePaths = EnumerateFiles()
//             .Map(x => x.FilePath);

//         DeleteFiles(allFilePaths);
//     }

// #if NET6_0_OR_GREATER
//     public virtual async Task ClearBucketAsync(CancellationToken token = default)
//     {
//         throw new NotImplementedException();
//         // var allFilePaths = await EnumerateFilesAsync(token: token)
//         //     .Select(x => x.FilePath).ToListAsync(token);

//         // DeleteFiles(allFilePaths);
//     }
// #endif

// }
