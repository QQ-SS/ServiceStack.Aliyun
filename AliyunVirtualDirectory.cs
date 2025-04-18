using Aliyun.OSS;
using Aliyun.OSS.Common;
using ServiceStack.IO;
using ServiceStack.VirtualPath;
using System.Runtime.CompilerServices;
using System.Text;

namespace ServiceStack.Aliyun;

public class AliyunVirtualDirectory : AbstractVirtualDirectoryBase
{
    internal AliyunVirtualFiles PathProvider { get; private set; }

    public AliyunVirtualDirectory(
        AliyunVirtualFiles pathProvider,
        string dirPath,
        AliyunVirtualDirectory parentDir
    )
        : base(pathProvider, parentDir)
    {
        this.PathProvider = pathProvider;
        this.DirPath = dirPath;
    }

    static readonly char DirSep = '/';

    public DateTime DirLastModified { get; set; }

    public override DateTime LastModified => DirLastModified;

    public override IEnumerable<IVirtualFile> Files => PathProvider.GetImmediateFiles(DirPath);

    public override IEnumerable<IVirtualDirectory> Directories =>
        PathProvider.GetImmediateDirectories(DirPath);

    public OssClient Client => PathProvider.ossClient;

    public string BucketName => PathProvider.BucketName;

    public string DirPath { get; set; }

    public override string VirtualPath => DirPath;

    public override string Name => DirPath?.SplitOnLast(MemoryVirtualFiles.DirSep).Last();

    public override IVirtualFile GetFile(string virtualPath)
    {
        try
        {
            var response = Client.GetObject(
                new GetObjectRequest(bucketName: BucketName, key: DirPath.CombineWith(virtualPath))
            );

            return new AliyunVirtualFile(PathProvider, this).Init(response);
        }
        catch (OssException ex)
        {
            if (ex.ErrorCode == OssErrorCode.NoSuchKey)
                return null;

            throw;
        }
    }

    public override IEnumerator<IVirtualNode> GetEnumerator()
    {
        throw new NotImplementedException();
    }

    protected override IVirtualFile GetFileFromBackingDirectoryOrDefault(string fileName)
    {
        return GetFile(fileName);
    }

    protected override IEnumerable<IVirtualFile> GetMatchingFilesInDir(string globPattern)
    {
        var matchingFilesInBackingDir = EnumerateFiles(globPattern);
        return matchingFilesInBackingDir;
    }

#if NET6_0_OR_GREATER
    protected virtual IAsyncEnumerable<AliyunVirtualFile> GetMatchingFilesInDirAsync(
        string globPattern,
        CancellationToken token = default
    )
    {
        return EnumerateFilesAsync(globPattern, token);
    }
#endif

    public IEnumerable<AliyunVirtualFile> EnumerateFiles(string pattern)
    {
        foreach (
            var file in PathProvider.GetImmediateFiles(DirPath).Where(f => f.Name.Glob(pattern))
        )
        {
            yield return file;
        }
    }

#if NET6_0_OR_GREATER
    public async IAsyncEnumerable<AliyunVirtualFile> EnumerateFilesAsync(
        string pattern,
        [EnumeratorCancellation] CancellationToken token = default
    )
    {
        foreach (
            var file in await PathProvider
                .GetImmediateFilesAsync(DirPath, token)
                .Where(f => f.Name.Glob(pattern))
                .ToListAsync(token)
        )
        {
            yield return file;
        }
    }
#endif

    protected override IVirtualDirectory GetDirectoryFromBackingDirectoryOrDefault(
        string directoryName
    )
    {
        return new AliyunVirtualDirectory(
            PathProvider,
            PathProvider.SanitizePath(DirPath.CombineWith(directoryName)),
            this
        );
    }

    public void AddFile(string filePath, string contents)
    {
        var encoding = Encoding.UTF8;
        var stream = new MemoryStream(encoding.GetByteCount(contents));
        using var writer = new StreamWriter(stream, encoding, -1, true);
        writer.Write(contents);
        writer.Flush();
        stream.Position = 0;

        Client.PutObject(
            new PutObjectRequest(
                bucketName: PathProvider.BucketName,
                key: StripDirSeparatorPrefix(filePath),
                content: stream
            )
        );
    }

    public void AddFile(string filePath, Stream stream)
    {
        Client.PutObject(
            new PutObjectRequest(
                bucketName: PathProvider.BucketName,
                key: StripDirSeparatorPrefix(filePath),
                content: stream
            )
        );
    }

    private static string StripDirSeparatorPrefix(string filePath)
    {
        return string.IsNullOrEmpty(filePath)
            ? filePath
            : (filePath[0] == DirSep ? filePath.Substring(1) : filePath);
    }

    public override IEnumerable<IVirtualFile> GetAllMatchingFiles(
        string globPattern,
        int maxDepth = int.MaxValue
    )
    {
        if (IsRoot)
        {
            return PathProvider
                .EnumerateFiles()
                .Where(x =>
                    (x.DirPath == null || x.DirPath.CountOccurrencesOf('/') < maxDepth - 1)
                    && x.Name.Glob(globPattern)
                );
        }

        return PathProvider
            .EnumerateFiles(DirPath)
            .Where(x =>
                x.DirPath != null
                && x.DirPath.CountOccurrencesOf('/') < maxDepth - 1
                && x.DirPath.StartsWith(DirPath)
                && x.Name.Glob(globPattern)
            );
    }

#if NET6_0_OR_GREATER
    public virtual async Task<List<AliyunVirtualFile>> GetAllMatchingFilesAsync(
        string globPattern,
        int maxDepth = int.MaxValue,
        CancellationToken token = default
    )
    {
        if (IsRoot)
        {
            return await PathProvider
                .EnumerateFilesAsync(token: token)
                .Where(x =>
                    (x.DirPath == null || x.DirPath.CountOccurrencesOf('/') < maxDepth - 1)
                    && x.Name.Glob(globPattern)
                )
                .ToListAsync(token);
        }

        return await PathProvider
            .EnumerateFilesAsync(DirPath, token)
            .Where(x =>
                x.DirPath != null
                && x.DirPath.CountOccurrencesOf('/') < maxDepth - 1
                && x.DirPath.StartsWith(DirPath)
                && x.Name.Glob(globPattern)
            )
            .ToListAsync(token);
    }
#endif
}
