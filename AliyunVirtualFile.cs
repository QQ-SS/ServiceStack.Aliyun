using Aliyun.OSS;
using Aliyun.OSS.Common;
using ServiceStack.IO;
using ServiceStack.Text;
using ServiceStack.VirtualPath;

namespace ServiceStack.Aliyun;

public class AliyunVirtualFile : AbstractVirtualFileBase
{
    private AliyunVirtualFiles PathProvider { get; set; }

    public OssClient Client => PathProvider.ossClient;

    public string BucketName => PathProvider.BucketName;

    public AliyunVirtualFile(AliyunVirtualFiles pathProvider, IVirtualDirectory directory)
        : base(pathProvider, directory)
    {
        this.PathProvider = pathProvider;
    }

    public string DirPath => base.Directory.VirtualPath;

    public string FilePath { get; set; }

    public string ContentType { get; set; }

    public override string Name => AliyunVirtualFiles.GetFileName(FilePath);

    public override string VirtualPath => FilePath;

    public DateTime FileLastModified { get; set; }

    public override DateTime LastModified => FileLastModified;

    public override long Length => ContentLength;

    public long ContentLength { get; set; }

    public string Etag { get; set; }

    public Stream Stream { get; set; }

    public AliyunVirtualFile Init(OssObject response)
    {
        FilePath = response.Key;
        ContentType = response.Metadata.ContentType;
        FileLastModified = response.Metadata.LastModified;
        ContentLength = response.Metadata.ContentLength;
        Etag = response.Metadata.ETag;
        Stream = response.ResponseStream;
        return this;
    }

    public override Stream OpenRead()
    {
        if (Stream is not { CanRead: true })
        {
            var response = Client.GetObject(
                new GetObjectRequest(bucketName: BucketName, key: FilePath)
            );
            Init(response);
        }

        return Stream;
    }

    public override void Refresh()
    {
        try
        {
            var o = new GetObjectRequest(bucketName: BucketName, key: FilePath);
            o.NonmatchingETagConstraints.Add(Etag);
            var response = Client.GetObject(o);
            Init(response);
        }
        catch (OssException ex)
        {
            if (ex.ErrorCode != OssErrorCode.NotModified)
                throw;
        }
    }

    public override async Task WritePartialToAsync(
        Stream toStream,
        long start,
        long end,
        CancellationToken token = default
    )
    {
        var o = new GetObjectRequest(bucketName: BucketName, key: FilePath);
        o.SetRange(start, end);
        var response = await Task.Run(() => Client.GetObject(o), token);
        Init(response);

        await response.ResponseStream.WriteToAsync(toStream, token).ConfigAwait();
    }
}
