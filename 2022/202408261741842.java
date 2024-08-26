package com.agile.utils;

import com.agile.base.constant.CosConstant;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.StringUtils;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Properties;

/**
 * 天宫云 非结构化
 *
 * @author liuxy
 */
@Component
public class CosUtilNew {

    private static final Logger log = Logger.getLogger("CosUtillogger");

    private final static Properties config = new Properties();
    private final static String CONFIG_PATH = "config.properties";

    static {
        try {
            config.load(Resources.getResourceAsReader(CONFIG_PATH));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    // 可以放到（T_PARAM_CONFIG）参数配置表中
    private static final String accessKey = config.getProperty("accessKeyNew");
    private static final String secretKey = config.getProperty("secretKeyNew");
    // 访问地址 https://<endpoint>/<accountID>:<bucketName>
    // 指到 endpoint这一层
    // 测试先用 http的 https的证书 需要写到加密串里面
    private static final String serviceEndpoint = config.getProperty("serviceEndpointNew");
    private static final String region = config.getProperty("regionNew");
    private static final String baseBucketName = config.getProperty("baseBucketName");
    private static final String hisBucketName = config.getProperty("hisBucketName");
    private static final String baseBucketNamePrefix = config.getProperty("baseBucketNamePrefix", "bhps");
    /**
     * cos存储桶（非面对面：桶名+年月 例如：fbhps202201）
     */
    private static final String FBHPS_BUCKET_NAME = config.getProperty("fbhpsBucketName");
    //	private static String baseBucketNameSuffix = config.getProperty("baseBucketNameSuffix", "fbhps");
    /**
     * cos存储桶（纸质工单：桶名+年月 例如：pbhps202201）
     */
    private static final String PBHPS_BUCKET_NAME = config.getProperty("pbhpsBucketName");
    private static AmazonS3 conn = null;



    static {
        if (null == conn) {
            conn = build();
        }
    }

    public static AmazonS3 getInstanceAmazonS3() {
        if (null == conn) {
            conn = build();
        }
        return conn;
    }

    private static AmazonS3 build() {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(credentials);

        // 客户端配置
        ClientConfiguration config = new ClientConfiguration();
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(
                serviceEndpoint, region);

        AmazonS3 conn = AmazonS3ClientBuilder.standard().withEndpointConfiguration(endpointConfiguration)
                .withCredentials(awsStaticCredentialsProvider)
                .withClientConfiguration(config.withProtocol(Protocol.HTTP).withSignerOverride("S3SignerType"))
                .withEndpointConfiguration(endpointConfiguration).build();
        System.out.println("CosUtil AmazonS3  调用初始化执行成功。。。。。。。。。");
        return conn;
    }

    /**
     * 列出所有桶列表
     *
     * @return
     */
    public static List<Bucket> getListBuckets() {
        AmazonS3 conn = CosUtilNew.getInstanceAmazonS3();
        return conn.listBuckets();
    }

    /**
     * 创建桶 一个账号 可以创建1000个 每个桶的存储目录无限制
     *
     * @param bucketName
     */
    public static void createBucket(String bucketName) {
        AmazonS3 conn = CosUtilNew.getInstanceAmazonS3();
        Bucket bucket = conn.createBucket(bucketName);
        System.out.println(bucket.getName() + " = Create Success !!!");
    }

    public static void uploadObject(String key, File file) {
        uploadObject("", key, file);
    }

    /**
     * 再某个桶中创建对象 上传对象时，key如果保持了目录结构，那么在上传到对象存储上时，就会自动创建目录 不需要单独创建目录对象key
     *
     * @param bucketName
     * @param key        datafile/2020/11_120/20200304/202004161757307601199XX12345567.pdf
     * @param file       /data/xxxx.pdf
     */
    public static void uploadObject(String opTime, String key, File file) {
        log.info("[CosUtilNew][uploadObject] -> Cos access to CosUtilNew");
        long startTime = System.currentTimeMillis();
        // ByteArrayInputStream input = new ByteArrayInputStream("Hello
        // World!".getBytes());
        // PutObjectResult result = conn.putObject(bucketName, "hello.txt", input,new
        // ObjectMetadata());
        // 新文件名称的
        String bucketName = convertBucketName(opTime);
        if (FileUtil.optionSystemWin()) {
            key = key.replace("\\", "/");
        }
        PutObjectResult result = conn.putObject(bucketName, convertCosKey(key), file);
        System.out.println("uploadObject key result = " + result.toString());
        long endTime = System.currentTimeMillis();
        log.info("[uploadObject] -> process time " + (endTime - startTime));
    }

    public static void uploadObjectByBucketName(String bucketName, String key, File file) {
        log.info("[CosUtilNew][uploadObjectByBucketName] -> Cos access to CosUtilNew");
        long startTime = System.currentTimeMillis();
        if (FileUtil.optionSystemWin()) {
            key = key.replace("\\", "/");
        }
        PutObjectResult result = conn.putObject(bucketName, convertCosKey(key), file);
        System.out.println("uploadObject key result = " + result.toString());
        long endTime = System.currentTimeMillis();
        log.info("[uploadObject] -> process time " + (endTime - startTime));
    }

    /**
     * 下载文件到本地-非面对面
     *
     * @param bucketName
     * @param key
     * @param localPath
     */
    public static void downloadFile(String key, String localPath) {
        downloadFile(null, convertCosKey(key), localPath);
    }

    public static boolean downloadFile(String opTime, String key, String localPath) {
        log.info("[CosUtilNew][downloadFile] -> Cos access to CosUtilNew");
        long startTime = System.currentTimeMillis();
        // 默认下载成功
        boolean flag = true;
        String bucketName = convertBucketName(opTime);
        File file = new File(localPath);
        if (!file.exists()) {
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
        }
        key = convertCosKey(key);
        if (doesObjectExist(bucketName, key)) {
            try {
                conn.getObject(new GetObjectRequest(bucketName, key), new File(localPath));
            } catch (Exception e) {
                log.info("[downloadFile] -> Cos Exception res=" + e);
            }
        } else {
            log.info("[downloadFile] -> Cos key not found");
            flag = false;
        }

        long endTime = System.currentTimeMillis();
        log.info("[downloadFile] -> process time " + (endTime - startTime));

        return flag;
    }

    /**
     * cos下载文件-完整桶名
     *
     * @param bucketName 桶名
     * @param key        cos云存储key
     * @param localPath  本地路径
     * @return boolean
     * @author yangzhenyu
     * @date 2022/6/9 15:22
     */
    public static boolean downloadFileByBucketName(String bucketName, String key, String localPath) {
        log.info("[CosUtilNew][downloadFileByBucketName] -> Cos access to CosUtilNew");
        long startTime = System.currentTimeMillis();
        // 默认下载成功
        boolean flag = true;
        File file = new File(localPath);
        if (!file.exists()) {
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
        }
        key = convertCosKey(key);
        if (doesObjectExist(bucketName, key)) {
            try {
                conn.getObject(new GetObjectRequest(bucketName, key), new File(localPath));
            } catch (Exception e) {
                log.info("[downloadFileByBucketName] -> Cos Exception res=" + e);
            }
        } else {
            log.info("[downloadFileByBucketName] -> Cos key not found");
            flag = false;
        }

        long endTime = System.currentTimeMillis();
        log.info("[downloadFileByBucketName] -> process time " + (endTime - startTime));

        return flag;
    }

    /**
     * 下载文件到本地-面对面
     *
     * @param bucketName
     * @param key
     * @param localPath
     */
    public static void downloadFileHis(String key, String localPath) {
        downloadFileHis(null, convertCosKey(key), localPath);
    }

    public static boolean downloadFileHis(String opTime, String key, String localPath) {
        log.info("[CosUtilNew][downloadFileHis] -> Cos access to CosUtilNew");
        long startTime = System.currentTimeMillis();
        // 默认下载成功
        boolean flag = true;
        String bucketName = convertBucketNameHis(opTime);
        File file = new File(localPath);
        if (!file.exists()) {
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
        }
        key = convertCosKey(key);
        if (doesObjectExist(bucketName, key)) {
            try {
                conn.getObject(new GetObjectRequest(bucketName, key), new File(localPath));
            } catch (Exception e) {
                log.info("[downloadFile] -> Cos Exception res=" + e);
            }
        } else {
            log.info("[downloadFile] -> Cos key not found");
            flag = false;
        }

        long endTime = System.currentTimeMillis();
        log.info("[downloadFile] -> process time " + (endTime - startTime));

        return flag;
    }

    //非面对面
    public static InputStream getS3ObjectInputStream(String key) {
        return getS3ObjectInputStream(null, key);
    }

    public static InputStream getS3ObjectInputStream(String opTime, String key) {
        log.info("[CosUtilNew][getS3ObjectInputStream] -> Cos access to CosUtilNew");
        long startTime = System.currentTimeMillis();
        String bucketName = convertBucketName(opTime);
        InputStream ins = null;
        try {
            S3Object object = conn.getObject(new GetObjectRequest(bucketName, convertCosKey(key)));
            ins = object.getObjectContent();
//			object.getTaggingCount();
//			object.getObjectMetadata();
//			object.getTaggingCount();
        } catch (Exception e) {
            log.info("[outputFile] ->" + e);
        }
        long endTime = System.currentTimeMillis();
        log.info("[getS3ObjectInputStream] -> process time " + (endTime - startTime));
        return ins;
    }

    //面对面
    public static InputStream getS3ObjectInputStreamHis(String key) {
        return getS3ObjectInputStream(null, key);
    }

    public static InputStream getS3ObjectInputStreamHis(String opTime, String key) {
        log.info("[CosUtilNew][getS3ObjectInputStreamHis] -> Cos access to CosUtilNew");
        long startTime = System.currentTimeMillis();
        String bucketName = convertBucketNameHis(opTime);
        InputStream ins = null;
        try {
            S3Object object = conn.getObject(new GetObjectRequest(bucketName, convertCosKey(key)));
            ins = object.getObjectContent();
//			object.getTaggingCount();
//			object.getObjectMetadata();
//			object.getTaggingCount();
        } catch (Exception e) {
            log.info("[outputFile] ->" + e);
        }
        long endTime = System.currentTimeMillis();
        log.info("[getS3ObjectInputStream] -> process time " + (endTime - startTime));
        return ins;
    }

    public static void deleteObject(String key) {
        deleteObject("", key);
    }

    public static void deleteObjectByBucketName(String bucketName, String key) {
        log.info("[CosUtilNew][deleteObjectByBucketName] -> Cos access to CosUtilNew");
        long startTime = System.currentTimeMillis();
        if (conn.doesObjectExist(bucketName, convertCosKey(key))) {
            try {
                conn.deleteObject(bucketName, convertCosKey(key));
            } catch (Exception e) {
                System.out.println("[deleteObject] [key=" + key + "][Exception->" + e);
            }
        }
        long endTime = System.currentTimeMillis();
        log.info("[deleteObject] -> process time " + (endTime - startTime));
    }

    public static void deleteObject(String opTime, String key) {
        log.info("[CosUtilNew][deleteObject] -> Cos access to CosUtilNew");
        long startTime = System.currentTimeMillis();
        String bucketName = convertBucketName(opTime);
        if (conn.doesObjectExist(bucketName, convertCosKey(key))) {
            try {
                conn.deleteObject(bucketName, convertCosKey(key));
            } catch (Exception e) {
                System.out.println("[deleteObject] [key=" + key + "][Exception->" + e);
            }
        }
        long endTime = System.currentTimeMillis();
        log.info("[deleteObject] -> process time " + (endTime - startTime));
    }

    /**
     * 判断 Cos key 文件是否存在
     *
     * @param key
     * @return
     */
    public static boolean doesObjectExist(String key) {
        return doesObjectExist(null, key);
    }

    public static boolean doesObjectExist(String bucketName, String key) {
        return conn.doesObjectExist(bucketName, key);
    }

    /**
     * 验证cos文件是否存在
     *
     * @param opTime 受理时间
     * @param key    cos存储key
     * @param flag   标识: 非面对面 true; 面对面 false;
     * @return boolean
     * @author yangzhenyu
     * @date 2022/1/14 11:27
     */
    public static boolean doesObjectExistByFlag(String opTime, String key, boolean flag) {
        log.info("[CosUtilNew][doesObjectExistByFlag] -> Cos access to CosUtilNew");
        String bucketName = null;
        if (flag) {
            bucketName = convertBucketName(opTime);
        } else {
            bucketName = convertBucketNameHis(opTime);
        }
        boolean fileExist = conn.doesObjectExist(bucketName, convertCosKey(key));
        log.info("[doesObjectExistByFlag] [key=" + key + "] fileExist is " + fileExist);
        return fileExist;
    }

    /**
     * 检查本地文件是否存在，不存在则下载-非面对面
     *
     * @param key
     * @return
     */

    public static String checkLocalFile(String key) {
        return checkLocalFile(null, key);
    }

    /**
     * 检查本地文件是否存在，不存在则下载-非面对面
     *
     * @param bucketName
     * @param key
     * @return
     */
    public static String checkLocalFile(String bucketName, String key) {
        File file = new File(key);
        if (!file.exists()) {
            downloadFile(bucketName, key, key);
        }
        return key;

    }

    /**
     * 检查本地文件是否存在，不存在则下载-面对面
     *
     * @param key
     * @return
     */
    public static String checkLocalFileHis(String key) {
        return checkLocalFile(null, key);
    }

    /**
     * 检查本地文件是否存在，不存在则下载-面对面
     *
     * @param bucketName
     * @param key
     * @return
     */
    public static String checkLocalFileHis(String bucketName, String key) {
        File file = new File(key);
        if (!file.exists()) {
            downloadFileHis(bucketName, key, key);
        }
        return key;
    }

    private static String convertCosKey(String key) {
        key = key.replace("/data/wwwroot/wzh.10010.com", "data/wwwroot/wzh.10010.com");
        log.info("[CosUtil][convertCosKey] => " + key);
        return key;
    }

    // 非面对面
    private static String convertBucketName(String bucketName) {
        if (null == bucketName || "".equals(bucketName)) {
            bucketName = baseBucketName;
        } else {
            bucketName = FBHPS_BUCKET_NAME + bucketName.replace("-", "").substring(0, 6);
        }
        log.info("[CosUtil][convertBucketName] => " + bucketName);
        return bucketName;
    }

    // 面对面
    private static String convertBucketNameHis(String bucketName) {
        if (null == bucketName || "".equals(bucketName)) {
            bucketName = hisBucketName;
        } else {
            bucketName = hisBucketName + bucketName.replace("-", "").substring(0, 6);
        }
        log.info("[CosUtil][convertBucketName] => " + bucketName);
        return bucketName;
    }

    /**
     * 获取cos桶名称
     *
     * @param opTime      受理时间
     * @param bucketLevel 存储桶等级：0 线上-受理单集中桶；1 线上-受理单分库桶； 2.线下-受理单分库桶  3.线下-纸质工单分库桶
     * @return java.lang.String
     * @author yangzhenyu
     * @date 2022/6/9 16:49
     */
    private static String convertBucketName(String opTime, Integer bucketLevel) {
        String bucketName = null;
        if (bucketLevel == CosConstant.ONLINE_ONE_CASE_BUCKET_LEVEL) {
            bucketName = baseBucketName;
        } else if (bucketLevel == CosConstant.ONLINE_SEPARATED_CASE_BUCKET_LEVEL) {
            bucketName = FBHPS_BUCKET_NAME;
        } else if (bucketLevel == CosConstant.OFFLINE_SEPARATED_CASE_BUCKET_LEVEL) {
            bucketName = hisBucketName;
        } else if (bucketLevel == CosConstant.OFFLINE_SEPARATED_PAPER_CASE_BUCKET_LEVEL) {
            bucketName = PBHPS_BUCKET_NAME;
        }
        if (org.apache.commons.lang.StringUtils.isNotEmpty(opTime)) {
            bucketName += opTime.replace("-", "").substring(0, 6);
        }
        log.info("[CosUtil][convertBucketName] => " + bucketName);
        return bucketName;
    }

    // 生成对象的下载连接（带签名）
    /*
     * http://test-bhps-002.cos.lf-tst.cos.test.tg.unicom.local/datafile/2020/11_110
     * /20200301/202004161757307601199XX12345567.pdf?
     * X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20200507T104301Z&X-Amz-
     * SignedHeaders=host&X-Amz-Expires=899&X-Amz-Credential=2BW5EK79HN38D1QG2L17%
     * 2F20200507%2Flf-tst%2Fs3%2Faws4_request&X-Amz-Signature=
     * 874e331aa96a83278dcb37be749011f8fd4dcd8bd702535ea1961c7e468e8e03
     */

    /**
     * cos获取本地文件路径：可根据受理时间、桶等级自行桶名称组装并获取本地文件路径
     *
     * @param opTime      受理时间
     * @param key         cos存储key
     * @param bucketLevel 存储桶等级：0 线上-受理单集中桶；1 线上-受理单分库桶； 2.线下-受理单分库桶  3.线下-纸质工单分库桶
     * @return java.lang.String
     * @author yangzhenyu
     * @date 2022/8/26 10:11
     */
    public static String getFilePathByBucketLevel(String opTime, String key, Integer bucketLevel) {
        File file = new File(key);
        if (!file.exists()) {
            downloadFileByBucketLevel(opTime, key, key, bucketLevel);
        }
        return key;
    }

    /**
     * cos删除文件：可根据受理时间、桶等级自行桶名称组装并获取文件
     *
     * @param opTime      受理时间
     * @param key         cos存储key
     * @param bucketLevel 存储桶等级：0 线上-受理单集中桶；1 线上-受理单分库桶； 2.线下-受理单分库桶  3.线下-纸质工单分库桶
     * @author yangzhenyu
     * @date 2022/8/18 17:27
     */
    public static void deleteObjectByBucketLevel(String opTime, String key, Integer bucketLevel) {
        log.info("[CosUtilNew][deleteObjectByBucketLevel] -> Cos access to CosUtilNew");
        long startTime = System.currentTimeMillis();
        String bucketName = convertBucketName(opTime, bucketLevel);
        if (conn.doesObjectExist(bucketName, convertCosKey(key))) {
            try {
                conn.deleteObject(bucketName, convertCosKey(key));
            } catch (Exception e) {
                log.error("[deleteObjectByBucketLevel] [key=" + key + "][Exception->" + e);
            }
        }
        long endTime = System.currentTimeMillis();
        log.info("[deleteObjectByBucketLevel] -> process time " + (endTime - startTime));
    }

    /**
     * cos下载文件：可根据受理时间、桶等级自行桶名称组装并获取文件
     *
     * @param opTime      受理时间
     * @param key         cos存储key
     * @param localPath   本地文件全路径地址(例如：D:\test.pdf)
     * @param bucketLevel 存储桶等级：0 线上-受理单集中桶；1 线上-受理单分库桶； 2.线下-受理单分库桶  3.线下-纸质工单分库桶
     * @return boolean
     * @author yangzhenyu
     * @date 2022/6/9 16:03
     */
    public static boolean downloadFileByBucketLevel(String opTime, String key, String localPath, Integer bucketLevel) {
        log.info("[CosUtilNew][downloadFileByBucketLevel] -> Cos access to CosUtilNew");
        long startTime = System.currentTimeMillis();
        //1.确认并创建上一级目录
        boolean flag = true;
        File file = new File(localPath);
        if (!file.exists()) {
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
        }
        key = convertCosKey(key);
        //2.根据bucketLevel及受理时间获取桶名
        String bucketName = convertBucketName(opTime, bucketLevel);
        //3.下载cos文件
        if (doesObjectExist(bucketName, key)) {
            try {
                conn.getObject(new GetObjectRequest(bucketName, key), new File(localPath));
            } catch (Exception e) {
                log.info("[downloadFileByBucketLevel] -> Cos Exception res=" + e);
            }
        } else {
            log.info("[downloadFileByBucketLevel] -> Cos key not found");
            flag = false;
        }
        long endTime = System.currentTimeMillis();
        log.info("[downloadFileByBucketLevel] -> process time " + (endTime - startTime));
        return flag;
    }

    /**
     * 上传文件到cos存储服务器：可根据受理时间、桶等级自行桶名称组装并上传文件
     *
     * @param opTime      受理时间
     * @param key         cos存储key
     * @param file        文件对象
     * @param bucketLevel 存储桶等级：0 线上-受理单集中桶；1 线上-受理单分库桶； 2.线下-受理单分库桶  3.线下-纸质工单分库桶
     * @author yangzhenyu
     * @date 2022/6/9 16:57
     */
    public static Boolean uploadFileByBucketLevel(String opTime, String key, File file, Integer bucketLevel) {
        log.info("[CosUtilNew][uploadFileByBucketLevel] -> Cos access to CosUtilNew");
        long startTime = System.currentTimeMillis();
        Boolean flag = true;
        if (FileUtil.optionSystemWin()) {
            key = key.replace("\\", "/");
        }
        //1.根据bucketLevel及受理时间获取桶名
        String bucketName = convertBucketName(opTime, bucketLevel);
        //2.将本地文件上传到cos存储服务器
        PutObjectResult result = null;
        try {
            result = conn.putObject(bucketName, convertCosKey(key), file);
        } catch (SdkClientException e) {
            flag = false;
            log.info("[uploadFileByBucketLevel] -> Cos Exception res=" + e);
        }
        log.info("[uploadObject] -> key result = " + result.toString());
        long endTime = System.currentTimeMillis();
        log.info("[uploadObject] -> process time " + (endTime - startTime));
        return flag;
    }

    public static void main(String[] args) {

        String key = "data/wwwroot/wzh.10010.com/datafile/2022/11_110/20220609/202206091712494991199202106021712.pdf";
        // CosUtil cosUtil = new CosUtil();
        // URL objectDownloadLink = cosUtil.getObjectDownloadLink("bhps", key);
        // System.out.println("连接+++"+conn.doesObjectExist("bhps201901", key));
        // System.out.println("连接+++"+conn.getObject(new GetObjectRequest("bhps201901",
        // convertCosKey(key)), new File("D://123.pdf")));
		/*downloadFile("201901",
				"data/wwwroot/wzh.10010.com/datafile/2019/11_110/20190102/202106021712494991199202106021712.pdf",
				"D://123.pdf");*/
        //uploadObjectByBucketName("bhpstemp", "data/wwwroot/wzh.10010.com/datafile/11_3500.pdf", new File("E:\\reqFile\\11_3500.pdf"));
        //uploadFileByBucketLevel("202206",key,new File("E:\\reqFile\\11_100209.pdf"),CosConstant.OFFLINE_SEPARATED_PAPER_CASE_BUCKET_LEVEL);
        downloadFileByBucketLevel("202206", key, "D:\\test.pdf", CosConstant.OFFLINE_SEPARATED_PAPER_CASE_BUCKET_LEVEL);
    }

    @Test
    public void testGetBuckets() {
        List<Bucket> buckets = conn.listBuckets();
        for (Bucket bucket : buckets) {
            System.out.println(bucket.getName() + " " + StringUtils.fromDate(bucket.getCreationDate()));
        }
    }

    // 列出某个桶中的对象
    @Test
    public void testListBucketObjects() {
        String bucketName = "bhps002";

        ObjectListing objects = conn.listObjects(bucketName);
        do {
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                System.out.println(objectSummary.getKey() + " " + objectSummary.getSize() + " "
                        + StringUtils.fromDate(objectSummary.getLastModified()));
            }
            objects = conn.listNextBatchOfObjects(objects);
        } while (objects.isTruncated());
    }

    /**
     * 创建子目录 不需要单独创建子目录
     *
     * @param folderName
     */
    public void createFolder(String folderName) {
        createFolder(null, folderName);
    }

    /**
     * 创建子目录 不需要单独创建子目录
     *
     * @param bucketName
     * @param folderName
     */
    public void createFolder(String opTime, String folderName) {
        String bucketName = convertBucketName(opTime);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        // 创建空内容
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, convertCosKey(folderName) + "/",
                emptyContent, metadata);
        // 创建目录
        conn.putObject(putObjectRequest);
    }

    /**
     * 获取对象下载链接
     *
     * @param bucketName
     * @param key
     * @return url
     */
    public URL getObjectDownloadLink(String bucketName, String key) {
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, convertCosKey(key));
        System.out.println("getObjectDownloadLink==" + conn.generatePresignedUrl(request));
        return conn.generatePresignedUrl(request);
    }

}
