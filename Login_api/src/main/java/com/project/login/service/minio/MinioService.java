package com.project.login.service.minio;

import io.minio.*;
import io.minio.http.Method;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class MinioService {

    private final MinioClient minioClient;

    private final String bucket = "notesharing";

    public String uploadFile(MultipartFile file) {
        try {
            String fileName = UUID.randomUUID() + "_" + file.getOriginalFilename();

            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucket)
                            .object(fileName)
                            .stream(
                                    file.getInputStream(),
                                    -1,                      // size 不确定
                                    10 * 1024 * 1024         // 分片大小 10MB（必填）
                            )
                            .contentType(
                                    Optional.ofNullable(file.getContentType())
                                            .orElse("application/octet-stream")
                            )
                            .build()
            );

            return fileName;

        } catch (Exception e) {
            e.printStackTrace(); // 打印堆栈，方便调试
            throw new RuntimeException("File upload failed", e);
        }
    }

    // 获取文件预览 URL（默认 7 天）
    public String getFileUrl(String fileName) {
        try {
            return minioClient.getPresignedObjectUrl(
                    GetPresignedObjectUrlArgs.builder()
                            .method(Method.GET)
                            .bucket(bucket)
                            .object(fileName)
                            .expiry(7, TimeUnit.DAYS)
                            .build()
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to get file preview URL", e);
        }
    }

    // 下载文件（返回字节数组）
    public byte[] download(String fileName) {
        try (GetObjectResponse response = minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(bucket)
                        .object(fileName)
                        .build()
        )) {
            return response.readAllBytes();
        } catch (Exception e) {
            throw new RuntimeException("Download failed", e);
        }
    }

    // 根据文件名删除文件
    public void deleteFile(String fileName) {
        try {
            minioClient.removeObject(
                    RemoveObjectArgs.builder()
                            .bucket(bucket)
                            .object(fileName)
                            .build()
            );
            log.info("File deleted from MinIO: {}", fileName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete file: " + fileName, e);
        }
    }

}

