package com.project.login.service.minio;

import io.minio.*;
import io.minio.http.Method;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

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
            // 处理文件名：移除特殊字符，只保留文件名（不含路径）
            String originalFilename = file.getOriginalFilename();
            if (originalFilename == null || originalFilename.isEmpty()) {
                originalFilename = "file";
            } else {
                // 只取文件名部分（去除路径）
                int lastSlash = originalFilename.lastIndexOf('/');
                int lastBackslash = originalFilename.lastIndexOf('\\');
                int lastSeparator = Math.max(lastSlash, lastBackslash);
                if (lastSeparator >= 0) {
                    originalFilename = originalFilename.substring(lastSeparator + 1);
                }
                // 移除特殊字符，只保留字母、数字、点、下划线、连字符
                originalFilename = originalFilename.replaceAll("[^a-zA-Z0-9._-]", "_");
            }
            
            // 获取文件扩展名
            String extension = "";
            int lastDot = originalFilename.lastIndexOf('.');
            if (lastDot > 0) {
                extension = originalFilename.substring(lastDot);
                originalFilename = originalFilename.substring(0, lastDot);
            }
            
            // 生成安全的文件名
            String fileName = UUID.randomUUID().toString() + "_" + originalFilename + extension;
            
            // 确保Content-Type正确
            String contentType = file.getContentType();
            if (contentType == null || contentType.isEmpty()) {
                // 根据扩展名推断Content-Type
                if (extension.equalsIgnoreCase(".jpg") || extension.equalsIgnoreCase(".jpeg")) {
                    contentType = "image/jpeg";
                } else if (extension.equalsIgnoreCase(".png")) {
                    contentType = "image/png";
                } else if (extension.equalsIgnoreCase(".gif")) {
                    contentType = "image/gif";
                } else if (extension.equalsIgnoreCase(".webp")) {
                    contentType = "image/webp";
                } else {
                    contentType = "application/octet-stream";
                }
            }

            log.info("Uploading file: {} with content type: {}", fileName, contentType);

            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucket)
                            .object(fileName)
                            .stream(
                                    file.getInputStream(),
                                    file.getSize(),          // 使用实际文件大小
                                    10 * 1024 * 1024         // 分片大小 10MB（必填）
                            )
                            .contentType(contentType)
                            .build()
            );

            log.info("File uploaded successfully: {}", fileName);
            return fileName;

        } catch (Exception e) {
            log.error("File upload failed", e);
            throw new RuntimeException("File upload failed: " + e.getMessage(), e);
        }
    }

    // 获取文件预览 URL（默认 7 天）
    public String getFileUrl(String fileName) {
        try {
            String url = minioClient.getPresignedObjectUrl(
                    GetPresignedObjectUrlArgs.builder()
                            .method(Method.GET)
                            .bucket(bucket)
                            .object(fileName)
                            .expiry(7, TimeUnit.DAYS)
                            .build()
            );
            log.info("Generated presigned URL for file: {}", fileName);
            return url;
        } catch (Exception e) {
            log.error("Failed to get file preview URL for: {}", fileName, e);
            throw new RuntimeException("Failed to get file preview URL: " + e.getMessage(), e);
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

