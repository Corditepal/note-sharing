package com.project.login.service.login;

import com.project.login.model.entity.UserEntity;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import io.jsonwebtoken.io.Decoders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.crypto.SecretKey;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;

@Service
public class JwtService {

    @Value("${jwt.secret}")
    private String secretKey;

    private static final long EXPIRATION = 1000 * 60 * 60; // 1 hour

    // --- Generate Token ---
    public String generateToken(UserEntity user) {

        Map<String, Object> claims = Map.of(
                "userId", user.getId(),
                "email", user.getEmail(),
                "studentNumber", user.getStudentNumber()
        );

        return buildToken(claims, user.getEmail());
    }

    private String buildToken(Map<String, Object> claims, String subject) {

        return Jwts.builder()
                .subject(subject)
                .claims(claims)
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + EXPIRATION))
                .signWith(getSigningKey())     // 0.12+ 不再需要指定算法
                .compact();
    }

    // --- Extract Claims ---

    public String extractUsername(String token) {
        return extractClaim(token, Claims::getSubject);
    }

    public Date extractExpiration(String token) {
        return extractClaim(token, Claims::getExpiration);
    }

    public <T> T extractClaim(String token, Function<Claims, T> fn) {
        final Claims claims = extractAllClaims(token);
        return fn.apply(claims);
    }

    private Claims extractAllClaims(String token) {
        return Jwts.parser()
                .verifyWith(getSigningKey())  // 新 API，替代 setSigningKey()
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }

    // --- Validate Token ---

    public boolean isTokenValid(String token, UserEntity user) {
        return extractUsername(token).equals(user.getEmail()) && !isTokenExpired(token);
    }

    public boolean isTokenExpired(String token) {
        return extractExpiration(token).before(new Date());
    }

    // --- Secret Key ---

    private SecretKey getSigningKey() {
        byte[] keyBytes = Decoders.BASE64.decode(secretKey);
        return Keys.hmacShaKeyFor(keyBytes);
    }

    // --- Extract Email from token ---
    public String extractEmail(String token) {
        try {
            String email = extractUsername(token); // subject 存储的是邮箱
            if (email == null || isTokenExpired(token)) {
                return null; // token 过期或无效
            }
            return email;
        } catch (Exception e) {
            return null; // token 无效
        }
    }

}
