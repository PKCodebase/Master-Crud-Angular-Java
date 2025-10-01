package com.example.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

@Component
public class ApiRequestLoggingFilter extends OncePerRequestFilter {

	@Value("${jwt.secret}")
    private String secretKey;
	
    private static final Logger logger = LoggerFactory.getLogger(ApiRequestLoggingFilter.class);

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {

        String ip = request.getHeader("X-Forwarded-For");
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }

        String method = request.getMethod();
        String uri = request.getRequestURI();
        String query = request.getQueryString();

        String username = "anonymous";
        String authHeader = request.getHeader("Authorization");

        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            try {
                String token = authHeader.substring(7);
                Claims claims = Jwts.parserBuilder()
                        .setSigningKey(secretKey.getBytes()) // same key as signing
                        .build()
                        .parseClaimsJws(token)
                        .getBody();
                username = claims.getSubject(); // "sub" field = username
            } catch (Exception e) {
                logger.warn("Failed to parse JWT for request [{} {}]: {}", method, uri, e.getMessage());
            }
        }

        // Log with username, ip, and API details
        logger.info("API Request: user='{}', ip='{}', [{}] {}{}",
                username,
                ip,
                method,
                uri,
                (query != null ? "?" + query : ""));

        filterChain.doFilter(request, response);
    }
}
