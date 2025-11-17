package com.example.demo;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.security.JwtUtil;

@CrossOrigin
@RestController
@RequestMapping("/auth")
public class AuthController {

	@Value("${valid.username}")
	private String validUsername;
	
	@Value("${valid.secret}")
	private String validSecret;
	
    @Autowired
    private JwtUtil jwtUtil;

    private static final Logger log = LoggerFactory.getLogger(AuthController.class);

//    @PostMapping("/login")
//    public ResponseEntity<?> login(@RequestBody Map<String, String> loginRequest) {
//        log.info("Send loginRequest={}", loginRequest);
//        String username = loginRequest.get("username");
//        String password = loginRequest.get("password");
//
//        if (validUsername.equals(username) && validSecret.equals(password)) {
//            String token = jwtUtil.generateToken(username);
//            return ResponseEntity.ok(Map.of("token", token));
//        } else {
//            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Invalid credentials"));
//        }
//    }
@PostMapping("/login")
public ResponseEntity<?> login(@RequestBody(required = false) Map<String, String> loginRequest) {

    long start = System.currentTimeMillis();
    log.info("Sending login request ");

    try {
        // ---------------------- Validate Incoming Request ----------------------
        if (loginRequest == null || loginRequest.isEmpty()) {
            log.warn("Login failed: Request body is missing or empty");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "Request body cannot be empty"));
        }

        String username = loginRequest.get("username");
        String password = loginRequest.get("password");

        // Prevent logging sensitive data
        log.debug("Login payload received for username='{}'", username);

        if (username == null || username.isBlank() || password == null || password.isBlank()) {
            log.warn("Login failed: Missing username or password");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "Username and password are required"));
        }

        // ---------------------- Credential Verification ----------------------
        if (validUsername.equals(username) && validSecret.equals(password)) {

            log.info("User '{}' authenticated successfully", username);

            String token = jwtUtil.generateToken(username);

            long duration = System.currentTimeMillis() - start;
            log.info("Login successful for '{}' | Token generated | TimeTaken={}ms", username, duration);

            return ResponseEntity.ok(Map.of(
                    "token", token,
                    "message", "Login successful"
            ));
        }

        // ---------------------- Invalid Credentials ----------------------
        log.warn("Login failed for username='{}' : Invalid credentials", username);

        return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                .body(Map.of("error", "Invalid credentials"));
    }
    catch (Exception ex) {
        // ---------------------- Unexpected Error ----------------------
        log.error("Unexpected error during login: {}", ex.getMessage(), ex);

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal server error", "details", ex.getMessage()));
    }
}

}
