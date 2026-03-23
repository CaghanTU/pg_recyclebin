/// pgBackRest AES-256-CBC encryption/decryption.
///
/// Key hierarchy:
///   repo-cipher-pass (from GUC)
///     → used to decrypt backup.info and backup.manifest (repo-level encryption)
///   cipher-subpass (extracted from decrypted manifest's [cipher] section)
///     → used to decrypt actual backup data files
///
/// pgBackRest encryption format (encrypt.c):
///   - Magic header: "Salted__" (8 bytes)
///   - Salt: 8 bytes (bytes 8-15)
///   - Cipher IV + key derived with EVP_BytesToKey (MD5, 1 iteration):
///       key = 32 bytes (AES-256), iv = 16 bytes (AES-256-CBC)
///   - Ciphertext: remaining bytes after the 16-byte header
///
/// This is compatible with `openssl enc -aes-256-cbc -md md5 -pass pass:...`
use openssl::symm::{decrypt, Cipher};

const MAGIC: &[u8] = b"Salted__";
const HEADER_LEN: usize = 16; // 8 bytes magic + 8 bytes salt

/// Decrypt a pgBackRest encrypted file given a passphrase.
/// Returns the decrypted plaintext (may still be compressed — call decompress after).
pub fn decrypt_file(data: &[u8], passphrase: &str) -> Result<Vec<u8>, String> {
    if data.len() < HEADER_LEN {
        return Err(format!(
            "encrypted data too short ({} bytes), expected at least {} bytes",
            data.len(),
            HEADER_LEN
        ));
    }
    if &data[..8] != MAGIC {
        return Err("missing 'Salted__' magic header — data may not be encrypted".into());
    }

    let salt = &data[8..16];
    let ciphertext = &data[HEADER_LEN..];

    // Derive key + IV using EVP_BytesToKey (OpenSSL compat: MD5, 1 pass, 32+16=48 bytes)
    let (key, iv) = evp_bytes_to_key(passphrase.as_bytes(), salt, 32, 16);

    decrypt(Cipher::aes_256_cbc(), &key, Some(&iv), ciphertext)
        .map_err(|e| format!("AES-256-CBC decrypt failed: {}", e))
}

/// Derive key and IV from passphrase + salt using EVP_BytesToKey (MD5, count=1).
/// This replicates OpenSSL's default behaviour for `openssl enc -aes-256-cbc -md md5`.
fn evp_bytes_to_key(pass: &[u8], salt: &[u8], key_len: usize, iv_len: usize) -> (Vec<u8>, Vec<u8>) {
    use openssl::hash::{hash, MessageDigest};

    let needed = key_len + iv_len;
    let mut derived = Vec::with_capacity(needed);
    let mut last_hash: Vec<u8> = Vec::new();

    while derived.len() < needed {
        let mut input = last_hash.clone();
        input.extend_from_slice(pass);
        input.extend_from_slice(salt);
        last_hash = hash(MessageDigest::md5(), &input)
            .expect("MD5 hash failed")
            .to_vec();
        derived.extend_from_slice(&last_hash);
    }

    let key = derived[..key_len].to_vec();
    let iv = derived[key_len..key_len + iv_len].to_vec();
    (key, iv)
}

/// Check if data starts with the pgBackRest "Salted__" encryption magic.
pub fn is_encrypted(data: &[u8]) -> bool {
    data.len() >= 8 && &data[..8] == MAGIC
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Produce a pgBackRest-compatible AES-256-CBC encrypted blob for testing.
    /// Equivalent to: echo -n "hello world" | openssl enc -aes-256-cbc -md md5 -pass pass:secret -S AABBCCDDEEFF0011
    fn encrypt_for_test(plaintext: &[u8], passphrase: &str) -> Vec<u8> {
        use openssl::symm::{encrypt, Cipher};
        let salt = [0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11u8];
        let (key, iv) = evp_bytes_to_key(passphrase.as_bytes(), &salt, 32, 16);

        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&salt);
        let ciphertext = encrypt(Cipher::aes_256_cbc(), &key, Some(&iv), plaintext).unwrap();
        out.extend_from_slice(&ciphertext);
        out
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let plaintext = b"SELECT * FROM orders WHERE id = 42;";
        let pass = "my-repo-cipher-pass";
        let encrypted = encrypt_for_test(plaintext, pass);
        assert!(is_encrypted(&encrypted));
        let decrypted = decrypt_file(&encrypted, pass).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_wrong_passphrase() {
        let plaintext = b"sensitive data";
        let encrypted = encrypt_for_test(plaintext, "correct-pass");
        let result = decrypt_file(&encrypted, "wrong-pass");
        assert!(result.is_err());
    }

    #[test]
    fn test_not_encrypted() {
        let plaintext = b"plain text data without salted header";
        assert!(!is_encrypted(plaintext));
        let result = decrypt_file(plaintext, "anypass");
        assert!(result.is_err());
    }

    #[test]
    fn test_short_data() {
        let result = decrypt_file(b"short", "pass");
        assert!(result.is_err());
    }

    #[test]
    fn test_is_encrypted_valid() {
        let data = b"Salted__XXXXXXXXCIPHERTEXT";
        assert!(is_encrypted(data));
    }

    #[test]
    fn test_is_encrypted_too_short() {
        assert!(!is_encrypted(b"Salted_"));
        assert!(!is_encrypted(b""));
    }
}
