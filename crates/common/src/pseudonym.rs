use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Pseudonymizes raw usernames with HMAC-SHA256 under a secret salt.
///
/// A plain hash (e.g. `sha256(username)`) would let anyone precompute a
/// dictionary of hashes for known usernames and match them against the
/// pseudonyms in our data. Keying the hash with a secret salt that never
/// leaves `ingest-gateway` closes that off: without the salt, the mapping
/// from username to pseudonym can't be reproduced or reversed.
#[derive(Clone)]
pub struct Pseudonymizer {
    salt: Vec<u8>,
}

impl Pseudonymizer {
    pub fn new(salt: impl Into<Vec<u8>>) -> Self {
        Self { salt: salt.into() }
    }

    /// Returns the hex-encoded HMAC-SHA256 pseudonym for a raw username.
    pub fn pseudonymize(&self, raw_user: &str) -> String {
        let mut mac =
            HmacSha256::new_from_slice(&self.salt).expect("HMAC accepts keys of any length");
        mac.update(raw_user.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_input_same_output() {
        let p = Pseudonymizer::new("salt1");
        assert_eq!(p.pseudonymize("Alice"), p.pseudonymize("Alice"));
    }

    #[test]
    fn different_users_differ() {
        let p = Pseudonymizer::new("salt1");
        assert_ne!(p.pseudonymize("Alice"), p.pseudonymize("Bob"));
    }

    #[test]
    fn different_salt_differs() {
        let a = Pseudonymizer::new("salt1");
        let b = Pseudonymizer::new("salt2");
        assert_ne!(a.pseudonymize("Alice"), b.pseudonymize("Alice"));
    }

    #[test]
    fn output_is_hex_sha256_length() {
        let p = Pseudonymizer::new("salt1");
        let out = p.pseudonymize("Alice");
        assert_eq!(out.len(), 64);
        assert!(out.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
