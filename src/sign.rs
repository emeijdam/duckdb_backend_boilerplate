//! SBOM signing — key-based ECDSA P-256 (pure Rust, no external tooling).
//!
//! The builder signs the CycloneDX bytes with a private key loaded from
//! `CROSV_SIGNING_KEY` (a PKCS#8 PEM). We emit the signature in two encodings:
//!   - DER (base64) — the canonical form `openssl`/cosign-family tools verify;
//!   - raw IEEE-P1363 `r‖s` (base64) — what the browser's WebCrypto verifies
//!     directly, so Sparrow R can flip its compliance badge with no DER decode.
//!
//! Signatures are deterministic (RFC 6979): the same bytes + key reproduce the
//! same signature, so a rebuilt list is bit-for-bit stable. If no key is
//! configured the SBOM is still emitted (hash-pinned) — signing is additive.

use base64::{engine::general_purpose::STANDARD, Engine};
use p256::ecdsa::{signature::Signer, Signature, SigningKey};
use p256::pkcs8::{DecodePrivateKey, EncodePublicKey, LineEnding};
use sha2::{Digest, Sha256};

pub struct LoadedSigner {
    key: SigningKey,
    /// Short fingerprint of the public key (SHA-256 of SPKI DER, first 16 hex).
    pub key_id: String,
    /// SPKI public key, PEM — served next to the SBOM for external verifiers.
    pub public_pem: String,
}

/// A signature over the SBOM, in both encodings, for the manifest + `.sig` file.
pub struct SbomSignature {
    pub alg: String,
    pub key_id: String,
    /// base64(DER) — written to `sbom.cdx.json.sig` (canonical / cosign-style).
    pub der_b64: String,
    /// base64(raw r‖s) — carried in the manifest for WebCrypto verification.
    pub raw_b64: String,
}

/// Load the signing key from `CROSV_SIGNING_KEY`. Returns None (and the build
/// proceeds unsigned) when unset or unreadable — never fails a build.
pub fn load_signer() -> Option<LoadedSigner> {
    let path = std::env::var("CROSV_SIGNING_KEY").ok()?;
    let pem = std::fs::read_to_string(&path).ok()?;
    let key = SigningKey::from_pkcs8_pem(&pem).ok()?;
    let vk = key.verifying_key();
    let spki = vk.to_public_key_der().ok()?;
    let fp = format!("{:x}", Sha256::digest(spki.as_bytes()));
    let public_pem = vk.to_public_key_pem(LineEnding::LF).ok()?;
    Some(LoadedSigner {
        key,
        key_id: fp[..16].to_string(),
        public_pem,
    })
}

impl LoadedSigner {
    /// Sign the given bytes (ECDSA P-256 / SHA-256).
    pub fn sign(&self, bytes: &[u8]) -> SbomSignature {
        let sig: Signature = self.key.sign(bytes);
        SbomSignature {
            alg: "ECDSA-P256-SHA256".to_string(),
            key_id: self.key_id.clone(),
            der_b64: STANDARD.encode(sig.to_der().as_bytes()),
            raw_b64: STANDARD.encode(sig.to_bytes()),
        }
    }
}
