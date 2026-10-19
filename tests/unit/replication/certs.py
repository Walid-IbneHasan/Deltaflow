"""A throwaway self-signed certificate, for TLS tests that need a trust anchor.

Generated once with (the private key was discarded, so nothing can present it):

    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
        -subj /CN=localhost -addext subjectAltName=DNS:localhost -days 36500
"""

from __future__ import annotations

from pathlib import Path

TRUST_ANCHOR = """-----BEGIN CERTIFICATE-----
MIIBlTCCATugAwIBAgIUZYXVrzL4v5JIvu+LhWUeEvvRjW0wCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MTAxOTA3MTIyOFoYDzIxMjYwOTI1
MDcxMjI4WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIBBggqhkjO
PQMBBwNCAARMwb0NjxmT+WKZSu52cByLBMCZ0lOXOnKNwnrd/OgUBCR1DIXNFTb+
uEZmwrog1lPKTY5RHVFggsNrdDGU7R8Qo2kwZzAdBgNVHQ4EFgQU8Qzu31IadrHB
rlucIZXGztjZMNQwHwYDVR0jBBgwFoAU8Qzu31IadrHBrlucIZXGztjZMNQwDwYD
VR0TAQH/BAUwAwEB/zAUBgNVHREEDTALgglsb2NhbGhvc3QwCgYIKoZIzj0EAwID
SAAwRQIgBHr18KEkVcoa8JJpyz8wf/BKCmD/Gny1oIENLiuXt7kCIQDfyAZbk1fX
PT9AxWmj7RlYxjCEuI4bVKwSulYstjpN6Q==
-----END CERTIFICATE-----
"""


def trust_anchor(directory: Path) -> Path:
    path = directory / "root.crt"
    path.write_text(TRUST_ANCHOR, encoding="ascii")
    return path
