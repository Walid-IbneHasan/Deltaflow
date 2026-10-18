"""SCRAM-SHA-256 client (RFC 5802 / RFC 7677), as used by PostgreSQL authentication.

Channel binding (SCRAM-SHA-256-PLUS) is not supported because the replication
connection does not use TLS. SASLprep is not implemented, so non-ASCII
passwords are refused up front instead of failing authentication obscurely.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets

MECHANISM = "SCRAM-SHA-256"
_GS2_HEADER = "n,,"  # no channel binding, no authzid


class ScramError(Exception):
    pass


def _hmac(key: bytes, msg: bytes) -> bytes:
    return hmac.digest(key, msg, "sha256")


def _attributes(message: bytes) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in message.decode().split(","):
        key, sep, value = part.partition("=")
        if not sep or len(key) != 1:
            raise ScramError(f"malformed SCRAM attribute {part!r}")
        out[key] = value
    return out


class ScramSha256:
    """One authentication exchange: client_first -> client_final -> verify_server_final."""

    def __init__(self, password: str, *, username: str = "", nonce: str | None = None) -> None:
        if not password.isascii() or any(ord(c) < 0x20 for c in password):
            raise ScramError("passwords needing SASLprep normalisation are not supported")
        self._password = password.encode()
        self._nonce = nonce if nonce is not None else secrets.token_urlsafe(18)
        self._first_bare = f"n={username},r={self._nonce}"
        self._server_signature: bytes | None = None

    def client_first(self) -> bytes:
        return (_GS2_HEADER + self._first_bare).encode()

    def client_final(self, server_first: bytes) -> bytes:
        attrs = _attributes(server_first)
        nonce = attrs.get("r", "")
        if not nonce.startswith(self._nonce) or len(nonce) == len(self._nonce):
            raise ScramError("server nonce does not extend the client nonce")
        try:
            salt = base64.b64decode(attrs["s"], validate=True)
            iterations = int(attrs["i"])
        except (KeyError, ValueError) as exc:
            raise ScramError(f"bad server-first-message: {server_first!r}") from exc

        salted = hashlib.pbkdf2_hmac("sha256", self._password, salt, iterations)
        client_key = _hmac(salted, b"Client Key")
        stored_key = hashlib.sha256(client_key).digest()
        channel = base64.b64encode(_GS2_HEADER.encode()).decode()
        without_proof = f"c={channel},r={nonce}"
        auth_message = f"{self._first_bare},{server_first.decode()},{without_proof}".encode()
        signature = _hmac(stored_key, auth_message)
        proof = bytes(a ^ b for a, b in zip(client_key, signature, strict=True))
        self._server_signature = _hmac(_hmac(salted, b"Server Key"), auth_message)
        return f"{without_proof},p={base64.b64encode(proof).decode()}".encode()

    def verify_server_final(self, server_final: bytes) -> None:
        if self._server_signature is None:
            raise ScramError("server-final-message before client-final-message")
        attrs = _attributes(server_final)
        if "e" in attrs:
            raise ScramError(f"server rejected authentication: {attrs['e']}")
        received = base64.b64decode(attrs.get("v", ""))
        if not hmac.compare_digest(received, self._server_signature):
            raise ScramError("server signature mismatch: the server does not know the password")
