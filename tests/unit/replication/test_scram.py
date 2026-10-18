"""SCRAM-SHA-256 client against the RFC 7677 test vector."""

from __future__ import annotations

import pytest

from deltaflow.replication.scram import ScramError, ScramSha256

# RFC 7677 section 3.
NONCE = "rOprNGfwEbeRWgbNEkqO"
SERVER_FIRST = (
    "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
)
CLIENT_FINAL = (
    "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,"
    "p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
)
SERVER_FINAL = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="


def test_rfc7677_exchange() -> None:
    scram = ScramSha256("pencil", username="user", nonce=NONCE)
    assert scram.client_first() == b"n,,n=user,r=" + NONCE.encode()
    assert scram.client_final(SERVER_FIRST.encode()) == CLIENT_FINAL.encode()
    scram.verify_server_final(SERVER_FINAL.encode())


def test_postgres_style_empty_username() -> None:
    # Postgres ignores the SCRAM username and the client sends an empty one.
    assert ScramSha256("pw", nonce="abc").client_first() == b"n,,n=,r=abc"


def test_wrong_server_signature_is_rejected() -> None:
    scram = ScramSha256("pencil", username="user", nonce=NONCE)
    scram.client_final(SERVER_FIRST.encode())
    with pytest.raises(ScramError, match="signature"):
        scram.verify_server_final(b"v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")


def test_server_nonce_must_extend_client_nonce() -> None:
    scram = ScramSha256("pencil", nonce=NONCE)
    with pytest.raises(ScramError, match="nonce"):
        scram.client_final(b"r=somethingelse,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096")


def test_server_error_attribute_is_surfaced() -> None:
    scram = ScramSha256("pencil", nonce=NONCE)
    scram.client_final(SERVER_FIRST.encode())
    with pytest.raises(ScramError, match="invalid-proof"):
        scram.verify_server_final(b"e=invalid-proof")


def test_non_ascii_password_is_refused_rather_than_mis_hashed() -> None:
    # SASLprep is not implemented; refusing beats a confusing auth failure.
    with pytest.raises(ScramError, match="SASLprep"):
        ScramSha256("pässword")
