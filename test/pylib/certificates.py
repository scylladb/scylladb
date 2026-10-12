#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Certificates for TLS tests, made in-process with `cryptography`."""

import datetime
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID


def _pem_key(key) -> bytes:
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL,
                             serialization.NoEncryption())


class CertificateAuthority:
    """A self-signed CA which can issue certificates, written as PEM files.
    Certificates are valid from a day ago to a day from now."""
    def __init__(self, name: str = "TestCA"):
        self.key = ec.generate_private_key(ec.SECP256R1())
        self.name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
        self.cert = self._sign(self.name, self.key.public_key(), ca=True)

    def _sign(self, subject: x509.Name, public_key, ca: bool, email: str | None = None) -> x509.Certificate:
        now = datetime.datetime.now(datetime.timezone.utc)
        builder = (x509.CertificateBuilder()
                   .subject_name(subject)
                   .issuer_name(self.name)
                   .public_key(public_key)
                   .serial_number(x509.random_serial_number())
                   .not_valid_before(now - datetime.timedelta(days=1))
                   .not_valid_after(now + datetime.timedelta(days=1))
                   .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True))
        if email:
            builder = builder.add_extension(x509.SubjectAlternativeName([x509.RFC822Name(email)]), critical=False)
        return builder.sign(self.key, hashes.SHA256())

    def cert_pem(self) -> bytes:
        return self.cert.public_bytes(serialization.Encoding.PEM)

    def key_pem(self) -> bytes:
        return _pem_key(self.key)

    def write_cert(self, path) -> str:
        Path(path).write_bytes(self.cert_pem())
        return str(path)

    def issue(self, name: str | None, cert_path, key_path, *, organization: str | None = None,
              email: str | None = None) -> x509.Certificate:
        """Issue a certificate with the subject CN=`name` (or O=`organization`
        without a name), and an email subject alternative name if given; write
        it and its key."""
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)] if name else
                            [x509.NameAttribute(NameOID.ORGANIZATION_NAME, organization)])
        key = ec.generate_private_key(ec.SECP256R1())
        cert = self._sign(subject, key.public_key(), ca=False, email=email)
        Path(key_path).write_bytes(_pem_key(key))
        Path(cert_path).write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        return cert

    def write_crl(self, path, revoked: list[x509.Certificate]) -> str:
        """Write a certificate revocation list revoking the given certificates."""
        now = datetime.datetime.now(datetime.timezone.utc)
        builder = (x509.CertificateRevocationListBuilder()
                   .issuer_name(self.name)
                   .last_update(now - datetime.timedelta(days=1))
                   .next_update(now + datetime.timedelta(days=1)))
        for cert in revoked:
            builder = builder.add_revoked_certificate(x509.RevokedCertificateBuilder()
                                                      .serial_number(cert.serial_number)
                                                      .revocation_date(now - datetime.timedelta(days=1))
                                                      .build())
        Path(path).write_bytes(builder.sign(self.key, hashes.SHA256()).public_bytes(serialization.Encoding.PEM))
        return str(path)
