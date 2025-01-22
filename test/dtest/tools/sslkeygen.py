#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import datetime
import ipaddress
import logging
import os.path
from socket import gethostname

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


logger = logging.getLogger(__name__)


def create_self_signed_x509_certificate(test_path, cert_file="scylla.crt", key_file="scylla.key", ip_list=None, cname=None, ca_key=None, ca_cert=None, email=None):  # noqa: PLR0913
    ip_list = ip_list or []

    cert_file = os.path.join(test_path, cert_file)
    key_file = os.path.join(test_path, key_file)

    # Create private RSA key
    one_day = datetime.timedelta(1, 0, 0)
    private_key = sign_key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())
    public_key = private_key.public_key()

    if cname is None:
        cname = gethostname()

    # create a self-signed cert
    builder = x509.CertificateBuilder()
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "IL"),
            x509.NameAttribute(NameOID.STREET_ADDRESS, "None"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "None"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "None"),
            x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "None"),
            x509.NameAttribute(NameOID.SERIAL_NUMBER, "1000"),
            x509.NameAttribute(NameOID.COMMON_NAME, cname),
        ]
    )

    if ca_cert is not None and ca_key is not None:
        with open(file=ca_cert, mode="rb") as file:
            data = file.read()
            ca = x509.load_pem_x509_certificate(data)
            issuer = ca.issuer
        with open(file=ca_key, mode="rb") as file:
            data = file.read()
            sign_key = serialization.load_pem_private_key(data, password=None)

    builder = builder.subject_name(subject)
    builder = builder.issuer_name(issuer)
    builder = builder.not_valid_before(datetime.datetime.today() - one_day)
    builder = builder.not_valid_after(datetime.datetime.today() + (one_day * 30))
    builder = builder.serial_number(x509.random_serial_number())
    builder = builder.public_key(public_key)

    altnames = [x509.DNSName(cname)]
    if email is not None:
        altnames.append(x509.RFC822Name(email))
    for ip in ip_list:
        altnames.append(x509.IPAddress(ipaddress.IPv4Address(ip)))

    builder = builder.add_extension(x509.SubjectAlternativeName(altnames), critical=False)
    builder = builder.add_extension(
        x509.BasicConstraints(ca=False, path_length=None),
        critical=True,
    )

    certificate = builder.sign(private_key=sign_key, algorithm=hashes.SHA256(), backend=default_backend())

    with open(file=cert_file, mode="wb") as file:
        file.write(certificate.public_bytes(serialization.Encoding.PEM))
    with open(file=key_file, mode="wb") as file:
        file.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )

    logger.debug(f'Created certificate file in "{cert_file}" path, and private key in "{key_file}" path')
    return cert_file, key_file
