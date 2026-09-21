# since RateLimitReached was introduced only on scylla-driver==3.26.3
# we need this fallback for when using older drivers or cassandra-driver
from cassandra.protocol import ConfigurationException

try:
    from cassandra.protocol import RateLimitReached

    rate_limit_expected_errors = (RateLimitReached,)
except ImportError:
    rate_limit_expected_errors = (ConfigurationException,)
