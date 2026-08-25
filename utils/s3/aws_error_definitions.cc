/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "aws_error.hh"


namespace aws {

using namespace utils::http;

const aws_errors& aws_error::get_errors() {
    static const std::unordered_map<std::string_view, const aws_error> aws_error_map{
        {"IncompleteSignature", aws_error(aws_error_type::INCOMPLETE_SIGNATURE, retryable::no)},
        {"IncompleteSignatureException", aws_error(aws_error_type::INCOMPLETE_SIGNATURE, retryable::no)},
        {"InvalidSignatureException", aws_error(aws_error_type::INVALID_SIGNATURE, retryable::no)},
        {"InvalidSignature", aws_error(aws_error_type::INVALID_SIGNATURE, retryable::no)},
        {"InternalFailureException", aws_error(aws_error_type::INTERNAL_FAILURE, retryable::yes)},
        {"InternalFailure", aws_error(aws_error_type::INTERNAL_FAILURE, retryable::yes)},
        {"InternalServerError", aws_error(aws_error_type::INTERNAL_FAILURE, retryable::yes)},
        {"InternalError", aws_error(aws_error_type::INTERNAL_FAILURE, retryable::yes)},
        {"InvalidActionException", aws_error(aws_error_type::INVALID_ACTION, retryable::no)},
        {"InvalidAction", aws_error(aws_error_type::INVALID_ACTION, retryable::no)},
        {"InvalidClientTokenIdException", aws_error(aws_error_type::INVALID_CLIENT_TOKEN_ID, retryable::no)},
        {"InvalidClientTokenId", aws_error(aws_error_type::INVALID_CLIENT_TOKEN_ID, retryable::no)},
        {"InvalidParameterCombinationException", aws_error(aws_error_type::INVALID_PARAMETER_COMBINATION, retryable::no)},
        {"InvalidParameterCombination", aws_error(aws_error_type::INVALID_PARAMETER_COMBINATION, retryable::no)},
        {"InvalidParameterValueException", aws_error(aws_error_type::INVALID_PARAMETER_VALUE, retryable::no)},
        {"InvalidParameterValue", aws_error(aws_error_type::INVALID_PARAMETER_VALUE, retryable::no)},
        {"InvalidQueryParameterException", aws_error(aws_error_type::INVALID_QUERY_PARAMETER, retryable::no)},
        {"InvalidQueryParameter", aws_error(aws_error_type::INVALID_QUERY_PARAMETER, retryable::no)},
        {"MalformedQueryStringException", aws_error(aws_error_type::MALFORMED_QUERY_STRING, retryable::no)},
        {"MalformedQueryString", aws_error(aws_error_type::MALFORMED_QUERY_STRING, retryable::no)},
        {"MissingActionException", aws_error(aws_error_type::MISSING_ACTION, retryable::no)},
        {"MissingAction", aws_error(aws_error_type::MISSING_ACTION, retryable::no)},
        {"MissingAuthenticationTokenException", aws_error(aws_error_type::MISSING_AUTHENTICATION_TOKEN, retryable::no)},
        {"MissingAuthenticationToken", aws_error(aws_error_type::MISSING_AUTHENTICATION_TOKEN, retryable::no)},
        {"MissingParameterException", aws_error(aws_error_type::MISSING_PARAMETER, retryable::no)},
        {"MissingParameter", aws_error(aws_error_type::MISSING_PARAMETER, retryable::no)},
        {"OptInRequired", aws_error(aws_error_type::OPT_IN_REQUIRED, retryable::no)},
        {"RequestExpiredException", aws_error(aws_error_type::REQUEST_EXPIRED, retryable::yes)},
        {"RequestExpired", aws_error(aws_error_type::REQUEST_EXPIRED, retryable::yes)},
        {"ServiceUnavailableException", aws_error(aws_error_type::SERVICE_UNAVAILABLE, retryable::yes)},
        {"ServiceUnavailableError", aws_error(aws_error_type::SERVICE_UNAVAILABLE, retryable::yes)},
        {"ServiceUnavailable", aws_error(aws_error_type::SERVICE_UNAVAILABLE, retryable::yes)},
        {"RequestThrottledException", aws_error(aws_error_type::THROTTLING, retryable::yes)},
        {"RequestThrottled", aws_error(aws_error_type::THROTTLING, retryable::yes)},
        {"ThrottlingException", aws_error(aws_error_type::THROTTLING, retryable::yes)},
        {"ThrottledException", aws_error(aws_error_type::THROTTLING, retryable::yes)},
        {"Throttling", aws_error(aws_error_type::THROTTLING, retryable::yes)},
        {"ValidationErrorException", aws_error(aws_error_type::VALIDATION, retryable::no)},
        {"ValidationException", aws_error(aws_error_type::VALIDATION, retryable::no)},
        {"ValidationError", aws_error(aws_error_type::VALIDATION, retryable::no)},
        {"AccessDeniedException", aws_error(aws_error_type::ACCESS_DENIED, retryable::no)},
        {"AccessDenied", aws_error(aws_error_type::ACCESS_DENIED, retryable::no)},
        {"ResourceNotFoundException", aws_error(aws_error_type::RESOURCE_NOT_FOUND, retryable::no)},
        {"ResourceNotFound", aws_error(aws_error_type::RESOURCE_NOT_FOUND, retryable::no)},
        {"UnrecognizedClientException", aws_error(aws_error_type::UNRECOGNIZED_CLIENT, retryable::no)},
        {"UnrecognizedClient", aws_error(aws_error_type::UNRECOGNIZED_CLIENT, retryable::no)},
        {"SlowDownException", aws_error(aws_error_type::SLOW_DOWN, retryable::yes)},
        {"SlowDown", aws_error(aws_error_type::SLOW_DOWN, retryable::yes)},
        {"SignatureDoesNotMatchException", aws_error(aws_error_type::SIGNATURE_DOES_NOT_MATCH, retryable::no)},
        {"SignatureDoesNotMatch", aws_error(aws_error_type::SIGNATURE_DOES_NOT_MATCH, retryable::no)},
        {"InvalidAccessKeyIdException", aws_error(aws_error_type::INVALID_ACCESS_KEY_ID, retryable::no)},
        {"InvalidAccessKeyId", aws_error(aws_error_type::INVALID_ACCESS_KEY_ID, retryable::no)},
        {"RequestTimeTooSkewedException", aws_error(aws_error_type::REQUEST_TIME_TOO_SKEWED, retryable::yes)},
        {"RequestTimeTooSkewed", aws_error(aws_error_type::REQUEST_TIME_TOO_SKEWED, retryable::yes)},
        {"RequestTimeoutException", aws_error(aws_error_type::REQUEST_TIMEOUT, retryable::yes)},
        {"RequestTimeout", aws_error(aws_error_type::REQUEST_TIMEOUT, retryable::yes)},
        {"HTTP_NOT_IMPLEMENTED", aws_error(aws_error_type::HTTP_NOT_IMPLEMENTED, retryable::no)},
        {"HTTP_UNAUTHORIZED", aws_error(aws_error_type::HTTP_UNAUTHORIZED, retryable::no)},
        {"HTTP_FORBIDDEN", aws_error(aws_error_type::HTTP_FORBIDDEN, retryable::no)},
        {"HTTP_NOT_FOUND", aws_error(aws_error_type::HTTP_NOT_FOUND, retryable::no)},
        {"HTTP_TOO_MANY_REQUESTS", aws_error(aws_error_type::HTTP_TOO_MANY_REQUESTS, retryable::yes)},
        {"HTTP_INTERNAL_SERVER_ERROR", aws_error(aws_error_type::HTTP_INTERNAL_SERVER_ERROR, retryable::yes)},
        {"HTTP_BANDWIDTH_LIMIT_EXCEEDED", aws_error(aws_error_type::HTTP_BANDWIDTH_LIMIT_EXCEEDED, retryable::yes)},
        {"HTTP_SERVICE_UNAVAILABLE", aws_error(aws_error_type::HTTP_SERVICE_UNAVAILABLE, retryable::yes)},
        {"HTTP_REQUEST_TIMEOUT", aws_error(aws_error_type::HTTP_REQUEST_TIMEOUT, retryable::yes)},
        {"HTTP_PAGE_EXPIRED", aws_error(aws_error_type::HTTP_PAGE_EXPIRED, retryable::yes)},
        {"HTTP_LOGIN_TIMEOUT", aws_error(aws_error_type::HTTP_LOGIN_TIMEOUT, retryable::yes)},
        {"HTTP_GATEWAY_TIMEOUT", aws_error(aws_error_type::HTTP_GATEWAY_TIMEOUT, retryable::yes)},
        {"HTTP_NETWORK_CONNECT_TIMEOUT", aws_error(aws_error_type::HTTP_NETWORK_CONNECT_TIMEOUT, retryable::yes)},
        {"HTTP_NETWORK_READ_TIMEOUT", aws_error(aws_error_type::HTTP_NETWORK_READ_TIMEOUT, retryable::yes)},
        // Service-specific entries below are generated from the AWS c2j
        // models by scripts/gen_aws_service_errors.py. Do not edit by hand.
        // @SCYLLA_AWS_ERRORS_BEGIN@
        // S3
        {"AnnotationLimitExceeded", aws_error(aws_error_type::ANNOTATION_LIMIT_EXCEEDED, retryable::no)},
        {"AnnotationNameTooLong", aws_error(aws_error_type::ANNOTATION_NAME_TOO_LONG, retryable::no)},
        {"BucketAlreadyExists", aws_error(aws_error_type::BUCKET_ALREADY_EXISTS, retryable::no)},
        {"BucketAlreadyOwnedByYou", aws_error(aws_error_type::BUCKET_ALREADY_OWNED_BY_YOU, retryable::no)},
        {"EncryptionTypeMismatch", aws_error(aws_error_type::ENCRYPTION_TYPE_MISMATCH, retryable::no)},
        {"IdempotencyParameterMismatch", aws_error(aws_error_type::IDEMPOTENCY_PARAMETER_MISMATCH, retryable::no)},
        {"InvalidAnnotationName", aws_error(aws_error_type::INVALID_ANNOTATION_NAME, retryable::no)},
        {"InvalidObjectState", aws_error(aws_error_type::INVALID_OBJECT_STATE, retryable::no)},
        {"InvalidPrefix", aws_error(aws_error_type::INVALID_PREFIX, retryable::no)},
        {"InvalidRequest", aws_error(aws_error_type::INVALID_REQUEST, retryable::no)},
        {"InvalidWriteOffset", aws_error(aws_error_type::INVALID_WRITE_OFFSET, retryable::no)},
        {"NoSuchAnnotation", aws_error(aws_error_type::NO_SUCH_ANNOTATION, retryable::no)},
        {"NoSuchBucket", aws_error(aws_error_type::NO_SUCH_BUCKET, retryable::no)},
        {"NoSuchKey", aws_error(aws_error_type::NO_SUCH_KEY, retryable::no)},
        {"NoSuchUpload", aws_error(aws_error_type::NO_SUCH_UPLOAD, retryable::no)},
        {"ObjectAlreadyInActiveTierError", aws_error(aws_error_type::OBJECT_ALREADY_IN_ACTIVE_TIER, retryable::no)},
        {"ObjectNotInActiveTierError", aws_error(aws_error_type::OBJECT_NOT_IN_ACTIVE_TIER, retryable::no)},
        {"TooManyParts", aws_error(aws_error_type::TOO_MANY_PARTS, retryable::no)},
        {"UnsupportedMediaType", aws_error(aws_error_type::UNSUPPORTED_MEDIA_TYPE, retryable::no)},
        // STS
        {"ExpiredTokenException", aws_error(aws_error_type::EXPIRED_TOKEN, retryable::no)},
        {"ExpiredTradeInTokenException", aws_error(aws_error_type::EXPIRED_TRADE_IN_TOKEN, retryable::no)},
        {"InvalidAuthorizationMessageException", aws_error(aws_error_type::INVALID_AUTHORIZATION_MESSAGE, retryable::no)},
        {"InvalidIdentityToken", aws_error(aws_error_type::INVALID_IDENTITY_TOKEN, retryable::no)},
        {"IDPCommunicationError", aws_error(aws_error_type::I_D_P_COMMUNICATION_ERROR, retryable::yes)},
        {"IDPRejectedClaim", aws_error(aws_error_type::I_D_P_REJECTED_CLAIM, retryable::no)},
        {"JWTPayloadSizeExceededException", aws_error(aws_error_type::J_W_T_PAYLOAD_SIZE_EXCEEDED, retryable::no)},
        {"MalformedPolicyDocument", aws_error(aws_error_type::MALFORMED_POLICY_DOCUMENT, retryable::no)},
        {"OutboundWebIdentityFederationDisabledException", aws_error(aws_error_type::OUTBOUND_WEB_IDENTITY_FEDERATION_DISABLED, retryable::no)},
        {"PackedPolicyTooLarge", aws_error(aws_error_type::PACKED_POLICY_TOO_LARGE, retryable::no)},
        {"RegionDisabledException", aws_error(aws_error_type::REGION_DISABLED, retryable::no)},
        {"SessionDurationEscalationException", aws_error(aws_error_type::SESSION_DURATION_ESCALATION, retryable::no)},
        // @SCYLLA_AWS_ERRORS_END@
    };
    return aws_error_map;
}

} // namespace aws
