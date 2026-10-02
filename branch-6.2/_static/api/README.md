# README

This directory contains static files and dependencies for the **Swagger UI library** to ensure compatibility with **Swagger 1.2** (deprecated). Files are managed for manual updates and customization.

## Configuration

- **Swagger UI Version:** 2.2.10
- **Source:** [Swagger UI Repository](https://github.com/swagger-api/swagger-ui/tree/v2.2.10/dist)
- **Included Files:**
  - `swagger-ui.min.js`
  - Relevant dependencies from the [dist/lib directory](https://github.com/swagger-api/swagger-ui/tree/v2.2.10/dist/lib)

## Rationale

 Files are included directly to avoid adding `npm` as a build dependency. Deprecated libraries are not loaded from a CDN to maintain control over versions and be able to submit fixes if necessary.

## Moving to OpenAPI 3

When transitioning to **OpenAPI 3**, the current setup should be reevaluated. 

- **Swagger 1.2** is deprecated, and using an outdated version may introduce maintenance overhead and compatibility issues.
- **OpenAPI 3** provides improved functionality, broader tool support, and up-to-date best practices.