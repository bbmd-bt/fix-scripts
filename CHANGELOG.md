# Changelog

All notable changes to this project will be documented in this file.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Fixed

- Prevented repeated reprocessing of duplicate deals by excluding trash-stage deals from selection and made creator filtering configurable so runs can cover all creators.
- Corrected duplicate-deal move reporting so HTTP 401/403 responses are recorded as errors and only HTTP 404 is recorded as not_found.
