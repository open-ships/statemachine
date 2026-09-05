# Security policy

Report suspected vulnerabilities privately through GitHub Security Advisories
for this repository. Include the affected version, reproduction, impact, and
any known mitigation. Do not include live vessel, operator, credential, or
safety-controller data.

Maintainers will acknowledge a report, assess affected versions, coordinate a
fix and disclosure, and publish a release when remediation is ready. Releases
use annotated Git tags, SHA-256 checksums, an SBOM, and separate GitHub
build-provenance and SBOM attestations for distributed artifacts. The tags
are not cryptographically signed; verify the artifact attestations and checksums
when consuming release archives.

Version 1.4.0 establishes the first stable release; earlier versions are
unsupported development prereleases. Security support targets the latest stable
major release. Product integrators remain responsible for command authentication,
controller fencing, secure boot/update, key management, and vulnerability
response for their deployed system.
