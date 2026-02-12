"""Map SharePoint library paths to RAG access-control tags.

Access tags are stored on S3 objects and in JSON twin metadata.  The RAG
query layer uses them to filter vector search results based on the
caller's role.

Rules are loaded from a YAML config file (default:
``src/config/access_rules.yaml``).  Patterns use :func:`fnmatch.fnmatch`
syntax.

Usage::

    from access_control import AccessControlMapper

    acl = AccessControlMapper()
    tags = acl.map_document("HR-Policies", "/onboarding/guide.pdf")
    # ["hr", "leadership", "admin", "all-staff"]

    allowed = acl.get_tags_for_user_role("engineer")
    # ["engineering", "all-staff"]
"""

import fnmatch
import logging
import os
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_DEFAULT_RULES_PATH = os.path.join(os.path.dirname(__file__), "config", "access_rules.yaml")


class AccessControlMapper:
    """Resolve access-control tags for documents and user roles."""

    def __init__(self, rules_path: str | None = None) -> None:
        """Load rules from a YAML config file.

        Parameters
        ----------
        rules_path:
            Path to the YAML rules file.  Defaults to
            ``src/config/access_rules.yaml``.
        """
        self._rules_path = rules_path or _DEFAULT_RULES_PATH
        self._rules: list[dict[str, Any]] = []
        self._role_mappings: dict[str, list[str]] = {}
        self._all_tags: set[str] = set()

        self._load(self._rules_path)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def map_document(self, sp_library: str, sp_path: str = "") -> list[str]:
        """Return the access tags for a document.

        Rules are evaluated top-to-bottom.  The first matching rule's tags
        are used.  Tags from the catch-all ``"*"`` rule are always appended
        so that every document has a baseline tag set.

        Parameters
        ----------
        sp_library:
            SharePoint document library name (e.g. ``"HR-Policies"``).
        sp_path:
            Optional SharePoint relative path (unused today but reserved
            for future path-level rules).

        Returns
        -------
        list[str]
            Deduplicated, sorted list of access tags.
        """
        tags: list[str] = []
        matched = False

        for rule in self._rules:
            pattern = rule["library_pattern"]

            if pattern == "*":
                # Wildcard rule is always appended, not counted as "match"
                tags.extend(rule["access_tags"])
                continue

            if not matched and fnmatch.fnmatch(sp_library, pattern):
                tags.extend(rule["access_tags"])
                matched = True

        # Deduplicate while preserving a deterministic order
        return sorted(set(tags))

    def get_tags_for_user_role(self, role: str) -> list[str]:
        """Return the document tags accessible to a platform role.

        Parameters
        ----------
        role:
            Platform role identifier (e.g. ``"admin"``, ``"engineer"``).

        Returns
        -------
        list[str]
            Tags the role can access.  ``"admin"`` receives all known tags.
            Unknown roles get an empty list.
        """
        allowed = self._role_mappings.get(role, [])

        if "*" in allowed:
            return sorted(self._all_tags)

        return sorted(allowed)

    @property
    def rules(self) -> list[dict[str, Any]]:
        """Return a copy of the loaded rules."""
        return list(self._rules)

    @property
    def role_mappings(self) -> dict[str, list[str]]:
        """Return a copy of the loaded role mappings."""
        return dict(self._role_mappings)

    # ------------------------------------------------------------------ #
    # Internal
    # ------------------------------------------------------------------ #

    def _load(self, path: str) -> None:
        """Parse and validate the YAML rules file."""
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        except FileNotFoundError:
            logger.error("Access rules file not found: %s", path)
            raise
        except yaml.YAMLError as exc:
            logger.error("Invalid YAML in %s: %s", path, exc)
            raise

        if not isinstance(data, dict):
            raise ValueError(f"Expected top-level dict in {path}, got {type(data).__name__}")

        # Load rules
        raw_rules = data.get("rules", [])
        if not isinstance(raw_rules, list):
            raise ValueError(f"'rules' must be a list in {path}")

        for i, rule in enumerate(raw_rules):
            if "library_pattern" not in rule or "access_tags" not in rule:
                raise ValueError(
                    f"Rule {i} in {path} must have 'library_pattern' and 'access_tags'"
                )
            if not isinstance(rule["access_tags"], list):
                raise ValueError(f"Rule {i} 'access_tags' must be a list in {path}")

        self._rules = raw_rules

        # Collect all known tags across all rules
        for rule in self._rules:
            self._all_tags.update(rule["access_tags"])

        # Load role mappings
        raw_roles = data.get("role_mappings", {})
        if not isinstance(raw_roles, dict):
            raise ValueError(f"'role_mappings' must be a dict in {path}")

        self._role_mappings = {
            str(k): list(v) for k, v in raw_roles.items()
        }

        logger.info(
            "Loaded %d access rules and %d role mappings from %s",
            len(self._rules), len(self._role_mappings), path,
        )
