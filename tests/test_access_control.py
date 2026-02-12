"""Tests for AccessControlMapper."""

import os
import tempfile
import textwrap

import pytest

from access_control import AccessControlMapper


# ===================================================================
# Fixtures
# ===================================================================

@pytest.fixture
def default_mapper():
    """Load the production access_rules.yaml."""
    return AccessControlMapper()


@pytest.fixture
def custom_rules(tmp_path):
    """Create a custom rules file and return a factory function."""
    def _make(yaml_content: str) -> AccessControlMapper:
        path = tmp_path / "rules.yaml"
        path.write_text(textwrap.dedent(yaml_content))
        return AccessControlMapper(str(path))
    return _make


# ===================================================================
# Loading & validation
# ===================================================================

class TestLoading:
    def test_loads_default_rules(self, default_mapper):
        assert len(default_mapper.rules) >= 6

    def test_loads_role_mappings(self, default_mapper):
        assert "admin" in default_mapper.role_mappings
        assert "engineer" in default_mapper.role_mappings

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            AccessControlMapper(str(tmp_path / "nonexistent.yaml"))

    def test_invalid_yaml_raises(self, tmp_path):
        bad = tmp_path / "bad.yaml"
        bad.write_text("  - invalid:\nyaml: [")
        with pytest.raises(Exception):
            AccessControlMapper(str(bad))

    def test_missing_required_fields_raises(self, custom_rules):
        with pytest.raises(ValueError, match="library_pattern"):
            custom_rules("""\
                rules:
                  - access_tags: ["a"]
            """)

    def test_missing_access_tags_field_raises(self, custom_rules):
        with pytest.raises(ValueError, match="library_pattern"):
            custom_rules("""\
                rules:
                  - library_pattern: "HR*"
            """)

    def test_access_tags_must_be_list(self, custom_rules):
        with pytest.raises(ValueError, match="must be a list"):
            custom_rules("""\
                rules:
                  - library_pattern: "HR*"
                    access_tags: "not-a-list"
            """)

    def test_rules_not_a_list_raises(self, custom_rules):
        with pytest.raises(ValueError, match="must be a list"):
            custom_rules("""\
                rules:
                  library_pattern: "HR*"
                  access_tags: ["hr"]
            """)

    def test_role_mappings_not_a_dict_raises(self, custom_rules):
        with pytest.raises(ValueError, match="must be a dict"):
            custom_rules("""\
                rules:
                  - library_pattern: "*"
                    access_tags: ["all"]
                role_mappings:
                  - admin
            """)

    def test_empty_rules_valid(self, custom_rules):
        mapper = custom_rules("""\
            rules: []
        """)
        assert mapper.rules == []

    def test_custom_rules_path(self, custom_rules):
        mapper = custom_rules("""\
            rules:
              - library_pattern: "Test*"
                access_tags: ["test"]
              - library_pattern: "*"
                access_tags: ["base"]
            role_mappings:
              tester: ["test", "base"]
        """)
        assert len(mapper.rules) == 2
        assert "tester" in mapper.role_mappings


# ===================================================================
# map_document — pattern matching
# ===================================================================

class TestMapDocument:
    def test_hr_library_matches(self, default_mapper):
        tags = default_mapper.map_document("HR-Policies")
        assert "hr" in tags
        assert "leadership" in tags
        assert "admin" in tags
        assert "all-staff" in tags

    def test_hr_prefix_matches(self, default_mapper):
        tags = default_mapper.map_document("HR Documents")
        assert "hr" in tags

    def test_finance_library_matches(self, default_mapper):
        tags = default_mapper.map_document("Finance")
        assert "finance" in tags
        assert "leadership" in tags

    def test_bd_library_matches(self, default_mapper):
        tags = default_mapper.map_document("BD-Captures")
        assert "bd" in tags
        assert "capture" in tags

    def test_engineering_library_matches(self, default_mapper):
        tags = default_mapper.map_document("Engineering")
        assert "engineering" in tags
        assert "tech-leads" in tags

    def test_contracts_library_matches(self, default_mapper):
        tags = default_mapper.map_document("Contracts")
        assert "contracts" in tags
        assert "leadership" in tags

    def test_unknown_library_gets_default(self, default_mapper):
        tags = default_mapper.map_document("Random-Library")
        assert tags == ["all-staff"]

    def test_wildcard_always_appended(self, default_mapper):
        tags = default_mapper.map_document("HR-Policies")
        assert "all-staff" in tags

    def test_first_match_wins(self, custom_rules):
        mapper = custom_rules("""\
            rules:
              - library_pattern: "HR*"
                access_tags: ["hr-specific"]
              - library_pattern: "HR-*"
                access_tags: ["hr-dash"]
              - library_pattern: "*"
                access_tags: ["base"]
        """)
        tags = mapper.map_document("HR-Policies")
        assert "hr-specific" in tags
        assert "hr-dash" not in tags
        assert "base" in tags

    def test_returns_sorted_deduplicated(self, custom_rules):
        mapper = custom_rules("""\
            rules:
              - library_pattern: "Dup*"
                access_tags: ["c", "a", "b", "a"]
              - library_pattern: "*"
                access_tags: ["a", "d"]
        """)
        tags = mapper.map_document("Dup-Lib")
        assert tags == sorted(set(tags))

    def test_sp_path_accepted_but_not_used_yet(self, default_mapper):
        tags_without = default_mapper.map_document("HR-Policies")
        tags_with = default_mapper.map_document("HR-Policies", "/subfolder/doc.pdf")
        assert tags_without == tags_with

    def test_no_rules_returns_empty(self, custom_rules):
        mapper = custom_rules("""\
            rules: []
        """)
        assert mapper.map_document("Anything") == []

    def test_only_wildcard_rule(self, custom_rules):
        mapper = custom_rules("""\
            rules:
              - library_pattern: "*"
                access_tags: ["everyone"]
        """)
        assert mapper.map_document("Any-Library") == ["everyone"]

    def test_case_sensitive_matching(self, custom_rules):
        mapper = custom_rules("""\
            rules:
              - library_pattern: "HR*"
                access_tags: ["hr"]
              - library_pattern: "*"
                access_tags: ["base"]
        """)
        # "hr-policies" lowercase should NOT match "HR*"
        tags = mapper.map_document("hr-policies")
        assert "hr" not in tags
        assert tags == ["base"]

    def test_question_mark_pattern(self, custom_rules):
        mapper = custom_rules("""\
            rules:
              - library_pattern: "BD-?"
                access_tags: ["bd-single"]
              - library_pattern: "*"
                access_tags: ["base"]
        """)
        assert "bd-single" in mapper.map_document("BD-A")
        assert "bd-single" not in mapper.map_document("BD-AB")

    def test_bracket_pattern(self, custom_rules):
        mapper = custom_rules("""\
            rules:
              - library_pattern: "[HF]*"
                access_tags: ["hf"]
              - library_pattern: "*"
                access_tags: ["base"]
        """)
        assert "hf" in mapper.map_document("HR")
        assert "hf" in mapper.map_document("Finance")
        assert "hf" not in mapper.map_document("Engineering")


# ===================================================================
# get_tags_for_user_role
# ===================================================================

class TestGetTagsForUserRole:
    def test_admin_gets_all_tags(self, default_mapper):
        tags = default_mapper.get_tags_for_user_role("admin")
        # Admin should have every tag from every rule
        assert "hr" in tags
        assert "finance" in tags
        assert "engineering" in tags
        assert "bd" in tags
        assert "contracts" in tags
        assert "all-staff" in tags
        assert "leadership" in tags

    def test_engineer_role(self, default_mapper):
        tags = default_mapper.get_tags_for_user_role("engineer")
        assert "engineering" in tags
        assert "all-staff" in tags
        assert "hr" not in tags
        assert "finance" not in tags

    def test_hr_role(self, default_mapper):
        tags = default_mapper.get_tags_for_user_role("hr")
        assert "hr" in tags
        assert "all-staff" in tags
        assert "engineering" not in tags

    def test_finance_role(self, default_mapper):
        tags = default_mapper.get_tags_for_user_role("finance")
        assert "finance" in tags
        assert "all-staff" in tags

    def test_leadership_role(self, default_mapper):
        tags = default_mapper.get_tags_for_user_role("leadership")
        assert "leadership" in tags
        assert "all-staff" in tags

    def test_tech_lead_role(self, default_mapper):
        tags = default_mapper.get_tags_for_user_role("tech-lead")
        assert "engineering" in tags
        assert "tech-leads" in tags
        assert "all-staff" in tags

    def test_bd_role(self, default_mapper):
        tags = default_mapper.get_tags_for_user_role("bd")
        assert "bd" in tags
        assert "capture" in tags
        assert "all-staff" in tags

    def test_contracts_role(self, default_mapper):
        tags = default_mapper.get_tags_for_user_role("contracts")
        assert "contracts" in tags
        assert "all-staff" in tags

    def test_staff_role(self, default_mapper):
        tags = default_mapper.get_tags_for_user_role("staff")
        assert tags == ["all-staff"]

    def test_unknown_role_returns_empty(self, default_mapper):
        tags = default_mapper.get_tags_for_user_role("nonexistent-role")
        assert tags == []

    def test_returns_sorted(self, default_mapper):
        tags = default_mapper.get_tags_for_user_role("admin")
        assert tags == sorted(tags)

    def test_custom_role_mappings(self, custom_rules):
        mapper = custom_rules("""\
            rules:
              - library_pattern: "Secret*"
                access_tags: ["secret"]
              - library_pattern: "*"
                access_tags: ["public"]
            role_mappings:
              spy: ["secret", "public"]
              guest: ["public"]
              superadmin: ["*"]
        """)
        assert mapper.get_tags_for_user_role("spy") == ["public", "secret"]
        assert mapper.get_tags_for_user_role("guest") == ["public"]
        # Wildcard "*" in mapping means all known tags
        assert mapper.get_tags_for_user_role("superadmin") == ["public", "secret"]


# ===================================================================
# Integration with map_document + role check
# ===================================================================

class TestAccessControlIntegration:
    def test_hr_doc_visible_to_hr_role(self, default_mapper):
        doc_tags = default_mapper.map_document("HR-Policies")
        role_tags = default_mapper.get_tags_for_user_role("hr")
        # There should be at least one overlapping tag
        assert set(doc_tags) & set(role_tags)

    def test_hr_doc_not_visible_to_engineer(self, default_mapper):
        doc_tags = default_mapper.map_document("HR-Policies")
        role_tags = default_mapper.get_tags_for_user_role("engineer")
        # The only overlap should be "all-staff"
        overlap = set(doc_tags) & set(role_tags)
        assert overlap == {"all-staff"}

    def test_engineering_doc_visible_to_tech_lead(self, default_mapper):
        doc_tags = default_mapper.map_document("Engineering")
        role_tags = default_mapper.get_tags_for_user_role("tech-lead")
        assert set(doc_tags) & set(role_tags)

    def test_admin_sees_everything(self, default_mapper):
        admin_tags = set(default_mapper.get_tags_for_user_role("admin"))
        for lib in ["HR-Policies", "Finance", "BD-Captures", "Engineering", "Contracts", "Random"]:
            doc_tags = set(default_mapper.map_document(lib))
            assert doc_tags.issubset(admin_tags), f"Admin can't see {lib}: {doc_tags - admin_tags}"

    def test_staff_only_sees_default_docs(self, default_mapper):
        staff_tags = set(default_mapper.get_tags_for_user_role("staff"))
        # Staff can see documents from an unknown library (all-staff)
        unknown_tags = set(default_mapper.map_document("General-Docs"))
        assert unknown_tags & staff_tags

        # But staff should NOT have hr, finance, etc.
        assert "hr" not in staff_tags
        assert "finance" not in staff_tags
