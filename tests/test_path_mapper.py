"""Tests for PathMapper – SharePoint path ↔ S3 key translation."""

import sys

import pytest

sys.path.insert(0, "src")

from utils.path_mapper import PathMapper, _sanitize_component, _sanitize_path


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def mapper():
    return PathMapper(
        bucket="dynamo-ai-documents",
        source_prefix="source",
        extracted_prefix="extracted",
    )


# ===================================================================
# _sanitize_component (internal helper)
# ===================================================================

class TestSanitizeComponent:
    def test_replaces_spaces_with_hyphens(self):
        assert _sanitize_component("Employee Handbook") == "Employee-Handbook"

    def test_removes_special_characters(self):
        assert _sanitize_component("file (1) [copy].pdf") == "file-1-copy.pdf"

    def test_preserves_hyphens_underscores_periods(self):
        assert _sanitize_component("my-file_v2.pdf") == "my-file_v2.pdf"

    def test_preserves_original_casing(self):
        assert _sanitize_component("HR-Policies") == "HR-Policies"
        assert _sanitize_component("QuarterlyReport") == "QuarterlyReport"

    def test_collapses_consecutive_hyphens(self):
        assert _sanitize_component("a - - b") == "a-b"

    def test_handles_ampersand_and_at(self):
        assert _sanitize_component("R&D @ HQ") == "RD-HQ"

    def test_handles_empty_string(self):
        assert _sanitize_component("") == ""

    def test_unicode_letters_preserved(self):
        # \w matches Unicode word chars, so accented letters stay
        assert _sanitize_component("café-résumé") == "café-résumé"

    def test_unicode_cjk_preserved(self):
        assert _sanitize_component("報告書") == "報告書"

    def test_emoji_stripped(self):
        # Emoji are not \w, so they get removed
        result = _sanitize_component("docs 📄 here")
        assert "📄" not in result
        assert "docs" in result


# ===================================================================
# _sanitize_path
# ===================================================================

class TestSanitizePath:
    def test_strips_leading_and_trailing_slashes(self):
        assert _sanitize_path("/foo/bar/") == "foo/bar"

    def test_collapses_double_slashes(self):
        assert _sanitize_path("a//b///c") == "a/b/c"

    def test_sanitizes_each_component(self):
        assert _sanitize_path("/My Folder/File (1).pdf") == "My-Folder/File-1.pdf"

    def test_empty_string(self):
        assert _sanitize_path("") == ""

    def test_single_slash(self):
        assert _sanitize_path("/") == ""

    def test_deeply_nested(self):
        result = _sanitize_path("/a/b/c/d/e/f/g.txt")
        assert result == "a/b/c/d/e/f/g.txt"


# ===================================================================
# to_s3_source_key
# ===================================================================

class TestToS3SourceKey:
    def test_spec_example(self, mapper):
        key = mapper.to_s3_source_key(
            "Dynamo", "HR-Policies", "/2025/Employee-Handbook.docx"
        )
        assert key == "source/Dynamo/HR-Policies/2025/Employee-Handbook.docx"

    def test_multiple_folder_levels(self, mapper):
        key = mapper.to_s3_source_key(
            "Dynamo", "Documents", "/Legal/Contracts/2024/NDA-Template.pdf"
        )
        assert key == "source/Dynamo/Documents/Legal/Contracts/2024/NDA-Template.pdf"

    def test_root_level_file(self, mapper):
        key = mapper.to_s3_source_key("Dynamo", "Documents", "README.txt")
        assert key == "source/Dynamo/Documents/README.txt"

    def test_spaces_become_hyphens(self, mapper):
        key = mapper.to_s3_source_key(
            "Dynamo", "Shared Documents", "/My Reports/Annual Report.docx"
        )
        assert key == "source/Dynamo/Shared-Documents/My-Reports/Annual-Report.docx"

    def test_special_characters_removed(self, mapper):
        key = mapper.to_s3_source_key(
            "Dynamo", "Docs", "/Budget (Final) [v2].xlsx"
        )
        assert key == "source/Dynamo/Docs/Budget-Final-v2.xlsx"

    def test_preserves_casing(self, mapper):
        key = mapper.to_s3_source_key("Dynamo", "HR-Policies", "/Q4-Report.PDF")
        assert key == "source/Dynamo/HR-Policies/Q4-Report.PDF"

    def test_collapses_double_slashes_in_path(self, mapper):
        key = mapper.to_s3_source_key("Dynamo", "Docs", "//folder///file.pdf")
        assert key == "source/Dynamo/Docs/folder/file.pdf"

    def test_strips_slashes_from_path(self, mapper):
        key = mapper.to_s3_source_key("Dynamo", "Docs", "/leading/trailing/")
        # "leading/trailing" has no file — it's a path to a directory
        assert key == "source/Dynamo/Docs/leading/trailing"

    def test_empty_path_gives_site_and_library_only(self, mapper):
        key = mapper.to_s3_source_key("Dynamo", "Docs", "")
        assert key == "source/Dynamo/Docs"

    def test_unicode_in_filename(self, mapper):
        key = mapper.to_s3_source_key("Dynamo", "Docs", "/報告書.pdf")
        assert key == "source/Dynamo/Docs/報告書.pdf"

    def test_unicode_accented_characters(self, mapper):
        key = mapper.to_s3_source_key("Dynamo", "Docs", "/résumé-café.docx")
        assert key == "source/Dynamo/Docs/résumé-café.docx"

    def test_file_with_no_extension(self, mapper):
        key = mapper.to_s3_source_key("Dynamo", "Docs", "/Makefile")
        assert key == "source/Dynamo/Docs/Makefile"

    def test_file_with_multiple_dots(self, mapper):
        key = mapper.to_s3_source_key("Dynamo", "Docs", "/archive.tar.gz")
        assert key == "source/Dynamo/Docs/archive.tar.gz"

    def test_library_name_with_spaces(self, mapper):
        key = mapper.to_s3_source_key("Dynamo", "Site Assets", "/logo.png")
        assert key == "source/Dynamo/Site-Assets/logo.png"

    def test_site_name_with_spaces(self, mapper):
        key = mapper.to_s3_source_key("My Company", "Docs", "/file.pdf")
        assert key == "source/My-Company/Docs/file.pdf"

    def test_very_long_path_truncated(self, mapper):
        """Paths exceeding S3's 1024-byte key limit are truncated."""
        long_folder = "a" * 100
        # Build a path that will exceed 1024 bytes
        deep_path = "/".join([long_folder] * 12) + "/file.pdf"
        key = mapper.to_s3_source_key("Dynamo", "Docs", deep_path)
        assert len(key.encode("utf-8")) <= 1024

    def test_custom_source_prefix(self):
        m = PathMapper("bucket", source_prefix="raw", extracted_prefix="out")
        key = m.to_s3_source_key("S", "L", "/file.pdf")
        assert key.startswith("raw/")


# ===================================================================
# to_s3_extracted_key
# ===================================================================

class TestToS3ExtractedKey:
    def test_spec_example(self, mapper):
        result = mapper.to_s3_extracted_key(
            "source/Dynamo/HR-Policies/2025/Employee-Handbook.docx"
        )
        assert result == "extracted/Dynamo/HR-Policies/2025/Employee-Handbook.json"

    def test_pdf_to_json(self, mapper):
        result = mapper.to_s3_extracted_key("source/Dynamo/Docs/report.pdf")
        assert result == "extracted/Dynamo/Docs/report.json"

    def test_pptx_to_json(self, mapper):
        result = mapper.to_s3_extracted_key("source/Dynamo/Docs/slides.pptx")
        assert result == "extracted/Dynamo/Docs/slides.json"

    def test_xlsx_to_json(self, mapper):
        result = mapper.to_s3_extracted_key("source/Dynamo/Docs/data.xlsx")
        assert result == "extracted/Dynamo/Docs/data.json"

    def test_file_with_no_extension(self, mapper):
        result = mapper.to_s3_extracted_key("source/Dynamo/Docs/Makefile")
        assert result == "extracted/Dynamo/Docs/Makefile.json"

    def test_file_with_multiple_dots(self, mapper):
        result = mapper.to_s3_extracted_key("source/Dynamo/Docs/file.backup.pdf")
        assert result == "extracted/Dynamo/Docs/file.backup.json"

    def test_preserves_nested_path(self, mapper):
        result = mapper.to_s3_extracted_key("source/Dynamo/D/a/b/c/d.docx")
        assert result == "extracted/Dynamo/D/a/b/c/d.json"

    def test_key_without_source_prefix(self, mapper):
        """Handles keys that don't start with the source prefix."""
        result = mapper.to_s3_extracted_key("Dynamo/Docs/file.pdf")
        assert result == "extracted/Dynamo/Docs/file.json"

    def test_custom_prefixes(self):
        m = PathMapper("bucket", source_prefix="raw", extracted_prefix="processed")
        result = m.to_s3_extracted_key("raw/S/L/file.pdf")
        assert result == "processed/S/L/file.json"


# ===================================================================
# source_key_to_sharepoint_path
# ===================================================================

class TestSourceKeyToSharepointPath:
    def test_spec_example(self, mapper):
        site, lib, path = mapper.source_key_to_sharepoint_path(
            "source/Dynamo/HR-Policies/2025/Employee-Handbook.docx"
        )
        assert site == "Dynamo"
        assert lib == "HR-Policies"
        assert path == "2025/Employee-Handbook.docx"

    def test_deeply_nested(self, mapper):
        site, lib, path = mapper.source_key_to_sharepoint_path(
            "source/Dynamo/Documents/Legal/Contracts/2024/NDA.pdf"
        )
        assert site == "Dynamo"
        assert lib == "Documents"
        assert path == "Legal/Contracts/2024/NDA.pdf"

    def test_root_level_file(self, mapper):
        site, lib, path = mapper.source_key_to_sharepoint_path(
            "source/Dynamo/Documents/readme.txt"
        )
        assert site == "Dynamo"
        assert lib == "Documents"
        assert path == "readme.txt"

    def test_file_at_library_root_no_path(self, mapper):
        """Key with only site and library (no file) returns empty path."""
        site, lib, path = mapper.source_key_to_sharepoint_path(
            "source/Dynamo/Documents"
        )
        assert site == "Dynamo"
        assert lib == "Documents"
        assert path == ""

    def test_raises_on_too_few_segments(self, mapper):
        with pytest.raises(ValueError, match="Cannot parse"):
            mapper.source_key_to_sharepoint_path("source/only-one")

    def test_raises_on_bare_prefix(self, mapper):
        with pytest.raises(ValueError, match="Cannot parse"):
            mapper.source_key_to_sharepoint_path("source/")

    def test_handles_key_without_prefix(self, mapper):
        """Keys not starting with source_prefix/ are parsed from position 0."""
        site, lib, path = mapper.source_key_to_sharepoint_path(
            "Dynamo/Documents/file.pdf"
        )
        assert site == "Dynamo"
        assert lib == "Documents"
        assert path == "file.pdf"

    def test_roundtrip_with_to_s3_source_key(self, mapper):
        """to_s3_source_key -> source_key_to_sharepoint_path is consistent."""
        key = mapper.to_s3_source_key("Dynamo", "HR", "/2025/Handbook.docx")
        site, lib, path = mapper.source_key_to_sharepoint_path(key)
        assert site == "Dynamo"
        assert lib == "HR"
        assert path == "2025/Handbook.docx"

    def test_roundtrip_with_unicode(self, mapper):
        key = mapper.to_s3_source_key("Dynamo", "Docs", "/報告/résumé.pdf")
        site, lib, path = mapper.source_key_to_sharepoint_path(key)
        assert site == "Dynamo"
        assert lib == "Docs"
        assert "résumé.pdf" in path


# ===================================================================
# build_s3_tags
# ===================================================================

class TestBuildS3Tags:
    def test_all_seven_tags_present(self):
        item = {
            "site_name": "Dynamo",
            "library_name": "HR-Policies",
            "sharepoint_path": "/2025/Employee-Handbook.docx",
            "author": "Alice Smith",
            "last_modified": "2024-06-15T10:30:00Z",
            "content_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "file_type": ".docx",
        }
        tags = PathMapper.build_s3_tags(item)
        assert tags["sp-site"] == "Dynamo"
        assert tags["sp-library"] == "HR-Policies"
        assert tags["sp-author"] == "Alice Smith"
        assert tags["sp-last-modified"] == "2024-06-15T10:30:00Z"
        assert tags["file-type"] == "docx"  # leading dot stripped
        assert len(tags) == 7

    def test_path_is_url_encoded(self):
        item = {
            "site_name": "S",
            "library_name": "L",
            "sharepoint_path": "/My Folder/File Name.pdf",
            "author": "A",
            "last_modified": "2024-01-01",
            "content_type": "application/pdf",
            "file_type": ".pdf",
        }
        tags = PathMapper.build_s3_tags(item)
        assert tags["sp-path"] == "/My%20Folder/File%20Name.pdf"

    def test_long_path_truncated_to_256_chars(self):
        long_path = "/" + "a" * 300 + "/file.pdf"
        item = {
            "site_name": "S",
            "library_name": "L",
            "sharepoint_path": long_path,
            "author": "A",
            "last_modified": "2024-01-01",
            "content_type": "application/pdf",
            "file_type": ".pdf",
        }
        tags = PathMapper.build_s3_tags(item)
        assert len(tags["sp-path"]) <= 256
        assert tags["sp-path"].endswith("...")

    def test_empty_values_omitted(self):
        item = {
            "site_name": "Dynamo",
            "library_name": "",
            "sharepoint_path": "",
            "author": "",
            "last_modified": "",
            "content_type": "",
            "file_type": "",
        }
        tags = PathMapper.build_s3_tags(item)
        assert "sp-site" in tags
        # Empty strings are dropped
        assert "sp-library" not in tags
        assert "sp-path" not in tags
        assert "sp-author" not in tags

    def test_missing_keys_handled(self):
        """build_s3_tags doesn't crash on an empty dict."""
        tags = PathMapper.build_s3_tags({})
        assert tags == {}

    def test_tag_count_within_s3_limit(self):
        item = {
            "site_name": "S",
            "library_name": "L",
            "sharepoint_path": "/f.pdf",
            "author": "A",
            "last_modified": "2024-01-01",
            "content_type": "application/pdf",
            "file_type": ".pdf",
        }
        tags = PathMapper.build_s3_tags(item)
        assert len(tags) <= 10  # S3 limit

    def test_all_tag_values_within_256_chars(self):
        item = {
            "site_name": "S" * 300,
            "library_name": "L" * 300,
            "sharepoint_path": "/" + "p" * 300,
            "author": "A" * 300,
            "last_modified": "T" * 300,
            "content_type": "C" * 300,
            "file_type": "." + "x" * 300,
        }
        tags = PathMapper.build_s3_tags(item)
        for key, value in tags.items():
            assert len(value) <= 256, f"Tag '{key}' exceeds 256 chars: {len(value)}"

    def test_file_type_dot_stripped(self):
        item = {
            "site_name": "S",
            "file_type": ".pptx",
        }
        tags = PathMapper.build_s3_tags(item)
        assert tags["file-type"] == "pptx"

    def test_unicode_path_encoded(self):
        item = {
            "site_name": "S",
            "sharepoint_path": "/docs/報告書.pdf",
            "file_type": ".pdf",
        }
        tags = PathMapper.build_s3_tags(item)
        # Unicode chars get percent-encoded
        assert "%E5%A0%B1" in tags["sp-path"]  # 報 encoded


# ===================================================================
# Edge cases & integration
# ===================================================================

class TestEdgeCases:
    def test_only_slashes_path(self, mapper):
        key = mapper.to_s3_source_key("Dynamo", "Docs", "///")
        assert key == "source/Dynamo/Docs"

    def test_path_with_only_special_chars(self, mapper):
        key = mapper.to_s3_source_key("Dynamo", "Docs", "/!@#$%^&*()")
        # All special chars removed, hyphens collapsed
        assert key == "source/Dynamo/Docs" or key.startswith("source/Dynamo/Docs/")

    def test_dot_files(self, mapper):
        key = mapper.to_s3_source_key("Dynamo", "Docs", "/.hidden")
        assert key == "source/Dynamo/Docs/.hidden"

    def test_extracted_then_reverse_is_consistent(self, mapper):
        source = "source/Dynamo/HR/2025/file.docx"
        extracted = mapper.to_s3_extracted_key(source)
        assert extracted == "extracted/Dynamo/HR/2025/file.json"

        # Reverse the source key
        site, lib, path = mapper.source_key_to_sharepoint_path(source)
        assert site == "Dynamo"
        assert lib == "HR"
        assert path == "2025/file.docx"

    def test_very_long_unicode_path(self, mapper):
        """Long path with multi-byte Unicode stays within 1024 bytes."""
        # Each CJK char is 3 bytes in UTF-8
        long_path = "/報告/" * 100 + "file.pdf"
        key = mapper.to_s3_source_key("Dynamo", "Docs", long_path)
        assert len(key.encode("utf-8")) <= 1024

    def test_custom_bucket_stored(self):
        m = PathMapper(bucket="my-bucket", source_prefix="src", extracted_prefix="ext")
        assert m.bucket == "my-bucket"
        assert m.source_prefix == "src"
        assert m.extracted_prefix == "ext"
