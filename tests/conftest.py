"""Root conftest — session-scoped fixtures for test file generation."""

import pytest
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def fixtures_dir():
    """Return the path to the fixtures directory."""
    FIXTURES_DIR.mkdir(exist_ok=True)
    return FIXTURES_DIR


@pytest.fixture(scope="session")
def sample_pdf(fixtures_dir):
    """Generate (or reuse) a sample PDF fixture."""
    from tests.fixtures.generate_fixtures import generate_pdf
    return generate_pdf(fixtures_dir / "sample.pdf")


@pytest.fixture(scope="session")
def sample_docx(fixtures_dir):
    """Generate (or reuse) a sample DOCX fixture."""
    from tests.fixtures.generate_fixtures import generate_docx
    return generate_docx(fixtures_dir / "sample.docx")


@pytest.fixture(scope="session")
def sample_pptx(fixtures_dir):
    """Generate (or reuse) a sample PPTX fixture."""
    from tests.fixtures.generate_fixtures import generate_pptx
    return generate_pptx(fixtures_dir / "sample.pptx")


@pytest.fixture(scope="session")
def sample_xlsx(fixtures_dir):
    """Generate (or reuse) a sample XLSX fixture."""
    from tests.fixtures.generate_fixtures import generate_xlsx
    return generate_xlsx(fixtures_dir / "sample.xlsx")
