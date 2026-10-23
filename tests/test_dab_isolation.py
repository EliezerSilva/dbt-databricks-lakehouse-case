import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS = ROOT / "notebooks"

SCHEMAS = ("bronze", "silver", "gold", "control")

class TestDabIsolation(unittest.TestCase):

    def test_all_medallion_notebooks_use_setup(self):
        paths = [
            *NOTEBOOKS.glob("bronze/*.py"),
            *NOTEBOOKS.glob("silver/*.py"),
            *NOTEBOOKS.glob("gold/*.py"),
        ]
        self.assertEqual(len(paths), 12)

        for path in paths:
            with self.subTest(notebook=path.name):
                content = path.read_text()
                self.assertIn(
                    "# MAGIC %run ../00_setup",
                    content
                )

    def test_no_fixed_workspace_schema_references(self):
        import ast

        paths = [
            NOTEBOOKS / "00_setup.py",
            *NOTEBOOKS.glob("bronze/*.py"),
            *NOTEBOOKS.glob("silver/*.py"),
            *NOTEBOOKS.glob("gold/*.py"),
            *[
                NOTEBOOKS / "troubleshooting" / name
                for name in (
                    "99_data_quality_checks.py",
                    "90_evidencias_validacao.py",
                    "91_evidencias_orchestration.py",
                )
            ],
        ]

        pattern = re.compile(
            r"(?<![\\w.])(?:workspace\\.)?"
            r"(?:bronze|silver|gold|control)\\."
            r"[A-Za-z_][A-Za-z0-9_]*\\b"
        )

        violations = []

        for path in paths:
            content = path.read_text(encoding="utf-8")
            tree = ast.parse(content, filename=str(path))
            ignored = set()

            # Ignore only actual Python docstring nodes.
            for node in ast.walk(tree):
                if isinstance(
                    node,
                    (ast.Module, ast.FunctionDef,
                     ast.AsyncFunctionDef, ast.ClassDef),
                ):
                    if node.body and isinstance(node.body[0], ast.Expr):
                        expr = node.body[0].value
                        if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
                            ignored.update(
                                range(node.body[0].lineno,
                                      node.body[0].end_lineno + 1)
                            )

            # Ignore only plain literal messages passed to print.
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "print"
                    and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and isinstance(node.args[0].value, str)
                ):
                    ignored.update(
                        range(node.lineno, node.end_lineno + 1)
                    )

            for number, line in enumerate(content.splitlines(), 1):
                if number in ignored:
                    continue
                if pattern.search(line):
                    violations.append(
                        f"{path}:{number}: {line.strip()}"
                    )

        self.assertFalse(violations, "\\n".join(violations))

    def test_dev_schema_parameters_exist(self):
        content = (ROOT / "databricks.yml").read_text()

        for schema in SCHEMAS:
            self.assertIn(
                f"{schema}_schema:",
                content
            )
            self.assertIn(
                f"default: {schema}_dev",
                content
            )

if __name__ == "__main__":
    unittest.main()
