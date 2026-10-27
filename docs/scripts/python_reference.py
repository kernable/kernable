"""Prints the Python SDK reference (Markdown) from the sources' docstrings and `#:` comments,
for the names in `kernable.__all__`. Reads the sources only; nothing is imported."""

import ast
import sys
from pathlib import Path

package = Path(sys.argv[1])
modules = {p.stem: (p.read_text(), ast.parse(p.read_text())) for p in package.glob("*.py")}
init_src, init = modules["__init__"]
exported = next(ast.literal_eval(n.value) for n in init.body
                if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == "__all__")


def comments_before(src: str, line: int) -> str:
    lines = src.splitlines()[: line - 1]
    out = []
    for l in reversed(lines):
        if l.strip().startswith("#:"):
            out.insert(0, l.strip()[2:].strip())
        else:
            break
    return " ".join(out)


def signature(fn: ast.FunctionDef) -> str:
    args = ast.unparse(fn.args)
    returns = f" -> {ast.unparse(fn.returns)}" if fn.returns else ""
    return f"{fn.name}({args}){returns}".replace("self, ", "").replace("(self)", "()")


def one_line(doc: str | None) -> str:
    return " ".join((doc or "").split())


out = ["# Python SDK", "",
       "`pip install kernable` (Python ≥ 3.10). Generated from the docstrings in `sdks/python/kernable`.", ""]
for name in exported:
    for src, tree in modules.values():
        node = next((n for n in tree.body if getattr(n, "name", None) == name), None)
        if node is None and name == "PROTOCOL":
            node = next((n for n in tree.body if isinstance(n, ast.Assign)
                         and getattr(n.targets[0], "id", "") == "PROTOCOL"), None)
            if node:
                out += [f"### {name}", "", f"`{name} = {ast.unparse(node.value)}`: {comments_before(src, node.lineno)}", ""]
                break
        if node is None:
            continue
        if isinstance(node, ast.ClassDef):
            out += [f"### {name}", "", one_line(ast.get_docstring(node)), ""]
            for item in node.body:
                if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name) and not item.target.id.startswith("_"):
                    note = comments_before(src, item.lineno)
                    out.append(f"- `{item.target.id}: {ast.unparse(item.annotation)}`" + (f": {note}" if note else ""))
                elif isinstance(item, ast.FunctionDef) and (not item.name.startswith("_") or item.name == "__init__"):
                    doc = one_line(ast.get_docstring(item))
                    sig = signature(item).replace("__init__", name)
                    out.append(f"- `{sig}`" + (f": {doc}" if doc else ""))
            out.append("")
        elif isinstance(node, ast.FunctionDef):
            out += [f"### {name}()", "", f"`{signature(node)}`", "", one_line(ast.get_docstring(node)), ""]
        break
print("\n".join(out))
