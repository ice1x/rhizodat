"""An embedder importable by module path (``embedders_fixture:Fixed``) for the tests."""


class Fixed:
    """Every text on the same unit vector: equal scores, a known model name."""

    model = "fixed-1"
    dimensions = 2

    def __init__(self) -> None:
        self.calls = 0

    def embed(self, texts: list[str]) -> list[list[float]]:
        self.calls += 1
        return [[1.0, 0.0] for _ in texts]


def not_an_embedder() -> int:
    return 1
