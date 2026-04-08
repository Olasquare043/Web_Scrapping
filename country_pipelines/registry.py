from __future__ import annotations

from dataclasses import dataclass

from country_pipelines.official_country_pipeline import (
    CountryPipelineConfig,
    build_auto_country_config,
)


@dataclass(frozen=True)
class CountryPipeline:
    country: str
    config: CountryPipelineConfig


def normalize_country_name(country: str) -> str:
    return " ".join(country.strip().replace("_", " ").replace("-", " ").split())


def get_country_pipeline(country: str) -> CountryPipeline:
    normalized = normalize_country_name(country)
    if not normalized:
        raise KeyError("Country name cannot be empty.")
    return CountryPipeline(
        country=normalized,
        config=build_auto_country_config(normalized),
    )
