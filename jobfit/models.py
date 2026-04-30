from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class Job:
    company: str
    title: str
    location: str
    url: str
    description: str = ""
    source: str = ""
    posted_at: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScoreResult:
    score: int
    recommendation: str
    priority: str
    reasons: List[str]
    resume_keywords: List[str]
    risks: List[str]
    summary: str = ""
