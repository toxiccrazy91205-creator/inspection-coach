# api/models.py
from typing import List, Optional

try:
    from pydantic import BaseModel, ConfigDict, Field
    _HAS_CONFIGDICT = True
except Exception:
    from pydantic import BaseModel, Field  # type: ignore
    _HAS_CONFIGDICT = False


class ViolationProb(BaseModel):
    code: str = Field(description="NYC violation code (e.g. '04L')")
    probability: float = Field(description="Estimated probability this violation recurs on the next inspection (0–1)")
    label: str = Field(description="Human-readable description of the violation")


class LastViolation(BaseModel):
    code: str = Field(description="NYC violation code (e.g. '04L')")
    description: str = Field(description="Full text description of the violation as recorded by the inspector")
    critical: bool = Field(description="True if the violation was flagged Critical by the inspector")


class CamisSuggestion(BaseModel):
    camis: str
    name: str
    last_inspection_date: Optional[str]


class ScoreRequest(BaseModel):
    camis: str = Field(description="NYC CAMIS identifier — the unique ID assigned to each restaurant by the city")


class ScoreResponse(BaseModel):
    camis: str = Field(description="NYC CAMIS identifier")
    prob_bc: float = Field(description="Predicted probability (0–1) of receiving a B or C grade on the next inspection")
    predicted_points: Optional[float] = Field(default=None, description="Predicted violation point total for the next inspection")
    top_reasons: List[str] = Field(default=[], description="Plain-language factors that drove the risk score")
    top_violation_probs: List[ViolationProb] = Field(default=[], description="Violation categories most likely to recur, based on inspection history")
    model_version: Optional[str] = Field(default=None, description="Identifier for the scoring model or heuristic version used")
    data_version: Optional[str] = Field(default=None, description="Source of the data used ('runtime' = live-refreshed, 'baked' = image fallback)")
    last_inspection_date: Optional[str] = Field(default=None, description="Date of the most recent recorded inspection (YYYY-MM-DD)")
    last_points: Optional[float] = Field(default=None, description="Violation point total from the most recent inspection")
    last_grade: Optional[str] = Field(default=None, description="Letter grade from the most recent inspection (A, B, or C), inferred from points if not explicitly recorded")
    rat_index: Optional[float] = Field(default=None, description="Composite rat pressure score (0–1) based on nearby rat 311 complaints and failed city rat inspections")
    pest_index: Optional[float] = Field(default=None, description="Composite pest pressure score (0–1) blending rat sightings, mouse sightings, indoor pest complaints, and DOHMH rat inspection failures within ~150–200m")
    rat311_cnt_180d_k1: Optional[int] = Field(default=None, description="Number of 311 rat sighting complaints within ~150–200m in the last 180 days")
    mouse311_cnt_180d_k1: Optional[int] = Field(default=None, description="Number of 311 mouse sighting complaints within ~150–200m in the last 180 days")
    pest311_cnt_180d_k1: Optional[int] = Field(default=None, description="Number of 311 indoor pest/cockroach complaints (UNSANITARY CONDITION) within ~150–200m in the last 180 days")
    ratinsp_fail_365d_k1: Optional[int] = Field(default=None, description="Number of failed DOHMH rat inspections at properties within ~150–200m in the last 365 days")
    latitude: Optional[float] = Field(default=None, description="Latitude of the restaurant (WGS84)")
    longitude: Optional[float] = Field(default=None, description="Longitude of the restaurant (WGS84)")
    score_history: List[List] = Field(default=[], description="Chronological list of [date, points] pairs across all recorded inspections, suitable for rendering a trend chart")
    last_violations: List[LastViolation] = Field(default=[], description="All violations cited at the most recent inspection, sorted critical-first then by violation code")
    suggested_camis: List[CamisSuggestion] = Field(default=[], description="Other CAMISes found at the same address with more recent inspections — likely the current operator after an ownership change")

    if _HAS_CONFIGDICT:
        model_config = ConfigDict(protected_namespaces=())
    else:
        class Config:  # type: ignore
            protected_namespaces = ()
