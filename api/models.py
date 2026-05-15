# api/models.py
"""Pydantic schemas for Health Inspection Coach — Ahmedabad."""
from typing import List, Optional

try:
    from pydantic import BaseModel, ConfigDict, Field
    _HAS_CONFIGDICT = True
except Exception:
    from pydantic import BaseModel, Field  # type: ignore
    _HAS_CONFIGDICT = False


class ViolationProb(BaseModel):
    code: str = Field(description="FSSAI Schedule 4 clause (e.g. 'Sch4.2.1')")
    probability: float = Field(description="Estimated probability this violation is flagged on next inspection (0–1)")
    label: str = Field(description="Human-readable description of the FSSAI violation")


class NearbyRiskFactor(BaseModel):
    name: str = Field(description="Name of the nearby place (e.g. 'Kalupur Bus Stand')")
    type: str = Field(description="Google Places type (marketplace, bus_station, transit_station, general_contractor)")
    distance_m: Optional[float] = Field(default=None, description="Approximate distance in metres from the restaurant")


class ScoreRequest(BaseModel):
    fssai_id: str = Field(description="FSSAI license number — unique identifier for each food business in India")


class ScoreResponse(BaseModel):
    fssai_id: str = Field(description="FSSAI license number")
    name: str = Field(description="Restaurant name")
    area: str = Field(description="Ahmedabad area / locality")
    prob_fssai_fail: float = Field(description="Predicted probability (0–1) of failing the next FSSAI inspection")
    predicted_points: Optional[float] = Field(default=None, description="Predicted violation point total for the next inspection")
    top_reasons: List[str] = Field(default=[], description="Plain-language factors that drove the risk score")
    top_violation_probs: List[ViolationProb] = Field(default=[], description="FSSAI Schedule 4 violation categories most likely to be flagged")
    environmental_index: float = Field(default=0.0, description="0–1 index of nearby environmental risk factors (markets, transit, construction within 200m)")
    nearby_risk_factors: List[NearbyRiskFactor] = Field(default=[], description="Nearby places that contribute to environmental risk")
    model_version: Optional[str] = Field(default=None, description="Identifier for the scoring model or heuristic version used")
    data_version: Optional[str] = Field(default=None, description="Source of the data used ('demo-seed', 'live', etc.)")
    last_inspection_date: Optional[str] = Field(default=None, description="Date of the most recent recorded inspection (YYYY-MM-DD)")
    latitude: Optional[float] = Field(default=None, description="Latitude of the restaurant (WGS84)")
    longitude: Optional[float] = Field(default=None, description="Longitude of the restaurant (WGS84)")

    if _HAS_CONFIGDICT:
        model_config = ConfigDict(protected_namespaces=())
    else:
        class Config:  # type: ignore
            protected_namespaces = ()
