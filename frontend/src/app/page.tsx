"use client";

import { useState, useEffect, useCallback } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8080";

/* ─── Types ─── */
interface Restaurant {
  fssai_id: string;
  name: string;
  area: string;
}

interface ViolationProb {
  code: string;
  probability: number;
  label: string;
}

interface NearbyRiskFactor {
  name: string;
  type: string;
  distance_m: number | null;
}

interface ScoreData {
  fssai_id: string;
  name: string;
  area: string;
  prob_fssai_fail: number;
  predicted_points: number | null;
  top_reasons: string[];
  top_violation_probs: ViolationProb[];
  environmental_index: number;
  nearby_risk_factors: NearbyRiskFactor[];
  model_version: string | null;
  data_version: string | null;
  last_inspection_date: string | null;
  latitude: number | null;
  longitude: number | null;
}

/* ─── Helpers ─── */
function getRiskLevel(prob: number) {
  if (prob >= 0.6) return "critical";
  if (prob >= 0.4) return "high";
  if (prob >= 0.25) return "moderate";
  return "low";
}

function getRiskLabel(prob: number) {
  if (prob >= 0.6) return "Critical Risk";
  if (prob >= 0.4) return "High Risk";
  if (prob >= 0.25) return "Moderate Risk";
  return "Low Risk";
}

function getRiskColorClass(prob: number) {
  if (prob >= 0.6) return "text-red";
  if (prob >= 0.4) return "text-orange";
  if (prob >= 0.25) return "text-yellow";
  return "text-green";
}

function getGaugeGradient(prob: number) {
  if (prob >= 0.6) return "linear-gradient(90deg, #e53e3e, #c53030)";
  if (prob >= 0.4) return "linear-gradient(90deg, #dd6b20, #c05621)";
  if (prob >= 0.25) return "linear-gradient(90deg, #d69e2e, #b7791f)";
  return "linear-gradient(90deg, #38a169, #2f855a)";
}

function getEnvIcon(type: string) {
  switch (type) {
    case "marketplace": return "🏪";
    case "bus_station": return "🚌";
    case "transit_station": return "🚇";
    case "general_contractor": return "🏗️";
    default: return "📍";
  }
}

function getViolationSeverity(prob: number) {
  if (prob >= 0.5) return "critical";
  if (prob >= 0.35) return "high";
  if (prob >= 0.2) return "moderate";
  return "low";
}

function getViolationSeverityLabel(prob: number) {
  if (prob >= 0.5) return "Critical";
  if (prob >= 0.35) return "High";
  if (prob >= 0.2) return "Moderate";
  return "Low";
}

/* ─── Components ─── */

function Header() {
  return (
    <header className="header">
      <div className="header-inner">
        <div className="header-logo">🍽️<span style={{ fontSize: 14, marginLeft: -4 }}>✅</span></div>
        <div>
          <div className="header-title">Health Inspection Coach</div>
          <div className="header-subtitle">FSSAI Compliance Coach — Ahmedabad</div>
        </div>
      </div>
    </header>
  );
}

function Footer() {
  return (
    <footer className="footer">
      <div className="footer-inner">
        <p className="footer-disclaimer">
          <strong>Disclaimer:</strong> Health Inspection Coach is a predictive tool and is not affiliated
          with FSSAI or any government body. Scores are predictive estimates based on environmental
          analysis and heuristic data — not official grades. For official inspection records, visit{" "}
          <a href="https://foscos.fssai.gov.in/" target="_blank" rel="noopener noreferrer" className="footer-link">
            FSSAI FOSCOS
          </a>.
        </p>
        <div className="footer-links">
          <span>Data: FSSAI Demo Seed · Google Places API</span>
          <a href={`${API_BASE}/docs`} target="_blank" rel="noopener noreferrer" className="footer-link">
            API Docs
          </a>
        </div>
      </div>
    </footer>
  );
}

function LoadingSpinner({ text = "Loading..." }: { text?: string }) {
  return (
    <div className="loading-container">
      <div className="spinner" />
      <div className="loading-text">{text}</div>
    </div>
  );
}

/* ─── Main Page ─── */
export default function Home() {
  const [restaurants, setRestaurants] = useState<Restaurant[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Detail view state
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [scoreData, setScoreData] = useState<ScoreData | null>(null);
  const [scoreLoading, setScoreLoading] = useState(false);
  const [scoreError, setScoreError] = useState<string | null>(null);

  // Search & filter
  const [searchQuery, setSearchQuery] = useState("");
  const [areaFilter, setAreaFilter] = useState("");

  // Fetch restaurants list
  useEffect(() => {
    fetch(`${API_BASE}/restaurants`)
      .then((res) => {
        if (!res.ok) throw new Error("Failed to fetch restaurants");
        return res.json();
      })
      .then(setRestaurants)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  // Fetch score when a restaurant is selected
  const fetchScore = useCallback(async (fssaiId: string) => {
    setSelectedId(fssaiId);
    setScoreLoading(true);
    setScoreError(null);
    setScoreData(null);
    try {
      const res = await fetch(`${API_BASE}/score`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ fssai_id: fssaiId }),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || "Failed to fetch score");
      }
      const data: ScoreData = await res.json();
      setScoreData(data);
    } catch (e: unknown) {
      setScoreError(e instanceof Error ? e.message : "Unknown error");
    } finally {
      setScoreLoading(false);
    }
  }, []);

  const goBack = () => {
    setSelectedId(null);
    setScoreData(null);
    setScoreError(null);
  };

  // Derived data
  const areas = [...new Set(restaurants.map((r) => r.area))];
  const filteredByArea = areaFilter
    ? restaurants.filter((r) => r.area.toLowerCase().includes(areaFilter.toLowerCase()))
    : restaurants;

  const searchResults = searchQuery
    ? restaurants.filter(
        (r) =>
          r.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
          r.area.toLowerCase().includes(searchQuery.toLowerCase()) ||
          r.fssai_id.includes(searchQuery)
      )
    : [];

  // ─── DETAIL VIEW ───
  if (selectedId) {
    return (
      <>
        <Header />
        <main className="main-container">
          {/* Search bar (shows selected restaurant name) */}
          <div className="search-section">
            <div className="search-container">
              <div className="search-icon">🔍</div>
              <input
                className="search-input"
                value={scoreData?.name || selectedId}
                readOnly
              />
              <button className="search-clear" onClick={goBack}>
                ✕
              </button>
            </div>
          </div>

          <div className="back-link" onClick={goBack}>
            ‹ Explore examples
          </div>

          {scoreLoading && <LoadingSpinner text="Analyzing restaurant risk..." />}

          {scoreError && <div className="error-banner">Error: {scoreError}</div>}

          {scoreData && (
            <div className="animate-in">
              {/* Header */}
              <div className="detail-header">
                <div>
                  <h1 className="detail-name">{scoreData.name}</h1>
                  <div className="detail-area">
                    {scoreData.area}, Ahmedabad
                    {scoreData.last_inspection_date &&
                      ` · Last inspected: ${scoreData.last_inspection_date}`}
                  </div>
                </div>
                <button
                  className="share-btn"
                  onClick={() => {
                    navigator.clipboard.writeText(window.location.href);
                  }}
                >
                  Share
                </button>
              </div>

              {/* Score Cards */}
              <div className="score-main">
                <div className={`score-card risk-${getRiskLevel(scoreData.prob_fssai_fail)}`}>
                  <div className={`score-number ${getRiskColorClass(scoreData.prob_fssai_fail)}`}>
                    {(scoreData.prob_fssai_fail * 100).toFixed(1)}%
                  </div>
                  <div className="score-label">FSSAI Fail Probability</div>
                  <div className={`risk-indicator ${getRiskLevel(scoreData.prob_fssai_fail)}`}>
                    {getRiskLevel(scoreData.prob_fssai_fail) === "critical" ? "⚠️" : getRiskLevel(scoreData.prob_fssai_fail) === "high" ? "🔶" : getRiskLevel(scoreData.prob_fssai_fail) === "moderate" ? "🟡" : "✅"}
                    {" "}{getRiskLabel(scoreData.prob_fssai_fail)}
                  </div>
                  <div className="gauge-container">
                    <div className="gauge-bar-bg">
                      <div
                        className="gauge-bar-fill"
                        style={{
                          width: `${scoreData.prob_fssai_fail * 100}%`,
                          background: getGaugeGradient(scoreData.prob_fssai_fail),
                        }}
                      />
                    </div>
                    <div className="gauge-labels">
                      <span>0%</span>
                      <span>100%</span>
                    </div>
                  </div>
                </div>

                <div className={`score-card risk-${getRiskLevel(scoreData.environmental_index)}`}>
                  <div className={`score-number ${getRiskColorClass(scoreData.environmental_index)}`}>
                    {(scoreData.environmental_index * 100).toFixed(0)}%
                  </div>
                  <div className="score-label">Environmental Risk Index</div>
                  <div className={`risk-indicator ${getRiskLevel(scoreData.environmental_index)}`}>
                    🌍 {scoreData.nearby_risk_factors.length} nearby risk factor{scoreData.nearby_risk_factors.length !== 1 ? "s" : ""}
                  </div>
                  <div className="gauge-container">
                    <div className="gauge-bar-bg">
                      <div
                        className="gauge-bar-fill"
                        style={{
                          width: `${scoreData.environmental_index * 100}%`,
                          background: getGaugeGradient(scoreData.environmental_index),
                        }}
                      />
                    </div>
                    <div className="gauge-labels">
                      <span>Safe</span>
                      <span>Risky</span>
                    </div>
                  </div>
                </div>
              </div>

              {/* Top Reasons */}
              {scoreData.top_reasons.length > 0 && (
                <div className="card animate-in animate-delay-1" style={{ marginBottom: 16 }}>
                  <div className="card-header">KEY RISK FACTORS</div>
                  <div className="card-body">
                    <ul className="reasons-list">
                      {scoreData.top_reasons.map((reason, i) => (
                        <li key={i}>
                          <span className="reason-icon">
                            {reason.toLowerCase().includes("high") ? "🔴" : reason.toLowerCase().includes("moderate") ? "🟠" : "🔵"}
                          </span>
                          {reason}
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              )}

              {/* Violation Probabilities */}
              {scoreData.top_violation_probs.length > 0 && (
                <div className="card animate-in animate-delay-2" style={{ marginBottom: 16 }}>
                  <div className="card-header">FSSAI SCHEDULE 4 VIOLATIONS</div>
                  <div className="card-body">
                    {scoreData.top_violation_probs.map((v, i) => (
                      <div key={i} className="detail-violation-item">
                        <div className="detail-violation-left">
                          <span className="detail-violation-code">{v.code}</span>
                          <span className="detail-violation-label">{v.label}</span>
                        </div>
                        <span className={`detail-violation-prob ${getRiskColorClass(v.probability)}`}>
                          {(v.probability * 100).toFixed(1)}%
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Environmental Risk Factors */}
              {scoreData.nearby_risk_factors.length > 0 && (
                <div className="card animate-in animate-delay-3">
                  <div className="card-header">NEARBY ENVIRONMENTAL RISKS (200m RADIUS)</div>
                  <div className="card-body">
                    {scoreData.nearby_risk_factors.map((f, i) => (
                      <div key={i} className="env-factor">
                        <div className={`env-icon ${f.type}`}>{getEnvIcon(f.type)}</div>
                        <div className="env-info">
                          <div className="env-name">{f.name}</div>
                          <div className="env-type">{f.type.replace(/_/g, " ")}</div>
                        </div>
                        {f.distance_m != null && (
                          <div className="env-distance">{f.distance_m.toFixed(0)}m</div>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Meta */}
              <div style={{ marginTop: 24, fontSize: 12, color: "var(--text-muted)" }}>
                Model: {scoreData.model_version} · Data: {scoreData.data_version}
                {scoreData.latitude && scoreData.longitude && (
                  <> · {scoreData.latitude.toFixed(4)}°N, {scoreData.longitude.toFixed(4)}°E</>
                )}
              </div>
            </div>
          )}
        </main>
        <Footer />
      </>
    );
  }

  // ─── HOME VIEW ───
  return (
    <>
      <Header />
      <main className="main-container">
        {/* Search */}
        <div className="search-section animate-in">
          <div className="search-container">
            <div className="search-icon">🔍</div>
            <input
              id="search-input"
              className="search-input"
              placeholder="Search by restaurant name, area, or FSSAI ID..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
            {searchQuery && (
              <button className="search-clear" onClick={() => setSearchQuery("")}>
                ✕
              </button>
            )}
          </div>

          {/* Search Results Dropdown */}
          {searchQuery && searchResults.length > 0 && (
            <div className="card" style={{ marginTop: 4, maxHeight: 320, overflowY: "auto" }}>
              {searchResults.map((r) => (
                <div
                  key={r.fssai_id}
                  style={{
                    padding: "14px 24px",
                    cursor: "pointer",
                    borderBottom: "1px solid var(--border-light)",
                    transition: "background 0.15s",
                  }}
                  onClick={() => {
                    setSearchQuery("");
                    fetchScore(r.fssai_id);
                  }}
                  onMouseEnter={(e) => (e.currentTarget.style.background = "var(--accent-blue-light)")}
                  onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
                >
                  <div style={{ fontWeight: 600, fontSize: 14 }}>{r.name}</div>
                  <div style={{ fontSize: 12, color: "var(--text-muted)" }}>
                    {r.area} · FSSAI: {r.fssai_id}
                  </div>
                </div>
              ))}
            </div>
          )}
          {searchQuery && searchResults.length === 0 && (
            <div className="card" style={{ marginTop: 4, padding: "20px 24px", color: "var(--text-muted)", fontSize: 14 }}>
              No restaurants found matching &ldquo;{searchQuery}&rdquo;
            </div>
          )}
        </div>

        {loading && <LoadingSpinner />}
        {error && <div className="error-banner">Error: {error}</div>}

        {!loading && !error && (
          <>
            {/* Stats */}
            <div className="stats-grid animate-in animate-delay-1">
              <div className="stat-card">
                <div className="stat-value">{restaurants.length}</div>
                <div className="stat-label">Restaurants Monitored</div>
              </div>
              <div className="stat-card">
                <div className="stat-value">{areas.length}</div>
                <div className="stat-label">Areas Covered</div>
              </div>
              <div className="stat-card">
                <div className="stat-value">200m</div>
                <div className="stat-label">Environmental Scan Radius</div>
              </div>
            </div>

            {/* Area Overview Table */}
            <div className="card animate-in animate-delay-2" style={{ marginBottom: 24 }}>
              <div className="card-header">RESTAURANTS BY AREA</div>
              <div className="card-body" style={{ padding: "0 0 8px" }}>
                <table className="area-table">
                  <thead>
                    <tr>
                      <th>Area</th>
                      <th>Restaurants</th>
                      <th>Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {areas.map((area) => {
                      const count = restaurants.filter((r) => r.area === area).length;
                      return (
                        <tr key={area}>
                          <td>
                            <div className="area-name">{area}</div>
                          </td>
                          <td>{count}</td>
                          <td>
                            <span
                              style={{
                                color: "var(--accent-blue)",
                                cursor: "pointer",
                                fontSize: 13,
                                fontWeight: 500,
                              }}
                              onClick={() => {
                                setAreaFilter(area);
                                setTimeout(() => {
                                  document.getElementById("area-results")?.scrollIntoView({ behavior: "smooth", block: "start" });
                                }, 100);
                              }}
                            >
                              View →
                            </span>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Common FSSAI Violation Categories */}
            <div className="card animate-in animate-delay-3" style={{ marginBottom: 40 }}>
              <div className="card-header">MOST COMMON FSSAI SCHEDULE 4 VIOLATIONS</div>
              <div className="card-body">
                <ul className="violation-list">
                  <li className="violation-item">
                    <span className="violation-badge high">High</span>
                    <span className="violation-text">
                      Temperature control of food — hot/cold holding requirements not maintained;
                      food stored outside safe temperature zones increasing bacterial growth risk.
                    </span>
                    <span className="violation-count">
                      {restaurants.length} <span>restaurants</span>
                    </span>
                  </li>
                  <li className="violation-item">
                    <span className="violation-badge moderate">Moderate</span>
                    <span className="violation-text">
                      Cleanliness of premises and equipment — food contact surfaces not properly
                      sanitized; equipment in disrepair or showing signs of contamination.
                    </span>
                    <span className="violation-count">
                      {restaurants.length} <span>restaurants</span>
                    </span>
                  </li>
                  <li className="violation-item">
                    <span className="violation-badge critical">Critical</span>
                    <span className="violation-text">
                      Pest control measures — evidence of pests on premises; inadequate pest
                      management systems leading to potential food contamination.
                    </span>
                    <span className="violation-count">
                      {restaurants.length} <span>restaurants</span>
                    </span>
                  </li>
                </ul>
              </div>
            </div>

            {/* Browse by Area */}
            <div className="browse-section animate-in animate-delay-4">
              <h2 className="browse-title">Browse by area</h2>
              <p className="browse-subtitle">
                See all restaurants in an Ahmedabad area ranked by inspection risk.
              </p>
              <div className="area-search-container">
                <input
                  id="area-search-input"
                  className="area-search-input"
                  placeholder="e.g. Navrangpura"
                  value={areaFilter}
                  onChange={(e) => setAreaFilter(e.target.value)}
                />
                <button
                  className="area-search-btn"
                  onClick={() => {
                    if (!areaFilter) return;
                  }}
                >
                  Search
                </button>
              </div>

              {/* Area Filter Results */}
              {areaFilter && (
                <div id="area-results" className="card" style={{ marginBottom: 24, scrollMarginTop: 24 }}>
                  <div className="card-header" style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <span>{filteredByArea.length} RESTAURANT{filteredByArea.length !== 1 ? "S" : ""} IN &ldquo;{areaFilter.toUpperCase()}&rdquo;</span>
                    <button 
                      onClick={() => setAreaFilter("")}
                      style={{ 
                        background: "none", 
                        border: "none", 
                        color: "var(--accent-blue)", 
                        fontSize: 12, 
                        cursor: "pointer",
                        fontWeight: 500
                      }}
                    >
                      Clear Filter ✕
                    </button>
                  </div>
                  <div className="card-body" style={{ padding: "0 0 8px" }}>
                    {filteredByArea.length === 0 ? (
                      <div style={{ padding: "20px 16px", color: "var(--text-muted)", fontSize: 14 }}>
                        No restaurants found in this area.
                      </div>
                    ) : (
                      <table className="area-table">
                        <thead>
                          <tr>
                            <th>Restaurant</th>
                            <th>FSSAI ID</th>
                            <th>Action</th>
                          </tr>
                        </thead>
                        <tbody>
                          {filteredByArea.map((r) => (
                            <tr key={r.fssai_id}>
                              <td>
                                <div className="area-name">{r.name}</div>
                                <div className="area-cuisine">{r.area}</div>
                              </td>
                              <td style={{ fontFamily: "'SF Mono', 'Fira Code', 'Consolas', monospace", fontSize: 12 }}>
                                {r.fssai_id}
                              </td>
                              <td>
                                <span
                                  style={{
                                    color: "var(--accent-blue)",
                                    cursor: "pointer",
                                    fontSize: 13,
                                    fontWeight: 500,
                                  }}
                                  onClick={() => fetchScore(r.fssai_id)}
                                >
                                  View risk score →
                                </span>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </div>
                </div>
              )}
            </div>

            {/* Example Restaurants */}
            <div className="animate-in animate-delay-5">
              <div className="examples-header">
                <h2 className="examples-title">Try an example</h2>
                <span className="examples-hint">Click any restaurant to see its risk score</span>
              </div>
              <div className="examples-grid">
                {restaurants.slice(0, 6).map((r) => (
                  <div
                    key={r.fssai_id}
                    className="example-card"
                    onClick={() => fetchScore(r.fssai_id)}
                  >
                    <div className="example-name">
                      {r.name}
                      <span className="example-area-badge">{r.area}</span>
                    </div>
                    <div className="example-address">FSSAI: {r.fssai_id}</div>
                    <span className="example-link">View risk score →</span>
                  </div>
                ))}
              </div>
            </div>
          </>
        )}
      </main>
      <Footer />
    </>
  );
}
