import { useState, useEffect, useCallback } from 'react'
import './App.css'

function PPDLogo({ size = 36 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 100 100" fill="none" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <linearGradient id="mtnL" x1="50%" y1="0%" x2="50%" y2="100%">
          <stop offset="0%" stopColor="#5ba3f5"/><stop offset="100%" stopColor="#1a3d8a"/>
        </linearGradient>
        <linearGradient id="mtnR" x1="50%" y1="0%" x2="50%" y2="100%">
          <stop offset="0%" stopColor="#4490d0"/><stop offset="100%" stopColor="#132e70"/>
        </linearGradient>
      </defs>
      <polygon points="36,90 66,26 96,90" fill="url(#mtnR)"/>
      <polygon points="4,90 38,10 72,90" fill="url(#mtnL)"/>
      <polygon points="38,10 27,28 49,28" fill="white"/>
      <polygon points="66,26 58,38 74,38" fill="white"/>
      <polyline points="8,74 24,60 42,66 62,46 80,28 96,16" fill="none" stroke="white" strokeWidth="3.5" strokeLinecap="round" strokeLinejoin="round"/>
      <circle cx="8"  cy="74" r="4" fill="white"/>
      <circle cx="24" cy="60" r="4" fill="white"/>
      <circle cx="42" cy="66" r="4" fill="white"/>
      <circle cx="62" cy="46" r="4" fill="white"/>
      <circle cx="96" cy="16" r="7.5" fill="#3b82f6"/>
      <circle cx="96" cy="16" r="4"   fill="#1e40af"/>
    </svg>
  )
}

// ── Helpers ──────────────────────────────────────────────────────────────────
function sevClass(s) {
  return `badge badge-${(s || 'unknown').toLowerCase()}`
}
function prioClass(p) {
  return `badge badge-${(p || 'backlog').toLowerCase()}`
}
function statusClass(s) {
  return `badge badge-${(s || 'open').toLowerCase()}`
}
function fmtDate(iso) {
  if (!iso) return '—'
  return new Date(iso).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
}
function fmtDateShort(iso) {
  if (!iso) return '—'
  return new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

// ── Dashboard View ───────────────────────────────────────────────────────────
function DashboardView() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    fetch('/api/dashboard')
      .then(r => r.json())
      .then(setData)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <div className="loading">Loading dashboard…</div>
  if (error)   return <div className="error-msg">{error}</div>
  if (!data)   return null

  const findings = data.open_findings || {}
  const plans    = data.action_plans  || {}
  const latest   = data.latest_scan   || {}
  const concerns = data.top_concerns  || []

  return (
    <div>
      <div className="view-title">Security Dashboard</div>

      {/* Severity stats */}
      <div className="stat-grid">
        {[['CRITICAL','stat-critical'],['HIGH','stat-high'],['MEDIUM','stat-medium'],['LOW','stat-low']].map(([sev, cls]) => (
          <div className="stat-card" key={sev}>
            <div className={`stat-num ${cls}`}>{(findings[sev] || 0).toLocaleString()}</div>
            <div className="stat-label">{sev}</div>
          </div>
        ))}
      </div>

      {/* Priority action plan counts */}
      <div className="priority-grid">
        {[['immediate','Immediate'],['this_week','This Week'],['backlog','Backlog']].map(([key, label]) => (
          <div className={`priority-card priority-${key}`} key={key}>
            <div className="priority-num">{plans[key] || 0}</div>
            <div className="priority-label">{label} Action Plans</div>
          </div>
        ))}
      </div>

      <div className="dash-row">
        {/* Latest scan info */}
        <div className="card">
          <div className="section-title">Latest Scan</div>
          {latest.id ? (
            <div style={{ fontSize: '0.83rem' }}>
              <div style={{ marginBottom: 8 }}>
                <span className={statusClass(latest.status)}>{latest.status}</span>
                <span style={{ color: '#64748b', marginLeft: 10 }}>{fmtDate(latest.completed_at)}</span>
              </div>
              <div style={{ color: '#94a3b8', marginBottom: 4 }}>
                {latest.images_scanned} images scanned
              </div>
              <div style={{ display: 'flex', gap: 16 }}>
                <span style={{ color: '#f87171' }}>{(latest.critical_count || 0).toLocaleString()} critical</span>
                <span style={{ color: '#fb923c' }}>{(latest.high_count || 0).toLocaleString()} high</span>
                <span style={{ color: '#60a5fa' }}>{(latest.findings_count || 0).toLocaleString()} total</span>
              </div>
            </div>
          ) : (
            <div style={{ color: '#64748b', fontSize: '0.83rem' }}>No completed scans yet.</div>
          )}
        </div>

        {/* Version drift */}
        <div className="card">
          <div className="section-title">Version Drift</div>
          <div style={{ textAlign: 'center', paddingTop: 8 }}>
            <div style={{ fontSize: '2rem', fontWeight: 800, color: data.version_drift_count ? '#fbbf24' : '#4ade80' }}>
              {data.version_drift_count || 0}
            </div>
            <div style={{ fontSize: '0.75rem', color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
              {data.version_drift_count === 1 ? 'service behind' : 'services behind'}
            </div>
          </div>
        </div>
      </div>

      {/* Recent scans */}
      <div className="card" style={{ marginBottom: 16 }}>
        <div className="section-title">Recent Scan Runs</div>
        {(data.recent_scans || []).map(run => (
          <div className="scan-run-row" key={run.id}>
            <span className="scan-run-id">#{run.id}</span>
            <span className="scan-run-type">{run.scan_type}</span>
            <span className={statusClass(run.status)}>{run.status}</span>
            <span className="scan-run-counts">
              {run.findings_count != null ? `${run.findings_count.toLocaleString()} findings` : ''}
            </span>
            <span className="scan-run-date">{fmtDate(run.started_at)}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Action Plans View ────────────────────────────────────────────────────────
function ActionPlansView() {
  const [plans, setPlans]       = useState([])
  const [loading, setLoading]   = useState(true)
  const [error, setError]       = useState(null)
  const [priority, setPriority] = useState('')
  const [status, setStatus]     = useState('open')
  const [expanded, setExpanded] = useState(new Set())
  const [updating, setUpdating] = useState(new Set())

  const load = useCallback(() => {
    setLoading(true)
    const params = new URLSearchParams()
    if (priority) params.set('priority', priority)
    if (status)   params.set('status', status)
    fetch(`/api/action-plans?${params}`)
      .then(r => r.json())
      .then(d => setPlans(d.plans || []))
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [priority, status])

  useEffect(() => { load() }, [load])

  const toggleExpand = id => {
    setExpanded(prev => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })
  }

  const updateStatus = async (id, newStatus) => {
    setUpdating(prev => new Set(prev).add(id))
    try {
      await fetch(`/api/action-plans/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: newStatus }),
      })
      setPlans(prev => prev.map(p => p.id === id ? { ...p, status: newStatus } : p))
    } catch (e) {
      setError(e.message)
    } finally {
      setUpdating(prev => { const n = new Set(prev); n.delete(id); return n })
    }
  }

  const groups = ['immediate', 'this_week', 'backlog']

  if (loading) return <div className="loading">Loading action plans…</div>
  if (error)   return <div className="error-msg">{error}</div>

  return (
    <div>
      <div className="view-title">Action Plans</div>

      <div className="filter-bar">
        <div className="filter-group">
          <span className="filter-label">Priority</span>
          <select className="filter-select" value={priority} onChange={e => setPriority(e.target.value)}>
            <option value="">All</option>
            <option value="immediate">Immediate</option>
            <option value="this_week">This Week</option>
            <option value="backlog">Backlog</option>
          </select>
        </div>
        <div className="filter-group">
          <span className="filter-label">Status</span>
          <select className="filter-select" value={status} onChange={e => setStatus(e.target.value)}>
            <option value="">All</option>
            <option value="open">Open</option>
            <option value="in_progress">In Progress</option>
            <option value="completed">Completed</option>
          </select>
        </div>
      </div>

      {plans.length === 0 && (
        <div className="empty-state">
          <div className="empty-state-icon">✓</div>
          <div className="empty-state-text">No action plans matching this filter.</div>
        </div>
      )}

      {groups.map(grp => {
        const grpPlans = plans.filter(p => p.priority === grp)
        if (!grpPlans.length) return null
        return (
          <div key={grp}>
            <div className="plan-section-header">
              <span className={prioClass(grp)}>{grp.replace('_', ' ')}</span>
              <span style={{ marginLeft: 8, color: '#64748b' }}>— {grpPlans.length} plan{grpPlans.length !== 1 ? 's' : ''}</span>
            </div>
            {grpPlans.map(plan => (
              <div className="plan-card" key={plan.id}>
                <div className="plan-card-header" onClick={() => toggleExpand(plan.id)}>
                  <div style={{ flex: 1 }}>
                    <div className="plan-title">{plan.title}</div>
                    <div className="plan-meta">
                      <span style={{ color: '#60a5fa', fontSize: '0.75rem' }}>{plan.service_name}</span>
                      <span className={`badge badge-${plan.action_type}`}>{plan.action_type}</span>
                      <span className={`badge badge-${plan.estimated_effort}`}>{plan.estimated_effort}</span>
                      <span className={statusClass(plan.status)}>{plan.status?.replace('_', ' ')}</span>
                      <span style={{ color: '#64748b', fontSize: '0.72rem' }}>{fmtDateShort(plan.scan_date)}</span>
                    </div>
                  </div>
                  <span className={`plan-chevron ${expanded.has(plan.id) ? 'open' : ''}`}>▼</span>
                </div>

                {expanded.has(plan.id) && (
                  <div className="plan-card-body">
                    <div className="plan-description">{plan.description}</div>

                    {(plan.steps || []).length > 0 && (
                      <>
                        <div className="plan-steps-title">Remediation Steps</div>
                        <ol className="plan-steps">
                          {plan.steps.map((step, i) => (
                            <li key={i}>
                              <span className="plan-step-num">{i + 1}</span>
                              <span>{step}</span>
                            </li>
                          ))}
                        </ol>
                      </>
                    )}

                    <div className="plan-actions">
                      {plan.status !== 'in_progress' && plan.status !== 'completed' && (
                        <button className="btn btn-warning" disabled={updating.has(plan.id)}
                          onClick={() => updateStatus(plan.id, 'in_progress')}>
                          Mark In Progress
                        </button>
                      )}
                      {plan.status !== 'completed' && (
                        <button className="btn btn-success" disabled={updating.has(plan.id)}
                          onClick={() => updateStatus(plan.id, 'completed')}>
                          Mark Complete ✓
                        </button>
                      )}
                      {plan.status !== 'open' && (
                        <button className="btn btn-ghost" disabled={updating.has(plan.id)}
                          onClick={() => updateStatus(plan.id, 'open')}>
                          Reopen
                        </button>
                      )}
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        )
      })}
    </div>
  )
}

// ── Findings View ────────────────────────────────────────────────────────────
function FindingsView() {
  const [findings, setFindings] = useState([])
  const [total, setTotal]       = useState(0)
  const [loading, setLoading]   = useState(true)
  const [error, setError]       = useState(null)
  const [images, setImages]     = useState([])
  const [severity, setSeverity] = useState('')
  const [image, setImage]       = useState('')
  const [status, setStatus]     = useState('open')
  const [cve, setCve]           = useState('')
  const [offset, setOffset]     = useState(0)
  const [updating, setUpdating] = useState(new Set())
  const limit = 100

  useEffect(() => {
    fetch('/api/images').then(r => r.json()).then(d => setImages(d.images || []))
  }, [])

  const load = useCallback(() => {
    setLoading(true)
    const params = new URLSearchParams({ limit, offset })
    if (severity) params.set('severity', severity)
    if (image)    params.set('image', image)
    if (status)   params.set('status', status)
    if (cve)      params.set('cve_id', cve)
    fetch(`/api/findings?${params}`)
      .then(r => r.json())
      .then(d => { setFindings(d.findings || []); setTotal(d.total || 0) })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [severity, image, status, cve, offset])

  useEffect(() => { setOffset(0) }, [severity, image, status, cve])
  useEffect(() => { load() }, [load])

  const updateStatus = async (id, newStatus) => {
    setUpdating(prev => new Set(prev).add(id))
    try {
      await fetch(`/api/findings/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: newStatus }),
      })
      setFindings(prev => prev.map(f => f.id === id ? { ...f, status: newStatus } : f))
    } catch (e) {
      setError(e.message)
    } finally {
      setUpdating(prev => { const n = new Set(prev); n.delete(id); return n })
    }
  }

  return (
    <div>
      <div className="view-title">Vulnerability Findings</div>

      <div className="filter-bar">
        <div className="filter-group">
          <span className="filter-label">Severity</span>
          <select className="filter-select" value={severity} onChange={e => setSeverity(e.target.value)}>
            <option value="">All</option>
            {['CRITICAL','HIGH','MEDIUM','LOW','UNKNOWN'].map(s => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>
        <div className="filter-group">
          <span className="filter-label">Image</span>
          <select className="filter-select" value={image} onChange={e => setImage(e.target.value)}>
            <option value="">All Images</option>
            {images.map(img => <option key={img} value={img}>{img}</option>)}
          </select>
        </div>
        <div className="filter-group">
          <span className="filter-label">Status</span>
          <select className="filter-select" value={status} onChange={e => setStatus(e.target.value)}>
            <option value="">All</option>
            <option value="open">Open</option>
            <option value="accepted">Accepted</option>
            <option value="false_positive">False Positive</option>
            <option value="resolved">Resolved</option>
          </select>
        </div>
        <div className="filter-group">
          <span className="filter-label">CVE</span>
          <input
            type="text"
            className="filter-select"
            placeholder="e.g. CVE-2025-1234"
            value={cve}
            onChange={e => { setCve(e.target.value); if (e.target.value) setStatus('') }}
            style={{ fontFamily: 'monospace', minWidth: '180px' }}
          />
        </div>
        <span style={{ marginLeft: 'auto', fontSize: '0.8rem', color: '#64748b' }}>
          {total.toLocaleString()} total
        </span>
      </div>

      {error && <div className="error-msg">{error}</div>}

      {loading ? (
        <div className="loading">Loading findings…</div>
      ) : findings.length === 0 ? (
        <div className="empty-state">
          <div className="empty-state-icon">✓</div>
          <div className="empty-state-text">No findings matching this filter.</div>
        </div>
      ) : (
        <div className="card" style={{ padding: 0 }}>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Severity</th>
                  <th>CVE</th>
                  <th>CVSS</th>
                  <th>Image</th>
                  <th>Package</th>
                  <th>Fixed In</th>
                  <th>Title</th>
                  <th>Status</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {findings.map(f => (
                  <tr key={f.id}>
                    <td><span className={sevClass(f.severity)}>{f.severity}</span></td>
                    <td className="td-cve">{f.cve_id || '—'}</td>
                    <td className="td-muted" style={{ textAlign: 'right' }}>
                      {f.cvss_score != null ? Number(f.cvss_score).toFixed(1) : '—'}
                    </td>
                    <td className="td-muted" style={{ fontSize: '0.75rem', maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {f.image_name}
                    </td>
                    <td className="td-mono">{f.package_name || '—'}</td>
                    <td>
                      {f.fixed_version
                        ? <span className="td-fixed">✓ {f.fixed_version}</span>
                        : <span className="td-nofixed">—</span>}
                    </td>
                    <td style={{ maxWidth: 220, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: '#94a3b8', fontSize: '0.78rem' }}>
                      {f.title || '—'}
                    </td>
                    <td><span className={statusClass(f.status)}>{f.status}</span></td>
                    <td className="td-actions">
                      {f.status === 'open' && (
                        <>
                          <button className="btn btn-secondary" style={{ marginRight: 4 }}
                            disabled={updating.has(f.id)}
                            onClick={() => updateStatus(f.id, 'accepted')}>
                            Accept
                          </button>
                          <button className="btn btn-ghost"
                            disabled={updating.has(f.id)}
                            onClick={() => updateStatus(f.id, 'false_positive')}>
                            FP
                          </button>
                        </>
                      )}
                      {f.status !== 'open' && (
                        <button className="btn btn-ghost"
                          disabled={updating.has(f.id)}
                          onClick={() => updateStatus(f.id, 'open')}>
                          Reopen
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="pagination" style={{ padding: '12px 16px' }}>
            <button className="btn btn-ghost" disabled={offset === 0}
              onClick={() => setOffset(Math.max(0, offset - limit))}>
              ← Prev
            </button>
            <span>
              {offset + 1}–{Math.min(offset + limit, total)} of {total.toLocaleString()}
            </span>
            <button className="btn btn-ghost" disabled={offset + limit >= total}
              onClick={() => setOffset(offset + limit)}>
              Next →
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Version Drift View ───────────────────────────────────────────────────────
function VersionDriftView() {
  const [drift, setDrift]     = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(null)

  useEffect(() => {
    fetch('/api/version-drift')
      .then(r => r.json())
      .then(d => setDrift(d.drift || []))
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <div className="loading">Loading version drift…</div>
  if (error)   return <div className="error-msg">{error}</div>

  return (
    <div>
      <div className="view-title">Version Drift</div>
      {drift.length === 0 ? (
        <div className="card drift-empty">
          <div style={{ fontSize: '2rem', marginBottom: 8 }}>✓</div>
          All monitored services are up to date.
        </div>
      ) : (
        <div className="card" style={{ padding: 0 }}>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Service</th>
                  <th>Image</th>
                  <th>Running</th>
                  <th>Latest</th>
                  <th>Behind</th>
                  <th>Release Notes</th>
                  <th>Checked</th>
                </tr>
              </thead>
              <tbody>
                {drift.map(d => (
                  <tr key={d.id}>
                    <td style={{ fontWeight: 600, color: '#e2e8f0' }}>{d.service_name}</td>
                    <td className="td-muted td-mono" style={{ fontSize: '0.75rem' }}>{d.image_name}</td>
                    <td className="td-mono" style={{ fontSize: '0.78rem', color: '#f87171' }}>{d.running_tag}</td>
                    <td className="td-mono" style={{ fontSize: '0.78rem', color: '#4ade80' }}>{d.latest_tag}</td>
                    <td>
                      <span className={d.versions_behind >= 2 ? 'drift-behind-high' : 'drift-behind-1'} style={{ fontWeight: 700 }}>
                        {d.versions_behind != null ? `${d.versions_behind} major` : '—'}
                      </span>
                    </td>
                    <td>
                      {d.release_notes_url
                        ? <a href={d.release_notes_url} target="_blank" rel="noreferrer"
                             style={{ color: '#60a5fa', fontSize: '0.78rem' }}>View ↗</a>
                        : <span className="td-muted">—</span>}
                    </td>
                    <td className="td-muted" style={{ fontSize: '0.75rem' }}>{fmtDate(d.checked_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

// ── App Shell ────────────────────────────────────────────────────────────────
const VIEWS = [
  { id: 'dashboard', label: 'Dashboard',     icon: '⬡' },
  { id: 'plans',     label: 'Action Plans',  icon: '⚡' },
  { id: 'findings',  label: 'Findings',      icon: '🔍' },
  { id: 'drift',     label: 'Version Drift', icon: '⬆' },
]

export default function App() {
  const [view, setView] = useState('dashboard')

  return (
    <div className="app">
      <header className="header">
        <PPDLogo size={36} />
        <div className="header-brand">
          <span className="header-brand-top">Peak Precision Data</span>
          <span className="header-brand-bottom">Security Portal</span>
        </div>
        <div className="header-spacer" />
        <div className="header-meta">security.peakprecisiondata.com</div>
      </header>

      <div className="body">
        <nav className="sidebar">
          {VIEWS.map(v => (
            <button key={v.id} className={`nav-item${view === v.id ? ' active' : ''}`}
              onClick={() => setView(v.id)}>
              <span className="nav-icon">{v.icon}</span>
              {v.label}
            </button>
          ))}
        </nav>

        <main className="main">
          {view === 'dashboard' && <DashboardView />}
          {view === 'plans'     && <ActionPlansView />}
          {view === 'findings'  && <FindingsView />}
          {view === 'drift'     && <VersionDriftView />}
        </main>
      </div>
    </div>
  )
}
