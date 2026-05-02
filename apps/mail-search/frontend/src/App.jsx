import { useState, useCallback, useRef, useEffect } from 'react'
import Markdown from 'react-markdown'
import './App.css'

// ── PPD Logo ─────────────────────────────────────────────────────────────────

function PPDLogo({ size = 36 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <linearGradient id="ppd-lg" x1="50%" y1="0%" x2="50%" y2="100%">
          <stop offset="0%" stopColor="#5ba3f5" />
          <stop offset="100%" stopColor="#1a3d8a" />
        </linearGradient>
        <linearGradient id="ppd-rg" x1="50%" y1="0%" x2="50%" y2="100%">
          <stop offset="0%" stopColor="#4490d0" />
          <stop offset="100%" stopColor="#132e70" />
        </linearGradient>
      </defs>
      {/* Right mountain */}
      <polygon points="36,90 66,26 96,90" fill="url(#ppd-rg)" />
      {/* Left mountain */}
      <polygon points="4,90 38,10 72,90" fill="url(#ppd-lg)" />
      {/* Snow caps */}
      <polygon points="38,10 27,28 49,28" fill="white" />
      <polygon points="66,26 58,38 74,38" fill="white" />
      {/* Data line */}
      <polyline
        points="8,74 24,60 42,66 62,46 80,28 96,16"
        fill="none"
        stroke="white"
        strokeWidth="3.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      {/* Data nodes */}
      <circle cx="8"  cy="74" r="4" fill="white" />
      <circle cx="24" cy="60" r="4" fill="white" />
      <circle cx="42" cy="66" r="4" fill="white" />
      <circle cx="62" cy="46" r="4" fill="white" />
      {/* Accent dot */}
      <circle cx="96" cy="16" r="7.5" fill="#3b82f6" />
      <circle cx="96" cy="16" r="4"   fill="#1e40af" />
    </svg>
  )
}

// ── Saved searches ───────────────────────────────────────────────────────────

const SAVED_KEY = 'ppd-mail-saved-searches'

function useSavedSearches() {
  const [saved, setSaved] = useState(() => {
    try { return JSON.parse(localStorage.getItem(SAVED_KEY) ?? '[]') }
    catch { return [] }
  })

  const persist = (next) => {
    localStorage.setItem(SAVED_KEY, JSON.stringify(next))
    setSaved(next)
  }

  const save   = (q) => persist(saved.includes(q) ? saved : [q, ...saved].slice(0, 30))
  const remove = (q) => persist(saved.filter(s => s !== q))

  return { saved, save, remove }
}

// ── Type metadata ────────────────────────────────────────────────────────────

const TYPE_META = {
  statement:   { label: 'Statement',   color: '#3b82f6' },
  eob:         { label: 'EOB',         color: '#8b5cf6' },
  legal:       { label: 'Legal',       color: '#ef4444' },
  government:  { label: 'Government',  color: '#f59e0b' },
  insurance:   { label: 'Insurance',   color: '#10b981' },
  personal:    { label: 'Personal',    color: '#ec4899' },
  marketing:   { label: 'Marketing',   color: '#6b7280' },
  utility:     { label: 'Utility',     color: '#06b6d4' },
  unknown:     { label: 'Unknown',     color: '#334155' },
}

function typeMeta(type) {
  return TYPE_META[type] ?? TYPE_META.unknown
}

// ── UploadZone ───────────────────────────────────────────────────────────────

function UploadZone() {
  const [state, setState] = useState('idle') // idle | uploading | success | error
  const [message, setMessage] = useState('')
  const [dragging, setDragging] = useState(false)
  const inputRef = useRef(null)
  const timerRef = useRef(null)

  const reset = () => {
    timerRef.current = setTimeout(() => setState('idle'), 5000)
  }

  useEffect(() => () => clearTimeout(timerRef.current), [])

  const uploadFiles = async (files) => {
    const pdfs = Array.from(files).filter(f => f.type === 'application/pdf')
    const nonPdfs = files.length - pdfs.length

    if (pdfs.length === 0) {
      setState('error')
      setMessage('Only PDF files are supported')
      reset()
      return
    }

    setState('uploading')
    setMessage(pdfs.length > 1 ? `Uploading ${pdfs.length} files…` : '')

    const results = await Promise.allSettled(
      pdfs.map(file => {
        const body = new FormData()
        body.append('file', file)
        return fetch('/api/upload', { method: 'POST', body }).then(res => {
          if (!res.ok) return res.json().then(d => Promise.reject(new Error(d.detail ?? `${res.status}`)))
        })
      })
    )

    const succeeded = results.filter(r => r.status === 'fulfilled').length
    const failed = results.filter(r => r.status === 'rejected').length + nonPdfs

    if (failed === 0) {
      setState('success')
      setMessage(
        succeeded === 1
          ? 'Document received — enrichment ready within ~30 min'
          : `${succeeded} documents received — enrichment ready within ~30 min`
      )
    } else if (succeeded === 0) {
      setState('error')
      setMessage(nonPdfs === files.length ? 'Only PDF files are supported' : `All ${failed} uploads failed`)
    } else {
      setState('error')
      setMessage(`${succeeded} uploaded, ${failed} failed`)
    }
    reset()
  }

  const onDrop = (e) => {
    e.preventDefault()
    setDragging(false)
    uploadFiles(e.dataTransfer.files)
  }

  const onDragOver = (e) => { e.preventDefault(); setDragging(true) }
  const onDragLeave = () => setDragging(false)

  return (
    <div
      className={`upload-zone ${dragging ? 'upload-zone-drag' : ''} ${state === 'uploading' ? 'upload-zone-busy' : ''}`}
      onDrop={onDrop}
      onDragOver={onDragOver}
      onDragLeave={onDragLeave}
      onClick={() => state === 'idle' && inputRef.current?.click()}
    >
      <input
        ref={inputRef}
        type="file"
        accept="application/pdf"
        multiple
        style={{ display: 'none' }}
        onChange={e => uploadFiles(e.target.files)}
      />
      {state === 'idle' && (
        <>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none"
            stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
            <polyline points="17 8 12 3 7 8"/>
            <line x1="12" y1="3" x2="12" y2="15"/>
          </svg>
          <span>Upload scans</span>
        </>
      )}
      {state === 'uploading' && <><Spinner size={14} /><span>{message || 'Uploading…'}</span></>}
      {state === 'success' && (
        <span className="upload-success">{message}</span>
      )}
      {state === 'error' && (
        <span className="upload-error">{message}</span>
      )}
    </div>
  )
}

// ── AnswerCard ───────────────────────────────────────────────────────────────

function AnswerCard({ answer }) {
  if (!answer) return null
  return (
    <div className="answer-card">
      <div className="answer-label">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none"
          stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <path d="M12 2a10 10 0 1 0 10 10A10 10 0 0 0 12 2z"/>
          <path d="M12 16v-4M12 8h.01"/>
        </svg>
        AI Answer
      </div>
      <div className="answer-text"><Markdown>{answer}</Markdown></div>
    </div>
  )
}

// ── SearchBar ────────────────────────────────────────────────────────────────

function SearchBar({ onSearch, loading, lastQuery, isSaved, onToggleSave }) {
  const [value, setValue] = useState('')
  const inputRef = useRef(null)

  const submit = (e) => {
    e.preventDefault()
    const q = value.trim()
    if (q) onSearch(q)
  }

  return (
    <form className="search-form" onSubmit={submit}>
      <div className="search-input-wrap">
        <svg className="search-icon" viewBox="0 0 24 24" fill="none"
          stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <circle cx="11" cy="11" r="8" />
          <line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>
        <input
          ref={inputRef}
          className="search-input"
          type="text"
          placeholder='Try: "Chase statements from 2025" or "documents requiring action"'
          value={value}
          onChange={e => setValue(e.target.value)}
          disabled={loading}
          autoFocus
        />
        {value && (
          <button
            type="button"
            className="search-clear"
            onClick={() => { setValue(''); inputRef.current?.focus() }}
            aria-label="Clear"
          >
            ×
          </button>
        )}
      </div>
      <button className="search-btn" type="submit" disabled={loading || !value.trim()}>
        {loading ? <Spinner size={16} /> : 'Search'}
      </button>
      {lastQuery && (
        <button
          type="button"
          className={`bookmark-btn${isSaved ? ' bookmarked' : ''}`}
          onClick={onToggleSave}
          title={isSaved ? 'Remove saved search' : 'Save this search'}
        >
          <svg width="16" height="16" viewBox="0 0 24 24"
            fill={isSaved ? 'currentColor' : 'none'}
            stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/>
          </svg>
        </button>
      )}
    </form>
  )
}

// ── Spinner ──────────────────────────────────────────────────────────────────

function Spinner({ size = 20 }) {
  return (
    <svg className="spinner" width={size} height={size} viewBox="0 0 24 24" fill="none">
      <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="3" strokeOpacity="0.25" />
      <path d="M12 2a10 10 0 0 1 10 10" stroke="currentColor" strokeWidth="3" strokeLinecap="round" />
    </svg>
  )
}

// ── DocumentCard ─────────────────────────────────────────────────────────────

function DocumentCard({ doc, selected, onClick, onActionUpdate }) {
  const meta = typeMeta(doc.document_type)
  const amounts = doc.dollar_amounts ?? []
  const [completing, setCompleting] = useState(false)
  const [notes, setNotes] = useState('')
  const [saving, setSaving] = useState(false)

  const handleCompleteClick = (e) => {
    e.stopPropagation()
    setCompleting(true)
  }

  const handleCancel = (e) => {
    e.stopPropagation()
    setCompleting(false)
    setNotes('')
  }

  const handleSubmit = async (e) => {
    e.stopPropagation()
    setSaving(true)
    try {
      const res = await fetch(`/api/documents/${doc.id}/action`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ completed: true, notes }),
      })
      if (res.ok) {
        setCompleting(false)
        setNotes('')
        onActionUpdate(doc.id, { action_completed: true, action_notes: notes })
      }
    } finally {
      setSaving(false)
    }
  }

  const handleUndoComplete = async (e) => {
    e.stopPropagation()
    const res = await fetch(`/api/documents/${doc.id}/action`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ completed: false, notes: '' }),
    })
    if (res.ok) {
      onActionUpdate(doc.id, { action_completed: false, action_notes: null })
    }
  }

  return (
    <div className={`doc-card ${selected ? 'selected' : ''}`} onClick={onClick}>
      <div className="doc-card-top">
        <span className="doc-type-badge" style={{ background: meta.color }}>
          {meta.label}
        </span>
        {doc.action_required && !doc.action_completed && (
          <span className="action-badge">Action Required</span>
        )}
        {doc.action_required && doc.action_completed && (
          <span className="completed-badge">✓ Completed</span>
        )}
      </div>

      <div className="doc-sender">{doc.sender_normalized || 'Unknown Sender'}</div>

      <div className="doc-date">
        {doc.document_date
          ? new Date(doc.document_date + 'T00:00:00').toLocaleDateString('en-US', {
              year: 'numeric', month: 'short', day: 'numeric',
            })
          : 'Date unknown'}
      </div>

      {amounts.length > 0 && (
        <div className="doc-amounts">
          {amounts.slice(0, 3).map((a, i) => (
            <span key={i} className="amount-chip">
              ${Number(a.value ?? 0).toFixed(2)}
              {a.label ? ` · ${a.label}` : ''}
            </span>
          ))}
        </div>
      )}

      {doc.summary && (
        <div className="doc-summary"><Markdown>{doc.summary}</Markdown></div>
      )}

      {doc.action_description && !doc.action_completed && (
        <p className="doc-action">{doc.action_description}</p>
      )}

      {doc.action_notes && doc.action_completed && (
        <p className="doc-action-notes">Note: {doc.action_notes}</p>
      )}

      {/* ── Action controls ── */}
      {doc.action_required && !doc.action_completed && !completing && (
        <button className="complete-btn" onClick={handleCompleteClick}>
          Mark as Complete
        </button>
      )}

      {doc.action_required && doc.action_completed && (
        <button className="undo-btn" onClick={handleUndoComplete}>
          Undo
        </button>
      )}

      {completing && (
        <div className="complete-form" onClick={e => e.stopPropagation()}>
          <textarea
            className="complete-notes"
            placeholder="Optional: describe what you did…"
            value={notes}
            onChange={e => setNotes(e.target.value)}
            rows={2}
            autoFocus
          />
          <div className="complete-form-actions">
            <button className="complete-submit-btn" onClick={handleSubmit} disabled={saving}>
              {saving ? 'Saving…' : 'Confirm Complete'}
            </button>
            <button className="complete-cancel-btn" onClick={handleCancel}>
              Cancel
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

// ── PdfPane ──────────────────────────────────────────────────────────────────

function PdfPane({ doc }) {
  if (!doc) {
    return (
      <div className="pdf-empty">
        <svg width="48" height="48" viewBox="0 0 24 24" fill="none"
          stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"
          style={{ opacity: 0.3 }}>
          <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
          <polyline points="14 2 14 8 20 8" />
          <line x1="16" y1="13" x2="8" y2="13" />
          <line x1="16" y1="17" x2="8" y2="17" />
          <polyline points="10 9 9 9 8 9" />
        </svg>
        <p>Select a document to view it here</p>
      </div>
    )
  }

  return (
    <div className="pdf-pane">
      <div className="pdf-toolbar">
        <span className="pdf-title">
          {doc.sender_normalized || 'Document'} &mdash; {doc.document_type}
        </span>
        <a
          className="pdf-new-tab"
          href={`/api/documents/${doc.id}/pdf`}
          target="_blank"
          rel="noreferrer"
        >
          Open in new tab ↗
        </a>
      </div>
      <iframe
        key={doc.id}
        className="pdf-frame"
        src={`/api/documents/${doc.id}/pdf`}
        title={`${doc.sender_normalized} — ${doc.document_date}`}
      />
    </div>
  )
}

// ── App ──────────────────────────────────────────────────────────────────────

export default function App() {
  const [results, setResults] = useState([])
  const [mode, setMode] = useState(null)
  const [answer, setAnswer] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [selectedDoc, setSelectedDoc] = useState(null)
  const [searched, setSearched] = useState(false)
  const [lastQuery, setLastQuery] = useState(null)
  const { saved: savedSearches, save: saveSearch, remove: removeSearch } = useSavedSearches()

  const handleSearch = useCallback(async (query) => {
    setLastQuery(query)
    setLoading(true)
    setError(null)
    setSelectedDoc(null)
    setAnswer(null)
    try {
      const res = await fetch('/api/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query }),
      })
      if (!res.ok) {
        const body = await res.json().catch(() => ({}))
        throw new Error(body.detail ?? `Search failed (${res.status})`)
      }
      const data = await res.json()
      setResults(data.results)
      setMode(data.mode)
      setAnswer(data.answer ?? null)
      setSearched(true)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }, [])

  const hasResults = results.length > 0

  const handleActionUpdate = useCallback((docId, patch) => {
    setResults(prev => prev.map(r => r.id === docId ? { ...r, ...patch } : r))
    setSelectedDoc(prev => prev?.id === docId ? { ...prev, ...patch } : prev)
  }, [])

  return (
    <div className="app">
      {/* ── Header ── */}
      <header className="app-header">
        <div className="header-top">
          <div className="brand">
            <PPDLogo size={36} />
            <div className="brand-text">
              <span className="brand-ppd">Peak Precision Data</span>
              <span className="brand-app">Mail Intelligence</span>
            </div>
          </div>
          <UploadZone />

          {searched && !loading && (
            <div className="search-meta">
              <span className={`mode-pill mode-${mode}`}>
                {mode === 'nlp' ? '✦ AI Search' : '⌕ Keyword Search'}
              </span>
              <span className="result-count">
                {results.length} {results.length === 1 ? 'result' : 'results'}
              </span>
            </div>
          )}
        </div>

        <SearchBar
          onSearch={handleSearch}
          loading={loading}
          lastQuery={lastQuery}
          isSaved={lastQuery != null && savedSearches.includes(lastQuery)}
          onToggleSave={() => savedSearches.includes(lastQuery) ? removeSearch(lastQuery) : saveSearch(lastQuery)}
        />

        {error && (
          <div className="error-banner">
            <strong>Error:</strong> {error}
          </div>
        )}
      </header>

      {answer && <AnswerCard answer={answer} />}

      {/* ── Body ── */}
      <div className="main-layout">
        {/* Results panel */}
        <aside className="results-panel">
          {loading && (
            <div className="state-message">
              <Spinner />
              <span>Searching…</span>
            </div>
          )}

          {!loading && !searched && (
            <>
              {savedSearches.length > 0 && (
                <div className="saved-searches">
                  <p className="saved-label">Saved searches</p>
                  {savedSearches.map(q => (
                    <div key={q} className="saved-row">
                      <button className="saved-chip" onClick={() => handleSearch(q)}>{q}</button>
                      <button className="saved-remove" onClick={() => removeSearch(q)} title="Remove">×</button>
                    </div>
                  ))}
                </div>
              )}
              <div className="state-message muted">
                <p>Search your scanned mail using plain English.</p>
                <ul className="hint-list">
                  <li>"Chase bank statements 2025"</li>
                  <li>"EOBs from Aetna this year"</li>
                  <li>"documents requiring action"</li>
                  <li>"utility bills over $200"</li>
                </ul>
              </div>
            </>
          )}

          {!loading && searched && !hasResults && (
            <div className="state-message muted">
              <p>No documents found.</p>
              <p>Try broader terms or check the spelling of the sender name.</p>
            </div>
          )}

          {!loading && results.filter(doc => doc.id != null).map(doc => (
            <DocumentCard
              key={doc.id}
              doc={doc}
              selected={selectedDoc?.id === doc.id}
              onClick={() => setSelectedDoc(doc)}
              onActionUpdate={handleActionUpdate}
            />
          ))}
        </aside>

        {/* PDF viewer */}
        <section className="pdf-section">
          <PdfPane doc={selectedDoc} />
        </section>
      </div>
    </div>
  )
}
