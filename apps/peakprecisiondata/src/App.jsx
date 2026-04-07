import { useState } from 'react'
import Logo from './Logo'
import './index.css'

/* ── Icons (inline SVG helpers) ── */
const Icon = ({ d, size = 22 }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none"
    stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d={d} />
  </svg>
)

const icons = {
  chart:   'M3 3v18h18M7 16l4-4 4 4 5-5',
  db:      'M12 2C6.48 2 2 4.24 2 7s4.48 5 10 5 10-2.24 10-5-4.48-5-10-5zM2 17c0 2.76 4.48 5 10 5s10-2.24 10-5M2 12c0 2.76 4.48 5 10 5s10-2.24 10-5',
  layers:  'M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5',
  cpu:     'M9 3H5a2 2 0 0 0-2 2v4m6-6h6m-6 0v18m6-18h4a2 2 0 0 1 2 2v4m-6-6v18m0 0H9m6 0h4a2 2 0 0 0 2-2v-4M3 9h18M3 15h18',
  mail:    'M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z M22 6l-10 7L2 6',
  phone:   'M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07A19.5 19.5 0 0 1 4.07 13a19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 3 2.18h3a2 2 0 0 1 2 1.72c.127.96.361 1.903.7 2.81a2 2 0 0 1-.45 2.11L7.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0 1 22 16.92z',
  loc:     'M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z M12 7a3 3 0 1 0 0 6 3 3 0 0 0 0-6z',
  arrow:   'M5 12h14M12 5l7 7-7 7',
  check:   'M20 6L9 17l-5-5',
}

const services = [
  {
    icon: icons.chart,
    title: 'Analytics & Business Intelligence',
    desc: 'Transform raw data into actionable dashboards and reports. We design BI solutions that give decision-makers real-time visibility into what matters most.',
  },
  {
    icon: icons.db,
    title: 'Data Engineering & Pipelines',
    desc: 'Robust, scalable data pipelines built on modern stacks. From ingestion to transformation, we ensure your data flows reliably and efficiently.',
  },
  {
    icon: icons.layers,
    title: 'Data Warehousing & Modeling',
    desc: 'Architecture and implementation of cloud data warehouses using Snowflake, BigQuery, or Redshift — structured for performance and governed for trust.',
  },
  {
    icon: icons.cpu,
    title: 'AI & Machine Learning Integration',
    desc: 'Embed predictive models and ML-driven insights directly into your operations. We bridge the gap between data science and production-ready systems.',
  },
]

const process = [
  { num: '01', title: 'Discover', desc: 'We audit your data landscape and define the right questions to answer.' },
  { num: '02', title: 'Design',   desc: 'Architecture and roadmap tailored to your stack, team, and goals.' },
  { num: '03', title: 'Build',    desc: 'Agile delivery of pipelines, models, and dashboards with full transparency.' },
  { num: '04', title: 'Optimize', desc: 'Continuous improvement — monitoring, tuning, and evolving with your needs.' },
]

const pillars = [
  'Precision-first methodology',
  'Cloud-native architecture',
  'Agile delivery cycles',
  'Transparent communication',
  'End-to-end ownership',
  'Scalable by design',
]

function ContactForm() {
  const [sent, setSent] = useState(false)

  const handleSubmit = (e) => {
    e.preventDefault()
    // In production wire to a real endpoint / email service
    setSent(true)
  }

  if (sent) {
    return (
      <div className="contact-form" style={{ textAlign: 'center', padding: '64px 32px' }}>
        <div style={{ color: 'var(--blue-bright)', marginBottom: 16 }}>
          <Icon d={icons.check} size={48} />
        </div>
        <h3 style={{ color: 'var(--navy)', fontSize: '1.25rem', fontWeight: 700, marginBottom: 8 }}>
          Message Received
        </h3>
        <p style={{ color: 'var(--gray-600)', fontSize: '0.95rem' }}>
          Thanks for reaching out — we'll be in touch within one business day.
        </p>
      </div>
    )
  }

  return (
    <form className="contact-form" onSubmit={handleSubmit}>
      <div className="form-row">
        <div className="form-group">
          <label htmlFor="fname">First Name</label>
          <input id="fname" type="text" placeholder="Jane" required />
        </div>
        <div className="form-group">
          <label htmlFor="lname">Last Name</label>
          <input id="lname" type="text" placeholder="Smith" required />
        </div>
      </div>
      <div className="form-group">
        <label htmlFor="email">Work Email</label>
        <input id="email" type="email" placeholder="jane@company.com" required />
      </div>
      <div className="form-group">
        <label htmlFor="company">Company</label>
        <input id="company" type="text" placeholder="Acme Corp" />
      </div>
      <div className="form-group">
        <label htmlFor="service">Area of Interest</label>
        <select id="service">
          <option value="">Select a service…</option>
          <option>Analytics &amp; Business Intelligence</option>
          <option>Data Engineering &amp; Pipelines</option>
          <option>Data Warehousing &amp; Modeling</option>
          <option>AI &amp; Machine Learning</option>
          <option>Other / Not Sure Yet</option>
        </select>
      </div>
      <div className="form-group">
        <label htmlFor="message">Tell us about your project</label>
        <textarea id="message" placeholder="What data challenges are you facing?" required />
      </div>
      <button type="submit" className="btn-submit">Send Message</button>
    </form>
  )
}

/* ── Hero chart animation (simple SVG) ── */
function HeroVisual() {
  return (
    <svg viewBox="0 0 340 260" fill="none" xmlns="http://www.w3.org/2000/svg"
      style={{ width: '100%', maxWidth: 380 }}>

      {/* Grid lines */}
      {[60, 110, 160, 210].map(y => (
        <line key={y} x1="40" y1={y} x2="320" y2={y}
          stroke="rgba(255,255,255,0.06)" strokeWidth="1" />
      ))}
      {[80, 140, 200, 260, 320].map(x => (
        <line key={x} x1={x} y1="30" x2={x} y2="230"
          stroke="rgba(255,255,255,0.06)" strokeWidth="1" />
      ))}

      {/* Area fill under chart */}
      <defs>
        <linearGradient id="area-grad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#2563eb" stopOpacity="0.35" />
          <stop offset="100%" stopColor="#2563eb" stopOpacity="0" />
        </linearGradient>
      </defs>
      <polygon
        points="60,195 100,160 150,180 200,110 250,130 300,70 300,230 60,230"
        fill="url(#area-grad)"
      />

      {/* Line */}
      <polyline
        points="60,195 100,160 150,180 200,110 250,130 300,70"
        stroke="#60a5fa" strokeWidth="2.5"
        strokeLinecap="round" strokeLinejoin="round"
        fill="none"
      />

      {/* Dots */}
      {[[60,195],[100,160],[150,180],[200,110],[250,130],[300,70]].map(([x,y], i) => (
        <circle key={i} cx={x} cy={y} r="4.5"
          fill={i === 5 ? '#ffffff' : '#60a5fa'}
          stroke="#1e3a8a" strokeWidth="2" />
      ))}

      {/* Bar chart in background */}
      {[[80,180,50],[140,155,75],[200,130,100],[260,95,135],[310,60,170]].map(([x, y, h], i) => (
        <rect key={i} x={x - 14} y={y} width={28} height={h}
          fill="#1e40af" opacity={0.25 + i * 0.05} rx="2" />
      ))}

      {/* Axis labels */}
      {['Q1','Q2','Q3','Q4','YTD'].map((label, i) => (
        <text key={i} x={80 + i * 60} y={248} fill="rgba(255,255,255,0.35)"
          fontSize="11" textAnchor="middle" fontFamily="Inter, sans-serif">
          {label}
        </text>
      ))}

      {/* Tooltip bubble */}
      <rect x="256" y="42" width="90" height="36" rx="6"
        fill="#1e40af" opacity="0.9" />
      <text x="301" y="57" fill="white" fontSize="10" textAnchor="middle"
        fontFamily="Inter, sans-serif" fontWeight="600">
        Revenue
      </text>
      <text x="301" y="70" fill="#93c5fd" fontSize="11" textAnchor="middle"
        fontFamily="Inter, sans-serif" fontWeight="700">
        +42.7%
      </text>
    </svg>
  )
}

/* ── About visual ── */
function AboutVisual() {
  return (
    <div style={{
      background: 'linear-gradient(135deg, #0a1f44 0%, #1e3a8a 100%)',
      borderRadius: 16, padding: 40,
      display: 'flex', flexDirection: 'column', gap: 20,
    }}>
      {[
        { label: 'Data Quality Score', value: 98, color: '#60a5fa' },
        { label: 'Pipeline Uptime',    value: 99.9, color: '#34d399', fmt: '99.9%' },
        { label: 'Query Performance',  value: 85, color: '#f59e0b' },
      ].map(({ label, value, color, fmt }) => (
        <div key={label}>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
            <span style={{ color: 'rgba(255,255,255,0.7)', fontSize: 13 }}>{label}</span>
            <span style={{ color, fontWeight: 700, fontSize: 13 }}>{fmt || `${value}%`}</span>
          </div>
          <div style={{ background: 'rgba(255,255,255,0.1)', borderRadius: 4, height: 6 }}>
            <div style={{
              width: `${value}%`, height: '100%',
              background: color, borderRadius: 4,
              transition: 'width 1s ease',
            }} />
          </div>
        </div>
      ))}

      <div style={{
        display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginTop: 8,
      }}>
        {[
          { val: '50+', label: 'Clients Served' },
          { val: '200+', label: 'Pipelines Built' },
          { val: '12+', label: 'Industries' },
          { val: '4.9★', label: 'Client Rating' },
        ].map(({ val, label }) => (
          <div key={label} style={{
            background: 'rgba(255,255,255,0.05)',
            borderRadius: 10, padding: '14px 16px', textAlign: 'center',
          }}>
            <div style={{ color: '#60a5fa', fontSize: '1.4rem', fontWeight: 800 }}>{val}</div>
            <div style={{ color: 'rgba(255,255,255,0.5)', fontSize: 12, marginTop: 4 }}>{label}</div>
          </div>
        ))}
      </div>
    </div>
  )
}

/* ── Main App ── */
export default function App() {
  return (
    <>
      {/* NAV */}
      <nav>
        <div className="nav-inner">
          <a href="#home" className="nav-logo">
            <Logo size={40} />
            <div className="nav-logo-text">
              <span className="top">Peak Precision</span>
              <span className="bottom">&mdash; Data &mdash;</span>
            </div>
          </a>
          <ul className="nav-links">
            <li><a href="#services">Services</a></li>
            <li><a href="#process">Process</a></li>
            <li><a href="#about">About</a></li>
            <li><a href="#contact" className="nav-cta">Get in Touch</a></li>
          </ul>
        </div>
      </nav>

      {/* HERO */}
      <section className="hero" id="home">
        <div className="hero-grid">
          <div className="hero-content">
            <div className="hero-badge">Data Engineering &amp; Analytics</div>
            <h1>
              Your data should be a <span>competitive advantage</span>
            </h1>
            <p>
              Peak Precision Data helps organizations build reliable data infrastructure,
              unlock actionable insights, and make better decisions — faster.
            </p>
            <div className="hero-actions">
              <a href="#contact" className="btn-primary">
                Start a Conversation <Icon d={icons.arrow} size={18} />
              </a>
              <a href="#services" className="btn-secondary">
                Our Services
              </a>
            </div>
          </div>
          <div className="hero-visual">
            <HeroVisual />
          </div>
        </div>
      </section>

      {/* STATS BAR */}
      <div className="stats-bar">
        <div className="stats-inner">
          {[
            { num: '50+',    label: 'Clients Across Industries' },
            { num: '200+',   label: 'Data Pipelines Delivered' },
            { num: '99.9%',  label: 'Average Pipeline Uptime' },
            { num: '4.9/5',  label: 'Average Client Rating' },
          ].map(({ num, label }) => (
            <div className="stat-item" key={label}>
              <div className="num">{num}</div>
              <div className="label">{label}</div>
            </div>
          ))}
        </div>
      </div>

      {/* SERVICES */}
      <section className="section section-alt" id="services">
        <div className="section-header">
          <span className="section-label">What We Do</span>
          <h2>End-to-end data solutions</h2>
          <p>
            From raw ingestion to polished dashboards, we handle every layer of
            your data stack with precision and care.
          </p>
        </div>
        <div className="services-grid">
          {services.map(({ icon, title, desc }) => (
            <div className="service-card" key={title}>
              <div className="service-icon">
                <Icon d={icon} size={24} />
              </div>
              <h3>{title}</h3>
              <p>{desc}</p>
            </div>
          ))}
        </div>
      </section>

      {/* PROCESS */}
      <section className="section section-dark" id="process">
        <div className="section-header">
          <span className="section-label">How We Work</span>
          <h2>A proven, collaborative process</h2>
          <p>
            We operate with full transparency at every stage — no black boxes,
            no surprise bills.
          </p>
        </div>
        <div className="process-steps">
          {process.map(({ num, title, desc }) => (
            <div className="process-step" key={num}>
              <div className="step-num">{num}</div>
              <h3>{title}</h3>
              <p>{desc}</p>
            </div>
          ))}
        </div>
      </section>

      {/* ABOUT */}
      <section className="section" id="about">
        <div className="about-grid">
          <div className="about-content">
            <span className="section-label">About Us</span>
            <h2>Built by engineers who've been in your shoes</h2>
            <p>
              Peak Precision Data was founded by a team of data engineers and
              analysts who spent years inside enterprise organizations — frustrated
              by unreliable pipelines, siloed data, and insights that arrived too
              late to act on.
            </p>
            <p>
              We built the consultancy we always wished existed: opinionated about
              quality, pragmatic about tooling, and relentlessly focused on outcomes
              that matter to the business.
            </p>
            <div className="pillars">
              {pillars.map(p => (
                <div className="pillar" key={p}>
                  <div className="pillar-dot" />
                  <p>{p}</p>
                </div>
              ))}
            </div>
          </div>
          <div className="about-visual">
            <AboutVisual />
          </div>
        </div>
      </section>

      {/* CONTACT */}
      <section className="section section-alt" id="contact">
        <div className="contact-grid">
          <div className="contact-info">
            <span className="section-label">Get In Touch</span>
            <h2>Let's talk about your data</h2>
            <p>
              Whether you're starting from scratch or untangling a legacy mess,
              we'd love to hear about your challenges and explore how we can help.
            </p>
            <div className="contact-details">
              <div className="contact-detail">
                <Icon d={icons.mail} size={20} />
                <span>hello@peakprecisiondata.com</span>
              </div>
              <div className="contact-detail">
                <Icon d={icons.phone} size={20} />
                <span>+1 (800) PPD-DATA</span>
              </div>
              <div className="contact-detail">
                <Icon d={icons.loc} size={20} />
                <span>Remote-first &mdash; serving clients nationwide</span>
              </div>
            </div>
          </div>
          <ContactForm />
        </div>
      </section>

      {/* FOOTER */}
      <footer>
        <div className="footer-inner">
          <div className="footer-logo">
            <Logo size={32} />
            <div className="footer-logo-text">
              <div className="top">Peak Precision</div>
              <div className="bottom">&mdash; Data &mdash;</div>
            </div>
          </div>
          <span className="footer-copy">
            &copy; {new Date().getFullYear()} Peak Precision Data. All rights reserved.
          </span>
          <ul className="footer-links">
            <li><a href="#services">Services</a></li>
            <li><a href="#about">About</a></li>
            <li><a href="#contact">Contact</a></li>
          </ul>
        </div>
      </footer>
    </>
  )
}
