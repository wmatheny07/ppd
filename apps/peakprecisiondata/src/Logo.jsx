export default function Logo({ size = 56 }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 200 200"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
    >
      <defs>
        <linearGradient id="mtn-grad" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="#1e3a8a" />
          <stop offset="100%" stopColor="#1e6bc4" />
        </linearGradient>
        <linearGradient id="peak-grad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#93c5fd" />
          <stop offset="100%" stopColor="#60a5fa" />
        </linearGradient>
      </defs>

      {/* Background mountain (left, darker) */}
      <polygon
        points="30,155 95,55 160,155"
        fill="url(#mtn-grad)"
        opacity="0.75"
      />

      {/* Foreground mountain (right, brighter) */}
      <polygon
        points="70,155 140,38 200,155"
        fill="url(#mtn-grad)"
      />

      {/* Snow caps */}
      <polygon points="95,55 80,90 110,90" fill="url(#peak-grad)" opacity="0.9" />
      <polygon points="140,38 122,80 158,80" fill="url(#peak-grad)" />

      {/* Data line connecting three points */}
      <polyline
        points="52,112 95,75 140,58 175,90"
        stroke="#93c5fd"
        strokeWidth="4"
        strokeLinecap="round"
        strokeLinejoin="round"
        fill="none"
        opacity="0.95"
      />

      {/* Data dots */}
      <circle cx="52"  cy="112" r="6" fill="#93c5fd" />
      <circle cx="95"  cy="75"  r="6" fill="#60a5fa" />
      <circle cx="140" cy="58"  r="7" fill="#ffffff" stroke="#60a5fa" strokeWidth="2" />
      <circle cx="175" cy="90"  r="6" fill="#93c5fd" />
    </svg>
  );
}
