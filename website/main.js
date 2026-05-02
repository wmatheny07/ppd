// Nav — becomes opaque on scroll
const nav = document.getElementById('nav')
window.addEventListener('scroll', () => {
  nav.classList.toggle('scrolled', window.scrollY > 20)
}, { passive: true })

// Mobile nav toggle
const toggle    = document.getElementById('nav-toggle')
const mobileNav = document.getElementById('nav-mobile')
toggle.addEventListener('click', () => mobileNav.classList.toggle('open'))
mobileNav.querySelectorAll('a').forEach(a => {
  a.addEventListener('click', () => mobileNav.classList.remove('open'))
})

// Apply 'open' display via JS to avoid flash-of-menu on load
const style = document.createElement('style')
style.textContent = '.nav-mobile.open { display: flex; }'
document.head.appendChild(style)

// Scroll reveal
const observer = new IntersectionObserver(entries => {
  entries.forEach(entry => {
    if (entry.isIntersecting) {
      entry.target.classList.add('visible')
      observer.unobserve(entry.target)
    }
  })
}, { threshold: 0.1, rootMargin: '0px 0px -40px 0px' })

document.querySelectorAll('.reveal').forEach(el => observer.observe(el))

// Stagger service cards and why-cards on reveal
const staggerGroups = ['.services-grid', '.why-cards']
staggerGroups.forEach(selector => {
  const parent = document.querySelector(selector)
  if (!parent) return
  parent.querySelectorAll('.reveal').forEach((card, i) => {
    card.style.transitionDelay = `${i * 60}ms`
  })
})
